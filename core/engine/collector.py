#!/usr/bin/env python3
# core/engine/collector.py
# 최종 수정: ABC 제거, 추상메서드 제거, _process_item 제거,
#           stats/validator 통합, config fallback, 로그 메시지 수정

import os
import logging
import sqlite3
import queue
import threading
import time
import glob
import re
import random
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable

from core.data.database import get_db_connection, init_checkpoint_table
from core.util.manage_log import build_domain_logger, resolve_domain_log_path
from core.engine.network import build_session, safe_json_request
from core.engine.shard import should_include_school
from core.kst_time import now_kst
from core.data.guard_data import DataGuard, DataDropException
from core.data.collect_log import CollectLog
from core.engine.collector_stats import CollectorStats
from core.data.validator_data import DataValidator
from constants.codes import NEIS_API_KEY
from constants.paths import NEIS_INFO_DB_PATH as MASTER_DB, LOG_DIR
from core.engine.retry import RetryManager
from constants.paths import FAILURES_DB_PATH
from constants.schema import SCHEMAS
from core.data.manage_schema import create_table_from_schema, save_batch


class CollectorEngine:
    # ----- 메타데이터 (하위 클래스에서 오버라이드) -----
    description = None
    table_name = None
    merge_script = None
    parallel_script = "scripts/run_pipeline.py"
    timeout_seconds = 3600
    parallel_timeout_seconds = 7200
    merge_timeout_seconds = 1800
    modes = ["통합", "odd 샤드", "even 샤드", "병렬 실행"]
    metrics_config = {"enabled": False}
    parallel_config = {}
    # ------------------------------------------------

    schema_name = None          # constants.schema.py의 키
    validate_data = False       # 데이터 유효성 검증 활성화 여부

    def __init__(
        self,
        name: str,
        base_dir: str,
        shard: str = "none",
        school_range: Optional[str] = None,
        quiet_mode: bool = False,
        **kwargs
    ):
        VALID_SHARDS = {"odd", "even", "none"}
        if shard not in VALID_SHARDS:
            print(f"❌ 잘못된 shard 값: {shard} (collector: {name})", file=sys.stderr)
            sys.exit(1)

        self.name = name
        self.base_dir = Path(base_dir)
        self.shard = shard
        self.school_range = school_range
        self.api_context = 'common'
        self.quiet_mode = quiet_mode

        if shard == "none":
            self.db_path = str(self.base_dir / f"{name}.db")
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            self.db_path = str(self.base_dir / f"{name}_{shard}{range_suffix}.db")

        self.total_db_path = str(self.base_dir / f"{name}_total.db")

        self.debug_mode = kwargs.get('debug_mode', False)

        self.logger = build_domain_logger(name, name, __file__)
        if self.debug_mode:
            self.logger.setLevel(logging.DEBUG)
        if not self.quiet_mode:
            print(f"📝 로그 파일: {resolve_domain_log_path(name, __file__)}")

        self.session = build_session()

        self.q = queue.Queue()
        self.writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name=f"{name}_writer"
        )
        self._writer_failed = False
        self._writer_error: Optional[Exception] = None
        self.writer_thread.start()

        self.school_cache: Dict[str, Any] = {}
        self.school_by_id: Dict[str, Any] = {}
        self._load_school_cache()

        self._closeable_resources = []
        self.data_guard = DataGuard()
        self.collect_log = CollectLog()
        self.retry_mgr = RetryManager(db_path=str(FAILURES_DB_PATH))

        # 배치 메트릭스 초기화
        self.stats = CollectorStats(name, self.logger)

        # 설정에서 validate_data 로드 (fallback 처리)
        try:
            from core.config import config
            collector_cfg = config.get_collector_config(name)
            self.validate_data = collector_cfg.get("validate_data", False)
        except (ImportError, AttributeError, Exception) as e:
            self.validate_data = False
            self.logger.debug(f"config 로드 실패, validate_data=False: {e}")

        self._init_db()
        self.completed_items: set = set()
        self._load_checkpoints()

        self.logger.debug(
            f"{name} 초기화 완료 "
            f"(샤드: {shard}, 범위: {school_range}, 캐시: {len(self.school_cache)}개)"
        )

    # ✅ 기본 _init_db 구현 (schema_name이 있으면 테이블 생성)
    def _init_db(self):
        if self.schema_name:
            with get_db_connection(self.db_path) as conn:
                create_table_from_schema(conn, self.schema_name)
                self._init_db_common(conn)

    def _init_db_common(self, conn):
        init_checkpoint_table(conn)

    def record_collect_failure(
        self, region: str, year: Optional[int] = None, error: str = ""
    ):
        self.retry_mgr.record_failure(
            domain='collect',
            task_type='fetch_region',
            region=region,
            year=year or now_kst().year,
            error=error
        )
        self.logger.warning(f"수집 실패 기록: region={region}, year={year}")

    def register_resource(self, resource):
        self._closeable_resources.append(resource)
        return resource

    def _load_school_cache(self):
        if not os.path.exists(MASTER_DB):
            self.logger.warning("neis_info.db 없음. 캐시 없이 동작")
            return
        try:
            with sqlite3.connect(str(MASTER_DB)) as conn:
                cur = conn.execute("""
                    SELECT sc_code, atpt_code, school_id, sc_name, sc_kind,
                           CASE WHEN sc_kind LIKE '%초등%' THEN '초'
                                WHEN sc_kind LIKE '%중%'  THEN '중'
                                WHEN sc_kind LIKE '%고%'  THEN '고'
                                WHEN sc_kind LIKE '%특수%' THEN '특'
                                ELSE '기타' END as school_level,
                           CASE WHEN sc_kind LIKE '%특수%' THEN 1 ELSE 0 END as is_special,
                           status
                    FROM schools_neis WHERE status = '운영'
                """)
                for row in cur:
                    sc_code, atpt_code, school_id, sc_name, sc_kind, \
                        level, is_special, status = row
                    self.school_cache[sc_code] = {
                        'atpt_code':  atpt_code,
                        'school_id':  school_id,
                        'name':       sc_name,
                        'kind':       sc_kind,
                        'level':      level,
                        'is_special': is_special,
                        'status':     status,
                    }
                    self.school_by_id[school_id] = self.school_cache[sc_code]
        except Exception as e:
            self.logger.error(f"학교 캐시 로드 실패: {e}")

    def get_school_info(self, school_code: str) -> Optional[Dict[str, Any]]:
        return self.school_cache.get(school_code)

    def _get_field(self, raw_item: dict, field: str, default=None):
        from constants.map_apis import get_api_field
        value = get_api_field(raw_item, field, context=self.api_context, default=default)
        return "" if value is None else value

    def _include_school(self, school_code: str) -> bool:
        if not school_code:
            return False
        return should_include_school(self.shard, self.school_range, school_code)

    def _fetch_paginated(
        self,
        url: str,
        base_params: dict,
        response_key: str,
        page_size: int = 100,
        max_page: int = 500,
        region: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[dict]:
        all_items = []
        page = 1
        retry_count = 0
        max_retries = 3

        while page <= max_page:
            params = {"pIndex": page, "pSize": page_size, **base_params}
            try:
                res = safe_json_request(self.session, url, params, self.logger)
                if not res or response_key not in res:
                    break
                rows = res[response_key][1].get("row", [])
                if not rows:
                    break
                all_items.extend(rows)

                head = res[response_key][0].get("head", [{}])
                total_count = int(head[0].get("list_total_count", 0))
                if len(all_items) >= total_count:
                    break

                page += 1
                retry_count = 0
                time.sleep(random.uniform(0.1, 0.3))

            except Exception as e:
                retry_count += 1
                self.logger.error(
                    f"페이지 {page} 에러 ({retry_count}/{max_retries}): {e}"
                )
                if retry_count >= max_retries:
                    if region:
                        self.record_collect_failure(region, year, str(e))
                    break
                sleep_time = min(2 ** retry_count, 30) + random.uniform(0, 1)
                time.sleep(sleep_time)

        return all_items

    def iterate_schools_by_month(
        self,
        region: str,
        year: int,
        month_range: List[tuple],
        per_school_month_func: Callable[[str, int, int], None]
    ):
        raise NotImplementedError

    def iterate_schools(
        self, region: str, per_school_func: Callable[[str, str], None]
    ):
        raise NotImplementedError

    def _writer_loop(self):
        try:
            while True:
                item = self.q.get()
                if item is None:
                    self.q.task_done()
                    break

                items_processed = 1
                batch = self._flatten(item)

                while len(batch) < 500:
                    try:
                        nxt = self.q.get_nowait()
                        if nxt is None:
                            self.q.task_done()
                            break
                        items_processed += 1
                        batch.extend(self._flatten(nxt))
                    except queue.Empty:
                        break

                if hasattr(self, 'debug_mode') and self.debug_mode and not self.quiet_mode:
                    print(f"🔍 [writer_loop] 배치 수집 완료: {len(batch)}개")

                try:
                    if batch:
                        self._save_batch(batch)
                except DataDropException as e:
                    self._writer_failed = True
                    self._writer_error = e
                    print(f"🚨 데이터 급감: {e}")
                    self.logger.error(f"🚨 데이터 급감 감지: {e}")
                except Exception as e:
                    self._writer_failed = True
                    self._writer_error = e
                    print(f"❌ 배치 저장 예외: {e}")
                    self.logger.error(f"배치 저장 실패: {e}", exc_info=True)
                finally:
                    for _ in range(items_processed):
                        self.q.task_done()
        except Exception as e:
            self._writer_failed = True
            self._writer_error = e
            if not self.quiet_mode:
                print(f"💥 writer_loop 치명적 예외: {e}")
            traceback.print_exc()
            raise

    def _flatten(self, data) -> List:
        if data is None:
            return []
        if isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, list):
                    result.extend(item)
                else:
                    result.append(item)
            return result
        return [data]

    def enqueue(self, data: Union[dict, List[dict]]):
        if hasattr(self, 'debug_mode') and self.debug_mode and not self.quiet_mode:
            print(f"🔍 [base_enqueue] 데이터 크기: {len(data) if isinstance(data, list) else 1}")
        self.q.put(data)

    def create_dated_backup(self):
        if not os.path.exists(self.db_path):
            return
        from core.kst_time import KST
        today = now_kst().strftime("%Y%m%d")
        backup_path = self.db_path.replace('.db', f'_{today}.db')
        if not os.path.exists(backup_path):
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(f"VACUUM INTO '{backup_path}'")
            self.logger.info(f"📅 날짜 백업 생성: {backup_path}")

        base = self.db_path.replace('.db', '')
        date_pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}})\.db$")
        for f in glob.glob(f"{base}_????????.db"):
            match = date_pattern.match(f)
            if not match:
                continue
            try:
                fdate = datetime.strptime(match.group(1), '%Y%m%d')
                fdate = KST.localize(fdate)
                if (now_kst() - fdate) > timedelta(days=30):
                    os.remove(f)
                    self.logger.info(f"🗑️ 오래된 백업 삭제: {f}")
            except ValueError:
                continue

    def print(self, *args, level: str = "info", **kwargs):
        if self.quiet_mode:
            return
        if level == "debug" and not (hasattr(self, 'debug_mode') and self.debug_mode):
            return
        print(*args, **kwargs)

    def print_progress(self, current: int, total: int, prefix: str = "", bar_length: int = 20):
        if self.quiet_mode:
            return
        if total <= 0:
            print(f"\r{prefix} [{'░' * bar_length}] {current}/0 (0.0%)", end='', flush=True)
            return
        percent = current / total
        filled = int(bar_length * percent)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\r{prefix} [{bar}] {current}/{total} ({percent*100:.1f}%)", end='', flush=True)
        if current == total:
            print()

    def _wait_for_queue_drain(self, timeout: float) -> bool:
        """큐가 비워질 때까지 대기. writer 비정상 종료 시 무한 대기를 피합니다."""
        deadline = time.time() + max(timeout, 0.0)
        while True:
            if self.q.unfinished_tasks == 0:
                return True
            if not self.writer_thread.is_alive():
                return False
            if timeout > 0 and time.time() >= deadline:
                return False
            time.sleep(0.1)

    def _get_target_key(self) -> Optional[str]:
        """체크포인트 키로 사용할 컬럼명. None이면 체크포인트 미사용."""
        return None

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]) -> None:
        if not self.schema_name:
            raise NotImplementedError(
                f"{self.__class__.__name__}은 schema_name을 정의하거나 _do_save_batch를 오버라이드해야 합니다."
            )
        schema = SCHEMAS.get(self.schema_name)
        if not schema:
            raise ValueError(f"schema '{self.schema_name}'이 constants/schema.py에 없습니다.")

        columns = [col[0].strip() for col in schema["columns"]]
        pk_columns = schema.get("primary_key", [])

        if self.validate_data:
            valid, errors, warnings = DataValidator.validate_batch(batch, columns, pk_columns)
            if warnings:
                for w in warnings[:5]:
                    self.logger.warning(f"데이터 검증 경고: {w}")
            if not valid:
                self.logger.error(f"데이터 유효성 검증 실패: {errors}")
                return

        self.logger.debug(f"저장할 테이블: {self.table_name}, 컬럼 수: {len(columns)}")
        save_batch(conn, self.table_name, columns, batch)

    def _save_batch(self, batch: List[dict]):
        start_time = time.time()
        success = False
        try:
            if len(batch) > 0:
                self.logger.debug(f"저장 시도: 배치 크기 {len(batch)}, 첫 번째 항목: {batch[0]}")
            with get_db_connection(self.db_path) as conn:
                self._do_save_batch(conn, batch)
            success = True
        except Exception as e:
            self.logger.error(f"배치 저장 예외: {e}", exc_info=True)
        finally:
            elapsed = time.time() - start_time
            self.stats.update(len(batch), elapsed, success)

    def _load_checkpoints(self):
        pass

    def save_checkpoint(
        self, region: str, school: str, sub_key: str, page: int, total: int
    ):
        pass

    def close(self, timeout: float = 60.0):
        self.logger.info(f"🔒 {self.name} 종료 시작...")
        drained = self._wait_for_queue_drain(timeout)
        if not drained:
            self.logger.warning("⚠️ queue drain timeout 또는 writer 비정상 종료 감지")

        if self.writer_thread.is_alive():
            self.q.put(None)
            self.writer_thread.join(timeout=timeout)
        if self.writer_thread.is_alive():
            self.logger.warning("⚠️ writer_thread가 정상 종료되지 않았습니다.")
        for res in self._closeable_resources:
            try:
                res.close()
            except Exception as e:
                self.logger.warning(f"리소스 close 실패: {e}")
        self.stats.log_summary()
        if self._writer_failed:
            err = f" ({self._writer_error})" if self._writer_error else ""
            raise RuntimeError(f"writer_loop에서 저장 실패가 발생했습니다{err}")
        self.logger.info(f"✅ {self.name} 종료 완료")

    def flush(self, timeout: float = 60.0):
        self.logger.info(f"🔄 {self.name} flush 시작...")
        drained = self._wait_for_queue_drain(timeout)
        if not drained:
            self.logger.warning("⚠️ flush 중 queue drain timeout 또는 writer 비정상 종료 감지")
        self.logger.info(f"✅ {self.name} flush 완료")
