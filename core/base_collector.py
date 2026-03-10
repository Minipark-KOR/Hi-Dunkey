#!/usr/bin/env python3
# core/base_collector.py
# 개발 가이드: docs/developer_guide.md 참조

import os
import sqlite3
import queue
import threading
import time
import glob
import re
import random
import sys
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable

from .database import get_db_connection, init_checkpoint_table
from .logger import build_logger
from .network import build_session, safe_json_request
from .shard import should_include_school
from .kst_time import now_kst
from .data_guard import DataGuard, DataDropException
from .collect_log import CollectLog
from constants.codes import NEIS_API_KEY
from constants.paths import NEIS_INFO_DB_PATH as MASTER_DB, LOG_DIR
from core.retry import RetryManager
from constants.paths import FAILURES_DB_PATH

class BaseCollector(ABC):
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

    def __init__(
        self,
        name: str,
        base_dir: str,
        shard: str = "none",
        school_range: Optional[str] = None,
        quiet_mode: bool = False,          # ✅ quiet_mode 추가
        **kwargs                            # ✅ 추가 인자 수용
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
        self.quiet_mode = quiet_mode        # ✅ 저장
        # kwargs는 필요시 self.kwargs = kwargs 로 저장하거나 무시

        if shard == "none":
            self.db_path = str(self.base_dir / f"{name}.db")
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            self.db_path = str(self.base_dir / f"{name}_{shard}{range_suffix}.db")

        self.total_db_path = str(self.base_dir / f"{name}_total.db")
        
        # ✅ 로그 파일은 LOG_DIR에 저장
        self.logger = build_logger(name, str(LOG_DIR / f"{name}.log"))
        if not self.quiet_mode:
            print(f"📝 로그 파일: {LOG_DIR / f'{name}.log'}")

        self.session = build_session()

        self.q = queue.Queue()
        self.writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name=f"{name}_writer"
        )
        self.writer_thread.start()
        if not self.quiet_mode:
            print(f"🔁 writer_thread 시작: {self.writer_thread.name}")

        self.school_cache: Dict[str, Any] = {}
        self.school_by_id: Dict[str, Any] = {}
        self._load_school_cache()

        self._closeable_resources = []
        self.data_guard = DataGuard()
        self.collect_log = CollectLog()
        self.retry_mgr = RetryManager(db_path=str(FAILURES_DB_PATH))

        self._init_db()
        self.completed_items: set = set()
        self._load_checkpoints()

        self.logger.info(
            f"🔥 {name} 초기화 완료 "
            f"(샤드: {shard}, 범위: {school_range}, 캐시: {len(self.school_cache)}개)"
        )

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
            self.logger.warning("school_info.db 없음. 캐시 없이 동작")
            return
        try:
            with sqlite3.connect(str(NEIS_INFO_DB_PATH)) as conn:
                cur = conn.execute("""
                    SELECT sc_code, atpt_code, school_id, sc_name, sc_kind,
                           CASE WHEN sc_kind LIKE '%초등%' THEN '초'
                                WHEN sc_kind LIKE '%중%'  THEN '중'
                                WHEN sc_kind LIKE '%고%'  THEN '고'
                                WHEN sc_kind LIKE '%특수%' THEN '특'
                                ELSE '기타' END as school_level,
                           CASE WHEN sc_kind LIKE '%특수%' THEN 1 ELSE 0 END as is_special,
                           status
                    FROM schools WHERE status = '운영'
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
        """학교 코드로 학교 정보 조회"""
        return self.school_cache.get(school_code)

    def _get_field(self, raw_item: dict, field: str, default=None):
        from constants.api_mappings import get_api_field
        return get_api_field(raw_item, field, context=self.api_context, default=default)

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
                # 지터가 포함된 지수 백오프 (최대 30초)
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
        if not self.quiet_mode:
            print("🔁 [writer_loop] started")
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
                            # None을 다시 넣지 않음
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
                    print(f"🚨 데이터 급감: {e}")
                    self.logger.error(f"🚨 데이터 급감 감지: {e}")
                except Exception as e:
                    print(f"❌ 배치 저장 예외: {e}")
                    self.logger.error(f"배치 저장 실패: {e}", exc_info=True)
                finally:
                    for _ in range(items_processed):
                        self.q.task_done()
        except Exception as e:
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
        today = now_kst().strftime("%Y%m%d")
        backup_path = self.db_path.replace('.db', f'_{today}.db')
        if not os.path.exists(backup_path):
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(f"VACUUM INTO '{backup_path}'")
            self.logger.info(f"📅 날짜 백업 생성: {backup_path}")

        base = self.db_path.replace('.db', '')
        date_pattern = re.compile(rf"^{re.escape(base)}_(\d{{8}})\.db$")
        from .kst_time import KST
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
        """
        통합 출력 메서드.
        - quiet 모드면 모든 출력 차단.
        - level이 'debug'면 debug 모드에서만 출력.
        - 그 외는 항상 출력 (quiet 모드가 아니면).
        """
        if self.quiet_mode:
            return
        if level == "debug" and not (hasattr(self, 'debug_mode') and self.debug_mode):
            return
        print(*args, **kwargs)

    def print_progress(self, current: int, total: int, prefix: str = "", bar_length: int = 20):
        """
        간단한 진행률 바 출력 (디버그 모드 아닐 때만, quiet 모드면 미출력).
        """
        if self.quiet_mode:
            return
        percent = current / total
        filled = int(bar_length * percent)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\r{prefix} [{bar}] {current}/{total} ({percent*100:.1f}%)", end='', flush=True)
        if current == total:
            print()  # 완료 시 줄바꿈

    @abstractmethod
    def _init_db(self):
        pass

    def _init_db_common(self, conn):
        init_checkpoint_table(conn)

    @abstractmethod
    def _get_target_key(self) -> str:
        pass

    @abstractmethod
    def _process_item(self, raw_item: dict) -> Union[dict, List[dict]]:
        pass

    @abstractmethod
    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]) -> None:
        pass

    def _save_batch(self, batch: List[dict]):
        if hasattr(self, 'debug_mode') and self.debug_mode and not self.quiet_mode:
            print(f"🔍 [_save_batch] 저장 시도: {len(batch)}개, DB 경로: {self.db_path}")
        with get_db_connection(self.db_path) as conn:
            self._do_save_batch(conn, batch)
        if hasattr(self, 'debug_mode') and self.debug_mode and not self.quiet_mode:
            print(f"✅ [_save_batch] 저장 완료")

    def _load_checkpoints(self):
        pass

    def save_checkpoint(
        self, region: str, school: str, sub_key: str, page: int, total: int
    ):
        pass

    def close(self, timeout: float = 60.0):
        self.logger.info(f"🔒 {self.name} 종료 시작...")
        self.q.join()
        self.q.put(None)
        self.writer_thread.join(timeout=timeout)
        if self.writer_thread.is_alive():
            self.logger.warning("⚠️ writer_thread가 정상 종료되지 않았습니다.")
        for res in self._closeable_resources:
            try:
                res.close()
            except Exception as e:
                self.logger.warning(f"리소스 close 실패: {e}")
        self.logger.info(f"✅ {self.name} 종료 완료")

    def flush(self, timeout: float = 60.0):
        self.logger.info(f"🔄 {self.name} flush 시작...")
        self.q.join()
        self.logger.info(f"✅ {self.name} flush 완료")
        