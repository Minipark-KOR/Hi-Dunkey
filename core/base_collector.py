#!/usr/bin/env python3
"""
수집기 베이스 클래스 - 공통 기능 통합
"""
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
from constants.paths import MASTER_DB_PATH as MASTER_DB
from core.retry import RetryManager


class BaseCollector(ABC):

    def __init__(
        self,
        name: str,
        base_dir: str,
        shard: str = "none",
        school_range: Optional[str] = None
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

        if shard == "none":
            self.db_path = str(self.base_dir / f"{name}.db")
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            self.db_path = str(self.base_dir / f"{name}_{shard}{range_suffix}.db")

        self.total_db_path = str(self.base_dir / f"{name}_total.db")
        self.logger = build_logger(name, str(self.base_dir / f"{name}.log"))
        print(f"📝 로그 파일: {self.base_dir / f'{name}.log'}")

        self.session = build_session()

        self.q = queue.Queue()
        self.writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name=f"{name}_writer"
        )
        self.writer_thread.start()
        print(f"🔁 writer_thread 시작: {self.writer_thread.name}")

        self.school_cache: Dict[str, Any] = {}
        self.school_by_id: Dict[str, Any] = {}
        self._load_school_cache()

        self._closeable_resources = []
        self.data_guard = DataGuard()
        self.collect_log = CollectLog()
        self.retry_mgr = RetryManager(db_path="data/failures.db")

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
            with sqlite3.connect(MASTER_DB) as conn:
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
                time.sleep(2 ** retry_count)

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
        # 강제 출력 및 flush
        print("🔁 [writer_loop] started", flush=True)
        try:
            while True:
                item = self.q.get()
                if item is None:
                    print("🔁 [writer_loop] received None, exiting", flush=True)
                    self.q.task_done()
                    break

                items_processed = 1
                batch = self._flatten(item)

                while len(batch) < 500:
                    try:
                        nxt = self.q.get_nowait()
                        if nxt is None:
                            print("🔁 [writer_loop] received None in batch", flush=True)
                            self.q.task_done()
                            self.q.put(None)
                            break
                        items_processed += 1
                        batch.extend(self._flatten(nxt))
                    except queue.Empty:
                        break

                print(f"🔍 [writer_loop] 배치 수집 완료: {len(batch)}개", flush=True)

                try:
                    if batch:
                        print(f"🔍 [writer_loop] _save_batch 호출 전", flush=True)
                        self._save_batch(batch)
                        print(f"✅ [writer_loop] _save_batch 정상 종료", flush=True)
                except DataDropException as e:
                    print(f"🚨 데이터 급감: {e}", flush=True)
                    self.logger.error(f"🚨 데이터 급감 감지: {e}")
                except Exception as e:
                    print(f"❌ 배치 저장 예외: {e}", flush=True)
                    traceback.print_exc()
                    self.logger.error(f"배치 저장 실패: {e}", exc_info=True)
                finally:
                    for _ in range(items_processed):
                        self.q.task_done()
        except Exception as e:
            print(f"💥 writer_loop 치명적 예외: {e}", flush=True)
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
        print(f"🔍 [_save_batch] 저장 시도: {len(batch)}개, DB 경로: {self.db_path}", flush=True)
        with get_db_connection(self.db_path) as conn:
            self._do_save_batch(conn, batch)
        print(f"✅ [_save_batch] 저장 완료", flush=True)

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
        