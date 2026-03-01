#!/usr/bin/env python3
"""
수집기 베이스 클래스 - 공통 기능 통합
- DataGuard(데이터 급감 감지), CollectLog(실행 로그) 연동
- _save_batch에서 데이터 무결성 검사 후 하위 클래스의 _do_save_batch 호출
"""
import os
import sqlite3
import queue
import threading
import time
import glob
import re
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
from constants.codes import NEIS_API_KEY, MASTER_DB

class BaseCollector(ABC):
    """공통 수집기 베이스 클래스"""
    
    def __init__(self, name: str, base_dir: str, shard: str = "none", school_range: Optional[str] = None):
        self.name = name
        self.base_dir = Path(base_dir)
        self.shard = shard
        self.school_range = school_range
        self.api_context = 'common'  # 하위 클래스에서 재정의
        
        # DB 경로 설정
        if shard == "none":
            self.db_path = str(self.base_dir / f"{name}.db")
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            self.db_path = str(self.base_dir / f"{name}_{shard}{range_suffix}.db")
        
        self.total_db_path = str(self.base_dir / f"{name}_total.db")
        
        # 로거
        self.logger = build_logger(name, str(self.base_dir / f"{name}.log"))
        
        # 네트워크 세션
        self.session = build_session()
        
        # 큐 및 쓰기 스레드
        self.q = queue.Queue()
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.writer_thread.start()
        
        # 학교 캐시
        self.school_cache = {}
        self.school_by_id = {}
        self._load_school_cache()
        
        # 리소스 관리 (자동 close)
        self._closeable_resources = []
        
        # 데이터 보호 및 로깅
        self.data_guard = DataGuard()
        self.collect_log = CollectLog()
        
        # 실패 기록용 DB 초기화
        self._init_failure_db()
        
        # DB 초기화
        self._init_db()
        
        # 체크포인트
        self.completed_items = set()
        self._load_checkpoints()
        
        self.logger.info(f"🔥 {name} 초기화 완료 (샤드: {shard}, 범위: {school_range}, 캐시: {len(self.school_cache)}개)")
    
    def _init_failure_db(self):
        """실패한 수집 내역을 저장할 DB 생성"""
        self.fail_db_path = self.base_dir / f"{self.name}_failures.db"
        with get_db_connection(self.fail_db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT,
                    school_id TEXT,
                    region TEXT,
                    year INTEGER,
                    sub_key TEXT,
                    failed_at TEXT,
                    retries INTEGER DEFAULT 0,
                    resolved INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX idx_failures_unresolved ON failures(resolved)")
    
    def _record_failure(self, school_id=None, region=None, year=None, sub_key=None):
        """실패 내역 기록 (원자적 저장)"""
        with get_db_connection(self.fail_db_path) as conn:
            conn.execute("""
                INSERT INTO failures (domain, school_id, region, year, sub_key, failed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (self.name, school_id, region, year, sub_key, now_kst().isoformat()))
            self.logger.warning(f"실패 기록: {school_id} ({region}/{year})")
    
    def register_resource(self, resource):
        """close() 시 자동으로 닫힐 리소스 등록"""
        self._closeable_resources.append(resource)
        return resource
    
    # --------------------------------------------------------
    # 공통 헬퍼 (이하 동일)
    # --------------------------------------------------------
    def _get_field(self, raw_item: dict, field: str, default=None):
        from constants.api_mappings import get_api_field
        return get_api_field(raw_item, field, context=self.api_context, default=default)
    
    def _include_school(self, school_code: str) -> bool:
        if not school_code:
            return False
        return should_include_school(self.shard, self.school_range, school_code)
    
    # --------------------------------------------------------
    # 공통 페이지네이션 (이하 동일)
    # --------------------------------------------------------
    def _fetch_paginated(self, url: str, base_params: dict, response_key: str,
                         page_size: int = 100, max_page: int = 500) -> List[dict]:
        # ... (기존 코드와 동일)
        pass
    
    # --------------------------------------------------------
    # 학교 목록 조회, iterate_schools_by_month 등 (기존과 동일)
    # --------------------------------------------------------
    
    # --------------------------------------------------------
    # 쓰기 스레드 (DataDropException 처리 강화)
    # --------------------------------------------------------
    def _writer_loop(self):
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
                        self.q.put(None)
                        break
                    items_processed += 1
                    batch.extend(self._flatten(nxt))
                except queue.Empty:
                    break

            if batch:
                try:
                    self._save_batch(batch)
                except DataDropException as e:
                    self.logger.error(f"🚨 데이터 급감 감지: {e}")
                    school_id = batch[0].get('school_id') if batch else None
                    self._record_failure(school_id=school_id)
                except Exception as e:
                    self.logger.error(f"배치 저장 실패: {e}", exc_info=True)
                finally:
                    for _ in range(items_processed):
                        self.q.task_done()
    
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
    
    # --------------------------------------------------------
    # 백업
    # --------------------------------------------------------
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
            fdate_str = match.group(1)
            try:
                fdate = datetime.strptime(fdate_str, '%Y%m%d')
                fdate = KST.localize(fdate)
                if (now_kst() - fdate) > timedelta(days=30):
                    os.remove(f)
                    self.logger.info(f"🗑️ 오래된 백업 삭제: {f}")
            except ValueError:
                continue
    
    # --------------------------------------------------------
    # 종료 처리
    # --------------------------------------------------------
    def close(self, timeout: float = 30.0):
        for res in self._closeable_resources:
            try:
                res.close()
            except Exception as e:
                self.logger.warning(f"리소스 close 실패: {e}")
        self.q.put(None)
        self.writer_thread.join(timeout=timeout)
        if self.writer_thread.is_alive():
            self.logger.warning("⚠️ writer_thread가 정상 종료되지 않았습니다.")
            