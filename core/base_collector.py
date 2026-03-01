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
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable

from .database import get_db_connection, init_checkpoint_table
from .logger import build_logger
from .network import build_session, safe_json_request
from .shard import should_include_school
from .kst_time import now_kst
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
        
        # DB 초기화
        self._init_db()
        
        # 체크포인트
        self.completed_items = set()
        self._load_checkpoints()
        
        self.logger.info(f"🔥 {name} 초기화 완료 (샤드: {shard}, 범위: {school_range}, 캐시: {len(self.school_cache)}개)")
    
    # --------------------------------------------------------
    # 공통 헬퍼
    # --------------------------------------------------------
    def _get_field(self, raw_item: dict, field: str, default=None):
        """API 응답에서 내부 필드명으로 값 추출 (컨텍스트 자동 적용)"""
        from constants.api_mappings import get_api_field
        return get_api_field(raw_item, field, context=self.api_context, default=default)
    
    def _include_school(self, school_code: str) -> bool:
        if not school_code:
            return False
        return should_include_school(self.shard, self.school_range, school_code)
    
    # --------------------------------------------------------
    # 공통 페이지네이션 (지수 백오프 재시도)
    # --------------------------------------------------------
    def _fetch_paginated(self, url: str, base_params: dict, response_key: str,
                         page_size: int = 100, max_page: int = 500) -> List[dict]:
        """
        공통 페이지네이션 처리 (재시도 포함)
        - url: API 엔드포인트
        - base_params: API 공통 파라미터 (KEY, Type 등은 자동 추가)
        - response_key: 응답 JSON에서 데이터 배열이 있는 키
        - 반환: 모든 페이지의 raw item 리스트
        """
        all_items = []
        p_idx = 1
        retry_count = 0
        max_retries = 3
        
        while p_idx <= max_page:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": page_size,
                **base_params
            }
            try:
                res = safe_json_request(self.session, url, params, self.logger)
                if not res or response_key not in res:
                    break
                
                # NEIS API 구조: [0] head, [1] row
                rows = res[response_key][1].get("row", [])
                if not rows:
                    break
                
                all_items.extend(rows)
                
                # 전체 개수 체크 (head에 total_count 있음)
                head = res[response_key][0].get("head", [{}])
                total_count = int(head[0].get("list_total_count", 0))
                if len(all_items) >= total_count:
                    break
                
                p_idx += 1
                retry_count = 0  # 성공 시 리셋
                time.sleep(0.05)
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"페이지 {p_idx} 에러 ({retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    self.logger.error(f"페이지 {p_idx} {max_retries}회 실패, 중단")
                    break
                time.sleep(2 ** retry_count)  # 지수 백오프
                # p_idx 증가 없이 재시도
        
        return all_items
    
    # --------------------------------------------------------
    # 학교 목록 조회 (캐시 활용)
    # --------------------------------------------------------
    def _get_school_list(self, region: str) -> List[tuple]:
        """지역 내 학교 목록 반환 (코드, 이름) - 캐시 활용"""
        return [
            (code, info['name'])
            for code, info in self.school_cache.items()
            if info['atpt_code'] == region and self._include_school(code)
        ]
    
    # --------------------------------------------------------
    # 학교별 월간 순회 (진행 로그 포함)
    # --------------------------------------------------------
    def iterate_schools_by_month(self, region: str, year: int,
                                  month_range: List[tuple],
                                  per_school_month_func: Callable[[str, int, int], None]):
        """
        학교별 월간 순회
        - month_range: [(y,m)] 형태의 월 리스트
        - per_school_month_func: (school_code, y, m)을 받는 함수
        """
        schools = self._get_school_list(region)
        if not schools:
            self.logger.warning(f"⚠️ [{region}] 수집 대상 학교 없음")
            return

        for idx, (sch_code, sch_name) in enumerate(schools, 1):
            self.logger.info(f"  [{region}] 진행 {idx}/{len(schools)}: {sch_name} ({sch_code})")
            for y, m in month_range:
                try:
                    per_school_month_func(sch_code, y, m)
                except Exception as e:
                    self.logger.error(f"    ❌ {y}년 {m}월 실패: {e}")
                time.sleep(0.05)
            time.sleep(0.1)
    
    # --------------------------------------------------------
    # 학교별 단순 순회 (진행 로그 포함, 학사일정 등에 사용)
    # --------------------------------------------------------
    def iterate_schools(self, region: str, per_school_func: Callable[[str, str], None]):
        """학교 목록을 순회하며 per_school_func 실행 (진행 로그 포함)"""
        schools = self._get_school_list(region)
        if not schools:
            self.logger.warning(f"⚠️ [{region}] 수집 대상 학교 없음")
            return
        
        for idx, (sch_code, sch_name) in enumerate(schools, 1):
            self.logger.info(f"  [{region}] 진행 {idx}/{len(schools)}: {sch_name} ({sch_code})")
            try:
                per_school_func(sch_code, sch_name)
            except Exception as e:
                self.logger.error(f"    ❌ {sch_code} 처리 실패: {e}")
            time.sleep(0.05)
    
    # --------------------------------------------------------
    # 학교정보 캐시 (MASTER_DB 상수 사용)
    # --------------------------------------------------------
    def _load_school_cache(self):
        """school_master.db 에서 운영중인 학교 정보 로드 (school_id 생성)"""
        if not os.path.exists(MASTER_DB):
            self.logger.warning("school_master.db 없음. 캐시 없이 동작")
            return
        
        try:
            with sqlite3.connect(MASTER_DB) as conn:
                cur = conn.execute("""
                    SELECT 
                        sc_code,
                        atpt_code,
                        ((CASE atpt_code
                            WHEN 'B10' THEN 1 WHEN 'C10' THEN 2 WHEN 'D10' THEN 3
                            WHEN 'E10' THEN 4 WHEN 'F10' THEN 5 WHEN 'G10' THEN 6
                            WHEN 'H10' THEN 7 WHEN 'I10' THEN 8 WHEN 'J10' THEN 9
                            WHEN 'K10' THEN 10 
                            -- 11번: L10 미사용
                            WHEN 'M10' THEN 12 WHEN 'N10' THEN 13 WHEN 'P10' THEN 14
                            WHEN 'Q10' THEN 15 WHEN 'R10' THEN 16 WHEN 'S10' THEN 17
                            WHEN 'T10' THEN 18
                            WHEN 'A00' THEN 21
                            WHEN 'Z01' THEN 22 WHEN 'Z10' THEN 23 WHEN 'Z11' THEN 24
                            WHEN 'Z12' THEN 25 WHEN 'Z20' THEN 26 WHEN 'Z21' THEN 27
                            WHEN 'Z22' THEN 28 WHEN 'Z23' THEN 29 WHEN 'Z99' THEN 31
                            ELSE 0 END) << 24) | CAST(sc_code AS INTEGER) as school_id,
                        sc_name,
                        sc_kind,
                        CASE 
                            WHEN sc_kind LIKE '%초등%' THEN '초'
                            WHEN sc_kind LIKE '%중%' THEN '중'
                            WHEN sc_kind LIKE '%고%' THEN '고'
                            WHEN sc_kind LIKE '%특수%' THEN '특'
                            ELSE '기타'
                        END as school_level,
                        CASE WHEN sc_kind LIKE '%특수%' THEN 1 ELSE 0 END as is_special,
                        status
                    FROM schools
                    WHERE status = '운영'
                """)
                for sc_code, atpt_code, school_id, sc_name, sc_kind, level, is_special, status in cur:
                    info = {
                        'atpt_code': atpt_code,
                        'school_id': school_id,
                        'name': sc_name,
                        'kind': sc_kind,
                        'level': level,
                        'is_special': is_special,
                        'status': status,
                    }
                    self.school_cache[sc_code] = info
                    self.school_by_id[school_id] = info
        except Exception as e:
            self.logger.error(f"학교 캐시 로드 실패: {e}")
    
    def get_school_info(self, school_code: str) -> Optional[Dict]:
        return self.school_cache.get(school_code)
    
    def get_school_by_id(self, school_id: int) -> Optional[Dict]:
        return self.school_by_id.get(school_id)
    
    # --------------------------------------------------------
    # 추상 메서드
    # --------------------------------------------------------
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
    def _save_batch(self, batch: List[dict]):
        pass
    
    # --------------------------------------------------------
    # 체크포인트
    # --------------------------------------------------------
    def _load_checkpoints(self):
        try:
            with get_db_connection(self.db_path, timeout=10.0) as conn:
                cur = conn.execute("""
                    SELECT region_code, school_code, sub_key
                    FROM collection_checkpoint
                    WHERE collector_type = ?
                """, (self.name,))
                for row in cur:
                    self.completed_items.add(f"{row[0]}|{row[1]}|{row[2]}")
        except Exception:
            pass
    
    def save_checkpoint(self, region: str, school: str, sub_key: str, page: int, total: int):
        with get_db_connection(self.db_path, timeout=10.0) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO collection_checkpoint
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.name, self._get_target_key(), region, school, sub_key,
                  page, total, now_kst().isoformat()))
    
    # --------------------------------------------------------
    # 쓰기 스레드
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
                except Exception as e:
                    self.logger.error(f"배치 저장 실패: {e}")
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
    
    def close(self, timeout: float = 30.0):
        self.q.put(None)
        self.writer_thread.join(timeout=timeout)
        if self.writer_thread.is_alive():
            self.logger.warning("⚠️ writer_thread가 정상 종료되지 않았습니다.")
            