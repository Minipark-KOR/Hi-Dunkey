#!/usr/bin/env python3
"""
수집기 베이스 클래스 (모든 Collector의 공통 부모)
"""
import os
import sqlite3
import queue
import threading
import time
import glob
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

from .database import get_db_connection, init_checkpoint_table
from .logger import build_logger
from .network import build_session
from .school_id import create_school_id
from .shard import should_include
from .kst_time import now_kst

class BaseCollector(ABC):
    """
    공통 수집기 베이스 클래스
    - 샤딩 지원
    - 학교정보 캐싱
    - 날짜 백업 생성 및 30일 초과 백업 자동 삭제
    - 쓰기 스레드 (queue 기반)
    """
    
    def __init__(self, name: str, base_dir: str, shard: str = "none"):
        self.name = name
        self.base_dir = base_dir
        self.shard = shard
        
        # DB 경로 설정 (샤딩)
        if shard == "none":
            self.db_path = os.path.join(base_dir, f"{name}.db")
        else:
            self.db_path = os.path.join(base_dir, f"{name}_{shard}.db")
        
        self.total_db_path = os.path.join(base_dir, f"{name}_total.db")
        
        # 로거
        self.logger = build_logger(name, os.path.join(base_dir, f"{name}.log"))
        
        # 네트워크 세션
        self.session = build_session()
        
        # 큐 및 쓰기 스레드
        self.q = queue.Queue()
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.writer_thread.start()
        
        # 학교정보 캐시 (school_master.db 참조)
        self.school_cache = {}      # key: school_code
        self.school_by_id = {}      # key: school_id (int)
        self._load_school_cache()
        
        # DB 초기화
        self._init_db()
        
        # 체크포인트 로드
        self.completed_items = set()
        self._load_checkpoints()
        
        self.logger.info(f"🔥 {name} 수집기 초기화 완료 (샤드: {shard}, 학교 캐시: {len(self.school_cache)}개)")
    
    # --------------------------------------------------------
    # 학교정보 캐시
    # --------------------------------------------------------
    def _load_school_cache(self):
        """school_master.db 에서 운영중인 학교 정보 로드 (필수 컬럼만)"""
        master_path = os.path.join(os.path.dirname(self.base_dir), "master", "school_master.db")
        if not os.path.exists(master_path):
            self.logger.warning("school_master.db 가 없습니다. 학교정보 캐시 없이 동작합니다.")
            return
        
        try:
            with sqlite3.connect(master_path) as conn:
                # school_id 도 함께 생성
                cur = conn.execute("""
                    SELECT 
                        sc_code,
                        atpt_code,
                        ((CASE atpt_code
                            WHEN 'B10' THEN 1 WHEN 'C10' THEN 2 WHEN 'D10' THEN 3
                            WHEN 'E10' THEN 4 WHEN 'F10' THEN 5 WHEN 'G10' THEN 6
                            WHEN 'H10' THEN 7 WHEN 'I10' THEN 8 WHEN 'J10' THEN 9
                            WHEN 'K10' THEN 10 WHEN 'L10' THEN 11 WHEN 'M10' THEN 12
                            WHEN 'N10' THEN 13 WHEN 'P10' THEN 14 WHEN 'Q10' THEN 15
                            WHEN 'R10' THEN 16 WHEN 'S10' THEN 17 WHEN 'T10' THEN 18
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
            self.logger.error(f"학교정보 캐시 로드 실패: {e}")
    
    def get_school_info(self, school_code: str) -> Optional[Dict]:
        """학교 코드로 학교정보 조회"""
        return self.school_cache.get(school_code)
    
    def get_school_by_id(self, school_id: int) -> Optional[Dict]:
        """school_id로 학교정보 조회"""
        return self.school_by_id.get(school_id)
    
    def is_special_school(self, school_code: str) -> bool:
        """특수학교 여부 (DB 우선, 없으면 False)"""
        info = self.get_school_info(school_code)
        return info['is_special'] if info else False
    
    def is_active_school(self, school_code: str) -> bool:
        """운영중인 학교인지 확인 (캐시에 있으면 운영)"""
        return school_code in self.school_cache
    
    def log_school(self, school_code: str, prefix: str = ""):
        """로깅용 학교명 출력"""
        info = self.get_school_info(school_code)
        if info:
            self.logger.info(f"{prefix} {info['name']}({school_code})")
        else:
            self.logger.info(f"{prefix} {school_code}")
    
    # --------------------------------------------------------
    # 추상 메서드
    # --------------------------------------------------------
    @abstractmethod
    def _init_db(self):
        """DB 스키마 초기화 (하위 클래스에서 구현)"""
        pass
    
    @abstractmethod
    def _get_target_key(self) -> str:
        """체크포인트용 대상 키 (예: 날짜, 학년도)"""
        pass
    
    @abstractmethod
    def _process_item(self, raw_item: dict) -> Union[dict, List[dict]]:
        """
        API 응답 아이템 1건을 처리하여 저장할 데이터로 변환
        - dict 또는 list 반환 가능 (list는 여러 개로 분할)
        """
        pass
    
    @abstractmethod
    def _save_batch(self, batch: List[dict]):
        """배치 데이터 저장 (하위 클래스에서 구현)"""
        pass
    
    # --------------------------------------------------------
    # 체크포인트
    # --------------------------------------------------------
    def _load_checkpoints(self):
        """이미 완료된 항목 로드 (선택 사항)"""
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
        """체크포인트 저장"""
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO collection_checkpoint
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.name, self._get_target_key(), region, school, sub_key,
                  page, total, now_kst().isoformat()))
    
    # --------------------------------------------------------
    # 쓰기 스레드 (배치 처리)
    # --------------------------------------------------------
    def _writer_loop(self):
        """백그라운드 쓰기 스레드"""
        while True:
            item = self.q.get()
            if item is None:
                self.q.task_done()
                break
            
            batch = self._flatten(item)
            # 간단한 배치 크기 제한 (500건)
            while len(batch) < 500:
                try:
                    nxt = self.q.get_nowait()
                    if nxt is None:
                        self.q.put(None)
                        break
                    batch.extend(self._flatten(nxt))
                except queue.Empty:
                    break
            
            if batch:
                try:
                    self._save_batch(batch)
                except Exception as e:
                    self.logger.error(f"배치 저장 실패: {e}")
                finally:
                    for _ in range(len(batch)):
                        self.q.task_done()
    
    def _flatten(self, data) -> List:
        """데이터를 1차원 리스트로 평탄화"""
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
        """큐에 데이터 추가"""
        self.q.put(data)
    
    # --------------------------------------------------------
    # 날짜 백업 생성 및 정리
    # --------------------------------------------------------
    def create_dated_backup(self):
        """
        active 주 파일의 날짜 백업 생성 (VACUUM INTO)
        - 같은 날짜 백업이 이미 있으면 생성하지 않음
        - 30일 초과된 날짜 백업 삭제
        """
        if not os.path.exists(self.db_path):
            return
        today = now_kst().strftime("%Y%m%d")
        backup_path = self.db_path.replace('.db', f'_{today}.db')
        
        if not os.path.exists(backup_path):
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(f"VACUUM INTO '{backup_path}'")
            self.logger.info(f"📅 날짜 백업 생성: {backup_path}")
        
        # 30일 초과된 날짜 백업 삭제
        base = self.db_path.replace('.db', '')
        for f in glob.glob(f"{base}_*.db"):
            try:
                fdate_str = f.split('_')[-1].replace('.db', '')
                fdate = datetime.strptime(fdate_str, '%Y%m%d')
                if (now_kst() - fdate) > timedelta(days=30):
                    os.remove(f)
                    self.logger.info(f"🗑️ 오래된 날짜 백업 삭제: {f}")
            except (ValueError, IndexError):
                continue
    
    # --------------------------------------------------------
    # 종료 처리
    # --------------------------------------------------------
    def close(self):
        """정리: 큐 종료 및 스레드 조인"""
        self.q.put(None)
        self.writer_thread.join()
        