#!/usr/bin/env python3
"""
시간표 수집기 (timetable) - 교사 정보 제거 버전
- 과목명만 저장
- 패턴 저장으로 최적화
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Optional
import hashlib

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from parsers.timetable_parser import parse_timetable_row
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.id_generator import IDGenerator


class TimetableCollector(BaseCollector):
    """시간표 수집기 - 교사 정보 제거"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False):
        super().__init__("timetable", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.run_ay = get_current_school_year()
        
        # 과목 vocab 캐시
        self.subject_cache = {}      # subject_name -> subject_id
        
        # 시간표 패턴 캐시 (새로 추가!)
        self.pattern_cache = {}      # pattern_hash -> pattern_id
        
        self._load_subject_cache()
        self._load_pattern_cache()
        
        self.logger.info(f"📚 TimetableCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _load_subject_cache(self):
        """과목 vocab 캐시 로드"""
        try:
            with get_db_connection(self.db_path) as conn:
                cur = conn.execute("SELECT subject_id, subject_name FROM vocab_subject")
                for subject_id, subject_name in cur:
                    self.subject_cache[subject_name] = subject_id
            self.logger.info(f"✅ 과목 캐시: {len(self.subject_cache)}개")
        except Exception as e:
            self.logger.error(f"과목 캐시 로드 실패: {e}")
    
    def _load_pattern_cache(self):
        """시간표 패턴 캐시 로드"""
        try:
            with get_db_connection(self.db_path) as conn:
                cur = conn.execute("SELECT pattern_id, pattern_hash FROM timetable_patterns")
                for pattern_id, pattern_hash in cur:
                    self.pattern_cache[pattern_hash] = pattern_id
            self.logger.info(f"✅ 패턴 캐시: {len(self.pattern_cache)}개")
        except Exception as e:
            self.logger.error(f"패턴 캐시 로드 실패: {e}")
    
    def _init_db(self):
        """DB 테이블 초기화 (교사 테이블 제거)"""
        with get_db_connection(self.db_path) as conn:
            # 과목 vocab 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_subject (
                    subject_id     INTEGER PRIMARY KEY,
                    subject_name   TEXT NOT NULL UNIQUE,
                    normalized_key TEXT,
                    level          TEXT,
                    created_at     TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # ✅ 시간표 패턴 테이블 (새로 추가!)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timetable_patterns (
                    pattern_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_hash TEXT UNIQUE NOT NULL,
                    period_1     INTEGER,
                    period_2     INTEGER,
                    period_3     INTEGER,
                    period_4     INTEGER,
                    period_5     INTEGER,
                    period_6     INTEGER,
                    period_7     INTEGER,
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 시간표 메인 테이블 (교사 ID 제거)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timetable (
                    school_id   INTEGER NOT NULL,
                    ay          INTEGER NOT NULL,
                    semester    INTEGER NOT NULL,
                    grade       INTEGER NOT NULL,
                    class_nm    TEXT NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    period      INTEGER NOT NULL,
                    subject_id  INTEGER NOT NULL,
                    pattern_id  INTEGER,  -- ✅ 패턴 ID 추가!
                    load_dt     TEXT,
                    PRIMARY KEY (school_id, ay, semester, grade, class_nm, day_of_week, period),
                    FOREIGN KEY (subject_id) REFERENCES vocab_subject(subject_id),
                    FOREIGN KEY (pattern_id) REFERENCES timetable_patterns(pattern_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timetable_school ON timetable(school_id, ay)")
            
            self._init_db_common(conn)
    
    def _get_pattern_hash(self, periods: List[int]) -> str:
        """시간표 패턴 해시 생성"""
        pattern_str = ','.join(str(p) for p in periods)
        return hashlib.md5(pattern_str.encode()).hexdigest()
    
    def _get_pattern_id(self, periods: List[int]) -> int:
        """패턴 ID 조회/생성"""
        pattern_hash = self._get_pattern_hash(periods)
        
        if pattern_hash in self.pattern_cache:
            return self.pattern_cache[pattern_hash]
        
        with get_db_connection(self.db_path) as conn:
            cur = conn.execute("""
                INSERT INTO timetable_patterns 
                (pattern_hash, period_1, period_2, period_3, period_4, period_5, period_6, period_7)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING pattern_id
            """, (pattern_hash, *periods))
            pattern_id = cur.fetchone()[0]
            
            self.pattern_cache[pattern_hash] = pattern_id
            return pattern_id
    
    def _get_subject_id(self, subject_name: str, normalized_key: str, level: str) -> int:
        """과목 ID 조회/생성"""
        if not subject_name:
            return 0
        
        if subject_name in self.subject_cache:
            return self.subject_cache[subject_name]
        
        subject_id = IDGenerator.text_to_int(
            text=normalized_key or subject_name,
            namespace="subject",
            bits=63
        )
        
        self.subject_cache[subject_name] = subject_id
        return subject_id
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 아이템 처리 (교사 정보 제거)"""
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code:
            return []
        
        if not self._include_school(sc_code):
            return []
        
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']

        parsed = parse_timetable_row(raw_item)
        if not parsed.get("subject_name"):
            return []

        subject_id = self._get_subject_id(
            parsed["subject_name"],
            parsed.get("normalized_key", ""),
            parsed.get("level", "")
        )

        return [{
            "school_id":      school_id,
            "ay":             parsed["ay"],
            "semester":       parsed["semester"],
            "grade":          parsed["grade"],
            "class_nm":       parsed["class_nm"],
            "day_of_week":    parsed["day_of_week"],
            "period":         parsed["period"],
            "subject_id":     subject_id,
            "subject_name":   parsed["subject_name"],
            "normalized_key": parsed.get("normalized_key", ""),
            "level":          parsed.get("level", ""),
            "load_dt":        now_kst().isoformat()
        }]
    
    def _save_batch(self, batch: List[dict]):
        """배치 데이터 저장 (패턴 최적화)"""
        with get_db_connection(self.db_path) as conn:
            # 과목 vocab 저장
            subj_set = {
                (it['subject_id'], it['subject_name'], it['normalized_key'], it['level'])
                for it in batch if it.get('subject_id')
            }
            if subj_set:
                conn.executemany(
                    "INSERT OR IGNORE INTO vocab_subject "
                    "(subject_id, subject_name, normalized_key, level) VALUES (?,?,?,?)",
                    list(subj_set)
                )
            
            # 시간표 저장 (패턴 ID는 아직 없음)
            # 실제로는 주단위로 패턴을 찾아서 저장해야 함
            for it in batch:
                # TODO: 같은 학년/반의 주간 패턴을 모아서 pattern_id 생성
                pattern_id = None
                
                conn.execute("""
                    INSERT OR REPLACE INTO timetable 
                    (school_id, ay, semester, grade, class_nm, day_of_week, period, 
                     subject_id, pattern_id, load_dt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    it['school_id'], it['ay'], it['semester'], it['grade'], it['class_nm'],
                    it['day_of_week'], it['period'], it['subject_id'], pattern_id, it['load_dt']
                ))
    