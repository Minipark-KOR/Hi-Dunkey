#!/usr/bin/env python3
"""
시간표 수집기 (timetable)
- BaseCollector 기반
- 샤딩 지원 (odd/even)
- 학교 코드 범위 필터링 지원 (A/B)
- vocab_subject, vocab_teacher 테이블로 과목명/교사명 관리
- IDGenerator 기반 고정 ID 생성
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Optional

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

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['timetable']


class TimetableCollector(BaseCollector):
    """시간표 수집기 - BaseCollector 기반"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False):
        """
        Args:
            shard: 샤드 타입 ('odd', 'even', 'none')
            school_range: 학교 코드 범위 ('A', 'B', None)
            incremental: 증분 수집 여부
            full: 전체 수집 후 백업 여부
        """
        super().__init__("timetable", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.run_ay = get_current_school_year()
        
        # 과목/교사 vocab 캐시
        self.subject_cache = {}      # subject_name -> subject_id
        self.teacher_cache = {}      # teacher_name -> teacher_id
        self._load_vocab_caches()
        
        self.logger.info(f"📚 TimetableCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _load_vocab_caches(self):
        """과목/교사 vocab 캐시 로드"""
        try:
            with get_db_connection(self.db_path) as conn:
                # 과목 캐시
                cur = conn.execute("SELECT subject_id, subject_name FROM vocab_subject")
                for subject_id, subject_name in cur:
                    self.subject_cache[subject_name] = subject_id
                
                # 교사 캐시
                cur = conn.execute("SELECT teacher_id, teacher_name FROM vocab_teacher")
                for teacher_id, teacher_name in cur:
                    self.teacher_cache[teacher_name] = teacher_id
                    
            self.logger.info(f"✅ 과목 캐시: {len(self.subject_cache)}개, 교사 캐시: {len(self.teacher_cache)}개")
        except Exception as e:
            self.logger.error(f"vocab 캐시 로드 실패: {e}")
    
    def _init_db(self):
        """DB 테이블 초기화"""
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
            
            # 교사 vocab 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_teacher (
                    teacher_id   INTEGER PRIMARY KEY,
                    teacher_name TEXT NOT NULL UNIQUE,
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 시간표 메인 테이블
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
                    teacher_id  INTEGER,
                    load_dt     TEXT,
                    PRIMARY KEY (school_id, ay, semester, grade, class_nm, day_of_week, period),
                    FOREIGN KEY (subject_id) REFERENCES vocab_subject(subject_id),
                    FOREIGN KEY (teacher_id) REFERENCES vocab_teacher(teacher_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timetable_school ON timetable(school_id, ay)")
            
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        """체크포인트용 대상 키"""
        return str(self.run_ay)
    
    def _get_subject_id(self, subject_name: str, normalized_key: str, level: str) -> int:
        """과목명으로 subject_id 조회/생성"""
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
    
    def _get_teacher_id(self, teacher_name: str) -> int:
        """교사명으로 teacher_id 조회/생성"""
        if not teacher_name:
            return 0
        
        if teacher_name in self.teacher_cache:
            return self.teacher_cache[teacher_name]
        
        teacher_id = IDGenerator.text_to_int(
            text=teacher_name,
            namespace="teacher",
            bits=63
        )
        
        self.teacher_cache[teacher_name] = teacher_id
        return teacher_id
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 아이템 처리"""
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code:
            return []
        
        if not self._include_school(sc_code):
            return []
        
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']

        # ✅ parsed를 먼저 정의
        parsed = parse_timetable_row(raw_item)
        
        if not parsed.get("subject_name"):
            return []

        subject_id = self._get_subject_id(
            parsed["subject_name"],
            parsed.get("normalized_key", ""),
            parsed.get("level", "")
        )
        
        teacher_id = self._get_teacher_id(parsed.get("teacher_name", ""))

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
            "teacher_id":     teacher_id,
            "teacher_name":   parsed.get("teacher_name", ""),
            "load_dt":        now_kst().isoformat()
        }]
    
    def _save_batch(self, batch: List[dict]):
        """배치 데이터 저장"""
        with get_db_connection(self.db_path) as conn:
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
            
            teacher_set = {
                (it['teacher_id'], it['teacher_name'])
                for it in batch if it.get('teacher_id')
            }
            if teacher_set:
                conn.executemany(
                    "INSERT OR IGNORE INTO vocab_teacher "
                    "(teacher_id, teacher_name) VALUES (?,?)",
                    list(teacher_set)
                )
            
            tt_data = [
                (
                    it['school_id'], it['ay'], it['semester'], it['grade'], it['class_nm'],
                    it['day_of_week'], it['period'], it['subject_id'], it['teacher_id'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO timetable VALUES (?,?,?,?,?,?,?,?,?,?)",
                tt_data
            )
    
    def fetch_school_timetable(self, school_code: str, ay: int, semester: int):
        """특정 학교의 시간표 수집 (학년/반 순회)"""
        school_info = self.get_school_info(school_code)
        if not school_info:
            return

        level = school_info.get('level', '')
        grades = range(1, 7) if level == '초' else range(1, 4)

        for grade in grades:
            empty_count = 0
            for class_nm in map(str, range(1, 21)):
                params = {
                    "KEY":  NEIS_API_KEY,
                    "Type": "json",
                    "pIndex": 1,
                    "pSize":  1000,
                    "ATPT_OFCDC_SC_CODE": school_info['atpt_code'],
                    "SD_SCHUL_CODE": school_code,
                    "AY":       str(ay),
                    "GRADE":    str(grade),
                    "CLASS_NM": class_nm,
                    "SEM":      str(semester)
                }
                try:
                    res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                    if not res or "classTimeTable" not in res:
                        empty_count += 1
                        if empty_count >= 3:
                            break
                        continue

                    rows = res["classTimeTable"][1].get("row", [])
                    if not rows:
                        empty_count += 1
                        if empty_count >= 3:
                            break
                        continue

                    empty_count = 0
                    batch = []
                    for r in rows:
                        items = self._process_item(r)
                        if items:
                            batch.extend(items)

                    if batch:
                        self.enqueue(batch)
                    self.logger.info(
                        f"{school_code} {grade}학년 {class_nm}반 → {len(rows)}건"
                    )
                except Exception as e:
                    self.logger.error(
                        f"{school_code} {grade}학년 {class_nm}반 실패: {e}"
                    )
                time.sleep(0.1)
    
    def close(self):
        """종료 처리"""
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="시간표 수집기")
    parser.add_argument("--ay", type=int, default=get_current_school_year(), help="학년도")
    parser.add_argument("--semester", type=int, default=1, choices=[1, 2])
    parser.add_argument("--regions", help="B10,C10,... 또는 ALL (미지정시 모든 학교)")
    parser.add_argument("--shard", default="none", choices=["odd", "even", "none"],
                       help="샤드 필터 (odd=홀수, even=짝수, none=전체)")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none",
                       help="학교 코드 범위 필터 (A=1-4, B=5-9)")
    parser.add_argument("--incremental", action="store_true", help="증분 수집 모드")
    parser.add_argument("--full", action="store_true", help="전체 수집 후 백업 생성")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    school_range = None if args.school_range == "none" else args.school_range

    collector = TimetableCollector(
        shard=args.shard,
        school_range=school_range,
        incremental=args.incremental,
        full=args.full
    )

    # 수집 대상 학교 결정
    if args.regions:
        if args.regions.upper() == "ALL":
            regions = set(ALL_REGIONS)
        else:
            regions = {r.strip() for r in args.regions.split(",")}
        
        schools = []
        for sc_code, info in collector.school_cache.items():
            if info['atpt_code'] in regions and collector._include_school(sc_code):
                schools.append(sc_code)
    else:
        schools = [sc_code for sc_code in collector.school_cache.keys() 
                  if collector._include_school(sc_code)]

    collector.logger.info(f"🚀 시간표 수집 시작: {len(schools)}개 학교, {args.ay}학년도 {args.semester}학기")
    if args.debug:
        collector.logger.info(f"  shard={args.shard}, range={school_range}")

    for sc_code in schools:
        collector.fetch_school_timetable(sc_code, args.ay, args.semester)

    collector.close()
    collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
    