#!/usr/bin/env python3
"""
시간표 수집기
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime
from typing import List, Dict

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.shard import should_include
from parsers.timetable_parser import parse_timetable_row
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['timetable']


class TimetableCollector(BaseCollector):
    def __init__(self, shard="none", incremental=False, full=False):
        super().__init__("timetable", BASE_DIR, shard)
        self.incremental = incremental
        self.full = full
        self.run_ay = get_current_school_year()

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_subject (
                    subject_id     INTEGER PRIMARY KEY,
                    subject_name   TEXT NOT NULL UNIQUE,
                    normalized_key TEXT,
                    level          TEXT,
                    created_at     TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_teacher (
                    teacher_id   INTEGER PRIMARY KEY,
                    teacher_name TEXT NOT NULL UNIQUE,
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
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
        return str(self.run_ay)

    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code:
            return []
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']

        parsed = parse_timetable_row(raw_item)
        if not parsed.get("subject_id"):
            return []

        return [{
            "school_id":      school_id,
            "ay":             parsed["ay"],
            "semester":       parsed["semester"],
            "grade":          parsed["grade"],
            "class_nm":       parsed["class_nm"],
            "day_of_week":    parsed["day_of_week"],
            "period":         parsed["period"],
            "subject_id":     parsed["subject_id"],
            "subject_name":   parsed["subject_name"],
            "normalized_key": parsed["normalized_key"],
            "level":          parsed["level"],
            "teacher_id":     parsed["teacher_id"],
            "teacher_name":   parsed["teacher_name"],
            "load_dt":        now_kst().isoformat()
        }]

    def _save_batch(self, batch: List[dict]):
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

    # --------------------------------------------------------
    # 학교별 시간표 수집
    # --------------------------------------------------------
    def fetch_school_timetable(self, school_code: str, ay: int, semester: int):
        """특정 학교의 시간표 수집 (학년/반 순회)"""
        school_info = self.get_school_info(school_code)
        if not school_info:
            return

        level = school_info.get('level', '')
        grades = range(1, 7) if level == '초' else range(1, 4)

        for grade in grades:
            empty_count = 0
            for class_nm in map(str, range(1, 21)):  # 1~20반
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

                    # 데이터 있으면 empty_count 리셋
                    empty_count = 0
                    batch = []
                    for r in rows:
                        batch.extend(self._process_item(r))

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
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="시간표 수집기")
    parser.add_argument("--ay", type=int, default=get_current_school_year(), help="학년도")
    parser.add_argument("--semester", type=int, default=1, choices=[1, 2])
    parser.add_argument("--regions", help="B10,C10,... 또는 ALL (미지정시 모든 학교)")
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"])
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    collector = TimetableCollector(
        shard=args.shard,
        incremental=args.incremental,
        full=args.full
    )

    # 수집 대상 학교 결정
    if args.regions:
        regions = (
            set(ALL_REGIONS) if args.regions.upper() == "ALL"
            else {r.strip() for r in args.regions.split(",")}
        )
        schools = [
            sc_code for sc_code, info in collector.school_cache.items()
            if info['atpt_code'] in regions
        ]
    else:
        schools = list(collector.school_cache.keys())

    collector.logger.info(f"🚀 시간표 수집 시작: {len(schools)}개 학교, {args.ay}학년도 {args.semester}학기")

    for sc_code in schools:
        if not should_include(args.shard, sc_code):
            continue
        collector.fetch_school_timetable(sc_code, args.ay, args.semester)

    collector.close()
    collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
