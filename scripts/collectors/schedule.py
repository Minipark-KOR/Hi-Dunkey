#!/usr/bin/env python3
"""
학사일정 수집기
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
from parsers.schedule_parser import parse_schedule_row
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['schedule']


class ScheduleCollector(BaseCollector):
    def __init__(self, shard="none", incremental=False, full=False):
        super().__init__("schedule", BASE_DIR, shard)
        self.incremental = incremental
        self.full = full
        self.run_date = now_kst().strftime("%Y%m%d")
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            # 이벤트 vocab
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_event (
                    ev_id INTEGER PRIMARY KEY,
                    ev_nm TEXT NOT NULL UNIQUE,
                    ev_date INTEGER
                )
            """)
            # 학사일정 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    school_id    INTEGER NOT NULL,
                    ev_date      INTEGER NOT NULL,
                    ev_id        INTEGER NOT NULL,
                    ay           INTEGER NOT NULL,
                    is_special   INTEGER NOT NULL,
                    grade_disp   TEXT NOT NULL,
                    grade_raw    TEXT NOT NULL,
                    grade_code   INTEGER NOT NULL,
                    sub_yn       INTEGER NOT NULL,
                    sub_code     TEXT,
                    dn_yn        INTEGER NOT NULL,
                    ev_content   TEXT,
                    load_dt      TEXT,
                    PRIMARY KEY (school_id, ev_date, ev_id, grade_code),
                    FOREIGN KEY (ev_id) REFERENCES vocab_event(ev_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schedule_date ON schedule(ev_date)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code:
            return []
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']
        
        parsed = parse_schedule_row(raw_item, school_info)
        if not parsed:
            return []
        
        results = []
        for grade_code in parsed.get("grade_codes", [0]):
            d = {
                "school_id": school_id,
                "ev_date": parsed["ev_date"],
                "ev_id": parsed["ev_id"],
                "ev_nm": parsed["ev_nm"],
                "ay": parsed["ay"],
                "is_special": parsed["is_sp"],
                "grade_disp": parsed["grade_disp"],
                "grade_raw": parsed["grade_raw"],
                "grade_code": grade_code,
                "sub_yn": parsed["sub_yn"],
                "sub_code": parsed["sub_code"],
                "dn_yn": parsed["dn_yn"],
                "ev_content": parsed["content"],
                "load_dt": parsed["load_dt"]
            }
            results.append(d)
        return results
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            # vocab_event 저장
            vocab_set = {(it['ev_id'], it['ev_nm'], it['ev_date']) for it in batch}
            conn.executemany(
                "INSERT OR IGNORE INTO vocab_event VALUES (?,?,?)",
                list(vocab_set)
            )
            # schedule 저장
            sched_data = [
                (
                    it['school_id'], it['ev_date'], it['ev_id'], it['ay'],
                    it['is_special'], it['grade_disp'], it['grade_raw'], it['grade_code'],
                    it['sub_yn'], it['sub_code'], it['dn_yn'], it['ev_content'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT INTO schedule VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT DO UPDATE SET
                    ev_content = excluded.ev_content,
                    sub_yn = excluded.sub_yn,
                    sub_code = excluded.sub_code,
                    dn_yn = excluded.dn_yn,
                    load_dt = excluded.load_dt
            """, sched_data)
    
    def fetch_region(self, region: str, date: str):
        p_idx = 1
        consecutive_errors = 0
        while p_idx <= 50:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region,
                "AA_YMD": date
            }
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "schoolSchedule" not in res:
                    break
                rows = res["schoolSchedule"][1].get("row", [])
                if not rows:
                    break
                
                batch = []
                for r in rows:
                    sc_code = r.get("SD_SCHUL_CODE")
                    if not sc_code:
                        continue
                    if not should_include(self.shard, sc_code):
                        continue
                    items = self._process_item(r)
                    batch.extend(items)
                
                if batch:
                    self.enqueue(batch)
                self.logger.info(f"[{region}] p={p_idx} → {len(rows)}건, 이벤트 {len(batch)}개")
                consecutive_errors = 0
                
                if len(rows) < 1000:
                    break
                p_idx += 1
                time.sleep(0.05)
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"[{region}] p={p_idx} 에러: {e}")
                if consecutive_errors >= 5:
                    break
                p_idx += 1
                time.sleep(2 ** min(consecutive_errors, 5))
    
    def close(self):
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="학사일정 수집기")
    parser.add_argument("--regions", required=True, help="B10,C10,... 또는 ALL")
    parser.add_argument("--date", default=now_kst().strftime("%Y%m%d"))
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"])
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    regions = ALL_REGIONS if args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]

    collector = ScheduleCollector(
        shard=args.shard,
        incremental=args.incremental,
        full=args.full
    )

    for region in regions:
        collector.logger.info(f"🚀 {region} 수집 시작")
        collector.fetch_region(region, args.date)

    collector.close()
    collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
    