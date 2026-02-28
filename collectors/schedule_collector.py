#!/usr/bin/env python3
"""
학사일정 수집기 - 통합 버전
"""
import os
import sqlite3
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
import hashlib

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.meta_vocab import MetaVocabManager
from parsers.schedule_parser import parse_schedule_row
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/active")
os.makedirs(BASE_DIR, exist_ok=True)

GLOBAL_VOCAB_PATH = os.path.join(BASE_DIR, "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['schedule']


class ScheduleCollector(BaseCollector):
    """학사일정 수집기"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False, debug_mode: bool = False):
        if shard == "none":
            db_name = "schedule.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"schedule_{shard}{range_suffix}.db"
        
        db_path = os.path.join(BASE_DIR, db_name)
        
        super().__init__("schedule", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.debug_mode = debug_mode
        self.run_ay = get_current_school_year()
        
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.event_cache = {}
        
        self._init_meta_table()
        self._load_event_cache()
        
        self.logger.info(f"📅 ScheduleCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _init_meta_table(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule_meta (
                    school_id INTEGER NOT NULL,
                    ev_date INTEGER NOT NULL,
                    ev_id INTEGER NOT NULL,
                    meta_id INTEGER NOT NULL,
                    PRIMARY KEY (school_id, ev_date, ev_id, meta_id)
                )
            """)
    
    def _load_event_cache(self):
        try:
            with get_db_connection(self.db_path) as conn:
                cur = conn.execute("SELECT ev_id, ev_nm FROM vocab_event")
                for ev_id, ev_nm in cur:
                    self.event_cache[ev_nm] = ev_id
        except Exception:
            pass
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_event (
                    ev_id INTEGER PRIMARY KEY,
                    ev_nm TEXT NOT NULL UNIQUE,
                    ev_date INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    school_id INTEGER NOT NULL,
                    ev_date INTEGER NOT NULL,
                    ev_id INTEGER NOT NULL,
                    ay INTEGER NOT NULL,
                    is_special INTEGER NOT NULL,
                    grade_disp TEXT NOT NULL,
                    grade_raw TEXT NOT NULL,
                    grade_code INTEGER NOT NULL,
                    sub_yn INTEGER NOT NULL,
                    sub_code TEXT,
                    dn_yn INTEGER NOT NULL,
                    ev_content TEXT,
                    load_dt TEXT,
                    PRIMARY KEY (school_id, ev_date, ev_id, grade_code)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schedule_date ON schedule(ev_date)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return str(self.run_ay)
    
    def _get_event_id(self, ev_nm: str, ev_date: int) -> int:
        if ev_nm in self.event_cache:
            return self.event_cache[ev_nm]
        
        key = f"schedule:{ev_nm}:{ev_date}"
        ev_id = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) % 10**12
        self.event_cache[ev_nm] = ev_id
        return ev_id
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code or not self._include_school(sc_code):
            return []
        
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        
        parsed = parse_schedule_row(raw_item, school_info)
        if not parsed:
            return []
        
        ev_id = self._get_event_id(parsed["ev_nm"], parsed["ev_date"])
        
        results = []
        for grade_code in parsed.get("grade_codes", [0]):
            d = {
                "school_id": school_info['school_id'],
                "ev_date": parsed["ev_date"],
                "ev_id": ev_id,
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
            vocab_set = {(it['ev_id'], it['ev_nm'], it['ev_date']) for it in batch}
            conn.executemany(
                "INSERT OR IGNORE INTO vocab_event VALUES (?,?,?)",
                list(vocab_set)
            )
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
    
    def fetch_region(self, region: str, year: int):
        date_from = f"{year}0301"
        date_to = (date(year + 1, 3, 1) - timedelta(days=1)).strftime("%Y%m%d")
        
        p_idx = 1
        while p_idx <= 200:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 50,
                "ATPT_OFCDC_SC_CODE": region,
                "AA_YMD_FROM": date_from,
                "AA_YMD_TO": date_to,
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
                    items = self._process_item(r)
                    if items:
                        batch.extend(items)
                
                if batch:
                    self.enqueue(batch)
                self.logger.info(f"[{region}] p={p_idx} → {len(rows)}건")
                
                if len(rows) < 1000:
                    break
                p_idx += 1
                time.sleep(0.05)
            except Exception as e:
                self.logger.error(f"[{region}] p={p_idx} 에러: {e}")
                if p_idx > 5:
                    break
                p_idx += 1
                time.sleep(2)
    
    def close(self):
        self.meta_vocab.close()
        super().close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="학사일정 수집기")
    parser.add_argument("--regions", required=True, help="교육청 코드")
    parser.add_argument("--year", type=int, default=get_current_school_year())
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()
    
    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return
    
    school_range = None if args.school_range == "none" else args.school_range
    
    collector = ScheduleCollector(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug
    )
    
    regions = ALL_REGIONS if args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]
    
    try:
        for region in regions:
            collector.fetch_region(region, args.year)
    finally:
        collector.close()


if __name__ == "__main__":
    main()