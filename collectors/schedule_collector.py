#!/usr/bin/env python3
"""
학사일정 수집기 - 학년도 전체 버전
"""
import hashlib
from datetime import date, timedelta
from pathlib import Path
from typing import List
from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.meta_vocab import MetaVocabManager
from parsers.schedule_parser import parse_schedule_row
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR = PROJECT_ROOT / "data" / "active"
GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['schedule']

class AnnualFullScheduleCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, debug_mode=False):
        super().__init__("schedule", str(ACTIVE_DIR), shard, school_range)
        self.api_context = 'schedule'
        self.debug_mode = debug_mode
        self.run_ay = get_current_school_year(now_kst())
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.event_cache = {}
        self._init_meta_table()
        self._load_event_cache()
    
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
            conn.execute("CREATE INDEX idx_schedule_date ON schedule(ev_date)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return str(self.run_ay)
    
    def _get_event_id(self, ev_nm: str, ev_date: int) -> int:
        # 충돌 가능성은 매우 낮으므로 현재 방식 유지
        if ev_nm in self.event_cache:
            return self.event_cache[ev_nm]
        key = f"schedule:{ev_nm}:{ev_date}"
        ev_id = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) % 10**12
        self.event_cache[ev_nm] = ev_id
        return ev_id
    
    def fetch_region(self, region: str, **kwargs):
        year = kwargs.get("year", self.run_ay)
        self.fetch_year(region, year)
    
    def fetch_year(self, region: str, year: int):
        date_from = f"{year}0301"
        date_to = (date(year+1, 3, 1) - timedelta(days=1)).strftime("%Y%m%d")
        self._fetch_schools_range(region, date_from, date_to)
    
    def _fetch_schools_range(self, region: str, date_from: str, date_to: str):
        # BaseCollector의 iterate_schools를 사용하여 로깅 자동화
        self.iterate_schools(
            region,
            lambda sch_code, _: self._fetch_school_schedule(region, sch_code, date_from, date_to)
        )
    
    def _fetch_school_schedule(self, region: str, school_code: str, date_from: str, date_to: str):
        base_params = {
            "ATPT_OFCDC_SC_CODE": region,
            "SD_SCHUL_CODE": school_code,
            "AA_YMD_FROM": date_from,
            "AA_YMD_TO": date_to,
        }
        rows = self._fetch_paginated(NEIS_URL, base_params, 'schoolSchedule', page_size=100)
        for r in rows:
            items = self._process_item(r)
            if items:
                self.enqueue(items)
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = self._get_field(raw_item, 'school_code')
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
                "load_dt": parsed["load_dt"],
                "ev_nm": parsed["ev_nm"],
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
    
    def close(self):
        self.meta_vocab.close()
        super().close()
        