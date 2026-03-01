#!/usr/bin/env python3
"""
학교 기본정보 수집기
"""
import os
import time
from pathlib import Path
from typing import List
from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS, MASTER_DB
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data/master")
os.makedirs(BASE_DIR, exist_ok=True)
GLOBAL_VOCAB_PATH = os.path.join(os.path.dirname(BASE_DIR), "active", "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']

class SchoolMasterCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, incremental=False, full=False, compare=False, debug_mode=False):
        super().__init__("school", BASE_DIR, shard, school_range)
        self.api_context = 'school'
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.logger.info(f"🏫 SchoolMasterCollector 초기화 완료")
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    sc_code TEXT PRIMARY KEY,
                    school_id INTEGER,
                    sc_name TEXT,
                    eng_name TEXT,
                    sc_kind TEXT,
                    atpt_code TEXT,
                    address TEXT,
                    tel TEXT,
                    homepage TEXT,
                    status TEXT DEFAULT '운영',
                    last_seen INTEGER,
                    load_dt TEXT,
                    city_id INTEGER,
                    district_id INTEGER,
                    street_id INTEGER,
                    number_bit INTEGER
                )
            """)
            conn.execute("CREATE INDEX idx_status ON schools(status)")
            conn.execute("CREATE INDEX idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX idx_street ON schools(street_id)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def fetch_region(self, region_code: str):
        params = {
            "ATPT_OFCDC_SC_CODE": region_code,
            "pSize": 100
        }
        res = safe_json_request(self.session, NEIS_URL, params, self.logger)
        if not res or "schoolInfo" not in res:
            self.logger.error(f"[{region_code}] 응답 없음")
            return
        try:
            total_count = res["schoolInfo"][0]["head"][0]["list_total_count"]
            total_pages = (total_count + 99) // 100
            self.logger.info(f"[{region_code}] 전체 {total_count}개, {total_pages}페이지")
        except Exception as e:
            self.logger.error(f"total_count 파싱 실패: {e}")
            return
        for page in range(1, total_pages+1):
            page_params = {**params, "pIndex": page}
            res = safe_json_request(self.session, NEIS_URL, page_params, self.logger)
            if not res:
                continue
            rows = res["schoolInfo"][1].get("row", [])
            if not rows:
                continue
            batch = []
            for r in rows:
                items = self._process_item(r)
                if items:
                    batch.extend(items)
            if batch:
                self.enqueue(batch)
            self.logger.info(f"[{region_code}] p={page} → {len(rows)}건")
            time.sleep(0.1)
        self.logger.info(f"[{region_code}] 수집 완료")
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = self._get_field(raw_item, 'school_code')
        if not sc_code or not self._include_school(sc_code):
            return []
        atpt_code = self._get_field(raw_item, 'region_code')
        school_id = create_school_id(atpt_code, sc_code)
        full_address = self._get_field(raw_item, 'address', default='')
        addr_ids = {}
        if full_address:
            try:
                addr_ids = self.meta_vocab.save_address(full_address)
            except Exception as e:
                self.logger.error(f"주소 변환 실패 {sc_code}: {e}")
        return [{
            "sc_code": sc_code,
            "school_id": school_id,
            "sc_name": self._get_field(raw_item, 'school_name', default=''),
            "eng_name": self._get_field(raw_item, 'eng_name', default=''),
            "sc_kind": self._get_field(raw_item, 'school_kind', default=''),
            "atpt_code": atpt_code,
            "address": full_address,
            "tel": self._get_field(raw_item, 'phone', default=''),
            "homepage": self._get_field(raw_item, 'homepage', default=''),
            "status": "운영",
            "last_seen": int(self.run_date),
            "load_dt": now_kst().isoformat(),
            "city_id": addr_ids.get("city_id", 0),
            "district_id": addr_ids.get("district_id", 0),
            "street_id": addr_ids.get("street_id", 0),
            "number_bit": addr_ids.get("number_bit", 0)
        }]
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            school_data = [
                (
                    it['sc_code'], it['school_id'], it['sc_name'],
                    it['eng_name'], it['sc_kind'], it['atpt_code'],
                    it['address'], it['tel'], it['homepage'],
                    it['status'], it['last_seen'], it['load_dt'],
                    it['city_id'], it['district_id'], it['street_id'],
                    it['number_bit']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO schools
                (sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                 address, tel, homepage, status, last_seen, load_dt,
                 city_id, district_id, street_id, number_bit)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, school_data)
    
    def close(self):
        self.meta_vocab.close()
        super().close()
        