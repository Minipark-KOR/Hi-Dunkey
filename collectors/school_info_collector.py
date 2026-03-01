#!/usr/bin/env python3
"""
학교 기본정보 수집기 (Diff 기반 좌표 갱신)
- geocoding을 루프 밖에서 일괄 처리하여 throttling 문제 해결
- hash 계산 1회로 최적화
- atpt_code None 방어 추가
"""
import os
import time
import random
from pathlib import Path
from typing import List, Dict, Tuple

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from core.filters import AddressFilter
from core.geo import VWorldGeocoder
from constants.codes import NEIS_ENDPOINTS
from constants.paths import MASTER_DB_PATH as MASTER_DB  # ✅ 수정: paths에서 가져옴
from core.kst_time import now_kst
from constants.paths import MASTER_DIR

BASE_DIR = str(MASTER_DIR)
GLOBAL_VOCAB_PATH = str(MASTER_DIR.parent / "active" / "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']


class SchoolInfoCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, incremental=False, full=False, compare=False, debug_mode=False):
        super().__init__("school", BASE_DIR, shard, school_range)
        self.api_context = 'school'
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")
        self.meta_vocab = self.register_resource(
            MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        )
        self.geocoder = VWorldGeocoder(calls_per_second=3.0)
        self.logger.info(f"🏫 SchoolInfoCollector 초기화 완료")

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
                    address_hash TEXT,
                    tel TEXT,
                    homepage TEXT,
                    status TEXT DEFAULT '운영',
                    last_seen INTEGER,
                    load_dt TEXT,
                    latitude REAL,
                    longitude REAL,
                    city_id INTEGER,
                    district_id INTEGER,
                    street_id INTEGER,
                    number_bit INTEGER
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON schools(address_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_street ON schools(street_id)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def fetch_region(self, region_code: str):
        base_params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(NEIS_URL, base_params, 'schoolInfo', page_size=100)
        if not rows:
            self.logger.error(f"[{region_code}] 수집된 데이터 없음")
            return
        self.logger.info(f"[{region_code}] 전체 {len(rows)}건 수집")
        self._update_schools_with_diff(rows, region_code)

    def _update_schools_with_diff(self, new_rows: List[dict], region_code: str):
        with get_db_connection(MASTER_DB) as conn:
            cur = conn.execute(
                "SELECT sc_code, address_hash, latitude, longitude FROM schools"
            )
            existing = {row[0]: {"hash": row[1], "lat": row[2], "lon": row[3]} for row in cur}

        # 1단계: 모든 row에 대해 hash를 미리 계산하여 저장
        row_meta = {}
        for row in new_rows:
            sc_code = row.get("SD_SCHUL_CODE")
            if not sc_code or not self._include_school(sc_code):
                continue
            full_address = row.get("ORG_RDNMA", "")
            new_hash = AddressFilter.hash(full_address) if full_address else ""
            row_meta[sc_code] = {
                "row": row,
                "full_address": full_address,
                "new_hash": new_hash,
                "old": existing.get(sc_code, {}),
            }

        # 2단계: 주소 변경된 학교 좌표 일괄 수집
        new_coords: Dict[str, Tuple[float, float]] = {}
        for sc_code, meta in row_meta.items():
            if meta["old"].get("hash") != meta["new_hash"] and meta["full_address"]:
                coords = self.geocoder.geocode(meta["full_address"])
                if coords:
                    new_coords[sc_code] = coords
                time.sleep(random.uniform(0.2, 0.5))

        # 3단계: 모든 row enqueue (hash 재계산 없음)
        for sc_code, meta in row_meta.items():
            row = meta["row"]
            atpt_code = row.get("ATPT_OFCDC_SC_CODE") or ""  # ✅ None 방어
            old = meta["old"]

            if sc_code in new_coords:
                lon, lat = new_coords[sc_code]
            else:
                lat, lon = old.get("lat"), old.get("lon")

            addr_ids = {}
            if meta["full_address"]:
                try:
                    addr_ids = self.meta_vocab.save_address(meta["full_address"])
                except Exception as e:
                    self.logger.error(f"주소 변환 실패 {sc_code}: {e}")

            school_item = {
                "sc_code": sc_code,
                "school_id": create_school_id(atpt_code, sc_code),
                "sc_name": row.get("SCHUL_NM", ""),
                "eng_name": row.get("ENG_SCHUL_NM", ""),
                "sc_kind": row.get("SCHUL_KND_SC_NM", ""),
                "atpt_code": atpt_code,
                "address": meta["full_address"],
                "address_hash": meta["new_hash"],
                "tel": row.get("ORG_TELNO", ""),
                "homepage": row.get("HMPG_ADRES", ""),
                "status": "운영",
                "last_seen": int(self.run_date),
                "load_dt": now_kst().isoformat(),
                "city_id": addr_ids.get("city_id", 0),
                "district_id": addr_ids.get("district_id", 0),
                "street_id": addr_ids.get("street_id", 0),
                "number_bit": addr_ids.get("number_bit", 0),
                "latitude": lat,
                "longitude": lon,
            }
            self.enqueue([school_item])

        self.logger.info(f"[{region_code}] 좌표 갱신: {len(new_coords)}개 / 완료")

    def _process_item(self, raw_item: dict) -> List[dict]:
        return []

    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            school_data = [
                (
                    it['sc_code'], it['school_id'], it['sc_name'],
                    it['eng_name'], it['sc_kind'], it['atpt_code'],
                    it['address'], it['address_hash'], it['tel'], it['homepage'],
                    it['status'], it['last_seen'], it['load_dt'],
                    it['latitude'], it['longitude'],
                    it['city_id'], it['district_id'], it['street_id'], it['number_bit']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO schools
                (sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                 address, address_hash, tel, homepage, status, last_seen, load_dt,
                 latitude, longitude, city_id, district_id, street_id, number_bit)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, school_data)


if __name__ == "__main__":
    from core.collector_cli import run_collector

    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region)

    run_collector(
        SchoolInfoCollector,
        _fetch,
        "학교 기본정보 수집기",
    )
    