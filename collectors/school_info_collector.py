#!/usr/bin/env python3
"""
학교 기본정보 수집기 (Diff 기반 좌표 갱신)
- GeoCollector 통합으로 캐시 및 API 사용량 추적
"""
import os
import time
import random
import sqlite3
from typing import List, Dict, Tuple
from datetime import timedelta

from core.base_collector import BaseCollector
from core.database import get_db_connection, get_db_reader
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from core.filters import AddressFilter
from core.kst_time import now_kst
from constants.codes import NEIS_ENDPOINTS
from constants.paths import MASTER_DB_PATH as MASTER_DB, MASTER_DIR

from collectors.geo_collector import GeoCollector

BASE_DIR = str(MASTER_DIR)
GLOBAL_VOCAB_PATH = str(MASTER_DIR.parent / "active" / "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']


class SchoolInfoCollector(BaseCollector):
    LEVEL_GEOCODING = 3
    LEVEL_FINAL = 4

    def __init__(
        self,
        shard: str = "none",
        school_range=None,
        incremental: bool = False,
        full: bool = False,
        compare: bool = False,
        debug_mode: bool = False
    ):
        super().__init__("school", str(MASTER_DIR.parent), shard, school_range)
        self.db_path = str(MASTER_DB)

        self.api_context = 'school'
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")

        self.meta_vocab = self.register_resource(
            MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        )

        self.geo_collector = self.register_resource(
            GeoCollector(
                global_db_path=GLOBAL_VOCAB_PATH,
                school_db_path=self.db_path,
                failures_db_path="data/failures.db",
                debug_mode=debug_mode,
            )
        )

        self.logger.info("🏫 SchoolInfoCollector 초기화 완료")

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    sc_code           TEXT PRIMARY KEY,
                    school_id         INTEGER,
                    sc_name           TEXT,
                    eng_name          TEXT,
                    sc_kind           TEXT,
                    atpt_code         TEXT,
                    address           TEXT,
                    cleaned_address   TEXT,
                    address_hash      TEXT,
                    tel               TEXT,
                    homepage          TEXT,
                    status            TEXT DEFAULT '운영',
                    last_seen         INTEGER,
                    load_dt           TEXT,
                    latitude          REAL,
                    longitude         REAL,
                    geocode_attempts  INTEGER DEFAULT 0,
                    last_error        TEXT,
                    city_id           INTEGER,
                    district_id       INTEGER,
                    street_id         INTEGER,
                    number_type       TEXT,
                    number_value      INTEGER,
                    number_start      INTEGER,
                    number_end        INTEGER,
                    number_bit        INTEGER
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON schools(address_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_street ON schools(street_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_missing ON schools(latitude) WHERE latitude IS NULL")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(atpt_code, last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_coords ON schools(latitude, longitude)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def fetch_region(self, region_code: str, **kwargs):
        if self.debug_mode:
            print(f"\n📡 [{region_code}] 데이터 수집 시작...")
        base_params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(
            NEIS_URL, base_params, 'schoolInfo',
            page_size=100,
            region=region_code,
            year=int(self.run_date[:4])
        )
        if not rows:
            self.logger.error(f"[{region_code}] 수집된 데이터 없음")
            if self.debug_mode:
                print(f"❌ [{region_code}] 수집된 데이터 없음")
            return
        self.logger.info(f"[{region_code}] 전체 {len(rows)}건 수집")
        if self.debug_mode:
            print(f"📋 [{region_code}] 전체 {len(rows)}건 수집")
        self._update_schools_with_diff(rows, region_code)
        if self.debug_mode:
            print(f"✅ [{region_code}] 처리 완료")

    def _update_schools_with_diff(self, new_rows: List[dict], region_code: str):
        existing = {}
        if os.path.exists(self.db_path):
            try:
                with get_db_reader(self.db_path) as conn:
                    cur = conn.execute(
                        "SELECT sc_code, address_hash, latitude, longitude, geocode_attempts, last_error FROM schools"
                    )
                    existing = {
                        row[0]: {"hash": row[1], "lat": row[2], "lon": row[3], "attempts": row[4], "last_error": row[5]}
                        for row in cur
                    }
            except sqlite3.OperationalError as e:
                self.logger.error(f"DB 읽기 실패 (락/연결): {e}. 지역 {region_code} 건너뜀.")
                if self.debug_mode:
                    print(f"❌ DB 읽기 실패, 지역 {region_code} 건너뜀.")
                return
            except Exception as e:
                self.logger.error(f"기존 schools 테이블 조회 실패: {e}")
                if self.debug_mode:
                    print(f"❌ 기존 데이터 조회 실패, 지역 {region_code} 계속 진행.")

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

        new_coords: Dict[str, Tuple[float, float]] = {}
        failed_count = 0
        skipped_count = 0

        for sc_code, meta in row_meta.items():
            if meta["old"].get("hash") != meta["new_hash"] and meta["full_address"]:
                cleaned = AddressFilter.clean(meta["full_address"], level=self.LEVEL_GEOCODING)
                
                # ✅ 수정: _geocode 예외 처리 추가
                try:
                    coords = self.geo_collector._geocode(cleaned)
                except Exception as e:
                    self.logger.warning(f"지오코딩 중 예외 발생 {sc_code}: {e}")
                    coords = None
                
                if coords:
                    new_coords[sc_code] = coords
                    if self.debug_mode:
                        print(f"  ✅ [{sc_code}] 좌표 획득 성공")
                else:
                    failed_count += 1
                    if self.shard != "none":
                        deadline = now_kst().replace(hour=15, minute=0, second=0, microsecond=0)
                        if now_kst() > deadline:
                            deadline += timedelta(days=1)
                        self.retry_mgr.record_failure(
                            domain='school', task_type='geocode',
                            shard=self.shard, sc_code=sc_code,
                            region=region_code, address=meta["full_address"],
                            error="Geocoding failed", deadline=deadline,
                        )
                        if self.debug_mode:
                            print(f"  ⚠️ [{sc_code}] 좌표 실패 → failures 등록")
                    else:
                        self.logger.warning(f"샤드 없음 → 지오코딩 실패 기록 생략: {sc_code}")
                time.sleep(random.uniform(0.2, 0.5))
            else:
                skipped_count += 1

        if self.debug_mode and (new_coords or failed_count or skipped_count):
            print(f"  📊 [{region_code}] 좌표 현황: 신규성공={len(new_coords)}, 실패={failed_count}, 스킵={skipped_count}")

        for sc_code, meta in row_meta.items():
            row = meta["row"]
            atpt_code = row.get("ATPT_OFCDC_SC_CODE") or ""
            old = meta["old"]

            if sc_code in new_coords:
                lon, lat = new_coords[sc_code]
                attempts, last_error = 0, None
            else:
                lat, lon = old.get("lat"), old.get("lon")
                attempts = old.get("attempts", 0) + 1
                last_error = old.get("last_error") or "Geocoding failed"

            cleaned = AddressFilter.clean(meta["full_address"], level=self.LEVEL_FINAL) if meta["full_address"] else ""
            addr_ids = {}
            if cleaned:
                try:
                    addr_ids = self.meta_vocab.save_address(cleaned)
                except Exception as e:
                    self.logger.error(f"주소 변환 실패 {sc_code}: {e}")

            self.enqueue([{
                "sc_code": sc_code,
                "school_id": create_school_id(atpt_code, sc_code),
                "sc_name": row.get("SCHUL_NM", ""),
                "eng_name": row.get("ENG_SCHUL_NM", ""),
                "sc_kind": row.get("SCHUL_KND_SC_NM", ""),
                "atpt_code": atpt_code,
                "address": meta["full_address"],
                "cleaned_address": cleaned,
                "address_hash": meta["new_hash"],
                "tel": row.get("ORG_TELNO", ""),
                "homepage": row.get("HMPG_ADRES", ""),
                "status": "운영",
                "last_seen": int(self.run_date),
                "load_dt": now_kst().isoformat(),
                "latitude": lat,
                "longitude": lon,
                "geocode_attempts": attempts,
                "last_error": last_error,
                "city_id": addr_ids.get("city_id", 0),
                "district_id": addr_ids.get("district_id", 0),
                "street_id": addr_ids.get("street_id", 0),
                "number_type": addr_ids.get("number_type"),
                "number_value": addr_ids.get("number"),
                "number_start": addr_ids.get("number_start"),
                "number_end": addr_ids.get("number_end"),
                "number_bit": addr_ids.get("number_bit", 0),
            }])

        self.logger.info(f"[{region_code}] 좌표 갱신: {len(new_coords)}개 / 완료")

    def _process_item(self, raw_item: dict) -> List[dict]:
        return []

    # ✅ 수정: BEGIN IMMEDIATE 제거 (BaseCollector 가 트랜잭션 관리)
    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]):
        try:
            conn.executemany("""
                INSERT OR REPLACE INTO schools
                (sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                 address, cleaned_address, address_hash, tel, homepage, status, 
                 last_seen, load_dt, latitude, longitude, geocode_attempts, last_error,
                 city_id, district_id, street_id, number_type, number_value, 
                 number_start, number_end, number_bit)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                (it['sc_code'], it['school_id'], it['sc_name'], it['eng_name'],
                 it['sc_kind'], it['atpt_code'], it['address'], it['cleaned_address'],
                 it['address_hash'], it['tel'], it['homepage'], it['status'],
                 it['last_seen'], it['load_dt'], it['latitude'], it['longitude'],
                 it['geocode_attempts'], it['last_error'], it['city_id'],
                 it['district_id'], it['street_id'], it['number_type'],
                 it['number_value'], it['number_start'], it['number_end'],
                 it['number_bit'])
                for it in batch
            ])
            if self.debug_mode:
                print(f"💾 배치 저장 완료: {len(batch)}개")
        except Exception as e:
            self.logger.error(f"배치 저장 실패: {e}")
            raise


if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)
    run_collector(SchoolInfoCollector, _fetch, "학교 기본정보 수집기")
    