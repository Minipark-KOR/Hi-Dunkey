#!/usr/bin/env python3
# collectors/neis_info_collector.py
# 최종 수정: 모든 NEIS API 필드 저장, 지오코딩 분리, 중앙 매핑 시스템 적용

import os
import sys
import time
import sqlite3
import argparse
import threading
from pathlib import Path
from typing import List, Dict, Tuple, Optional

sys.path.append(str(Path(__file__).parent.parent))

from core.config import config
from core.base_collector import BaseCollector
from core.database import get_db_connection, get_db_reader
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from core.address.address_filter import AddressFilter
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS, REGION_NAMES
from constants.paths import NEIS_INFO_DB_PATH as MASTER_DB, MASTER_DIR, FAILURES_DB_PATH
from constants.api_mappings import get_api_field   # ✅ 중앙 매핑 함수 임포트

BASE_DIR = str(MASTER_DIR)
GLOBAL_VOCAB_DB_PATH = MASTER_DIR.parent / "active" / "global_vocab.db"
NEIS_URL = NEIS_ENDPOINTS['school']

GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


class NeisInfoCollector(BaseCollector):
    description = "학교 기본정보 (NEIS)"
    table_name = "schools"
    merge_script = "scripts/merge_neis_info_dbs.py"
    _cfg = config.get_collector_config("neis_info")
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)
    metrics_config = {"enabled": True, "collect_geo": True, "collect_global": True}
    parallel_config = {
        "max_workers": _cfg.get("max_workers", 4),
        "cpu_factor": _cfg.get("cpu_factor", 1.0),
        "max_by_api": _cfg.get("max_by_api", 10),
        "absolute_max": _cfg.get("absolute_max", 16),
    }

    LEVEL_GEOCODING = 3
    LEVEL_FINAL = 4

    def __init__(
        self,
        shard: str = "none",
        school_range=None,
        incremental: bool = False,
        full: bool = False,
        compare: bool = False,
        debug_mode: bool = False,
        quiet_mode: bool = False
    ):
        super().__init__("neis_info", str(MASTER_DIR), shard, school_range)
        self.api_context = 'school'
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.quiet_mode = quiet_mode
        self.run_date = now_kst().strftime("%Y%m%d")

        self.meta_vocab = self.register_resource(
            MetaVocabManager(str(GLOBAL_VOCAB_DB_PATH), debug_mode)
        )
        # 지오코딩은 별도 워커에서 처리 → geo_collector 제거

        self.total_new = 0
        self.total_failed = 0
        self.total_skipped = 0
        self._counter_lock = threading.Lock()

        if not quiet_mode:
            self.print(f"🏫 NeisInfoCollector 초기화 완료 (샤드: {shard})")
        self.logger.info(f"🏫 NeisInfoCollector 초기화 완료 (샤드: {shard})")

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
                    cleaned_address TEXT,
                    address_hash TEXT,
                    tel TEXT,
                    homepage TEXT,
                    status TEXT DEFAULT '운영',
                    last_seen INTEGER,
                    load_dt TEXT,
                    latitude REAL,
                    longitude REAL,
                    geocode_attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    city_id INTEGER,
                    district_id INTEGER,
                    street_id INTEGER,
                    number_type TEXT,
                    number_value INTEGER,
                    number_start INTEGER,
                    number_end INTEGER,
                    number_bit INTEGER,
                    kakao_address TEXT,
                    jibun_address TEXT,
                    -- NEIS API 전체 필드 (마이그레이션 필요)
                    atpt_ofcdc_sc_nm TEXT,
                    lctn_sc_nm TEXT,
                    ju_org_nm TEXT,
                    fond_sc_nm TEXT,
                    org_rdnzc TEXT,
                    org_rdnda TEXT,
                    org_faxno TEXT,
                    coedu_sc_nm TEXT,
                    hs_sc_nm TEXT,
                    indst_specl_ccccl_exst_yn TEXT,
                    hs_gnrl_busns_sc_nm TEXT,
                    spcly_purps_hs_ord_nm TEXT,
                    ene_bfe_sehf_sc_nm TEXT,
                    dght_sc_nm TEXT,
                    fond_ymd TEXT,
                    foas_memrd TEXT,
                    load_dtm TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON schools(address_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_street ON schools(street_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_missing ON schools(latitude) WHERE latitude IS NULL")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(atpt_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_coords ON schools(latitude, longitude)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def _fetch_paginated(self, url, base_params, root_key, page_size=100, region=None, year=None):
        try:
            region_name = REGION_NAMES.get(region, region) if region else "알 수 없음"
        except NameError:
            region_name = region if region else "알 수 없음"
            self.logger.warning("REGION_NAMES가 정의되지 않아 기본값을 사용합니다.")
        try:
            return super()._fetch_paginated(url, base_params, root_key, page_size=page_size, region=region, year=year)
        except Exception as e:
            self.logger.error(f"[{region_name}] API 호출 실패: {e}")
            raise

    def fetch_region(self, region_code: str, limit: Optional[int] = None, year: Optional[int] = None, **kwargs) -> int:
        try:
            region_name = REGION_NAMES.get(region_code, region_code)
        except NameError:
            region_name = region_code
            self.logger.warning("REGION_NAMES가 정의되지 않아 지역 코드를 이름으로 사용합니다.")

        if year is None:
            year = get_current_school_year(now_kst())

        self.print(f"📡 [{region_name}({region_code})] 학년도 {year} 데이터 수집 시작...", level="debug")

        base_params = {"ATPT_OFCDC_SC_CODE": region_code}

        try:
            rows = self._fetch_paginated(
                NEIS_URL, base_params, 'schoolInfo',
                page_size=100,
                region=region_code,
                year=year
            )

            if not rows:
                self.logger.error(f"[{region_name}] 수집된 데이터 없음")
                return 0

            self.logger.info(f"[{region_name}] 전체 {len(rows)}건 수집")

            new, failed, skipped = self._update_schools_with_diff(rows, region_code, limit=limit)

            with self._counter_lock:
                self.total_new += new
                self.total_failed += failed
                self.total_skipped += skipped

            self.print(f"  📊 [{region_name}] 신규성공={new}, 실패={failed}, 스킵={skipped}")
            return new

        except Exception as e:
            self.logger.error(f"[{region_name}] 수집 실패: {e}")
            return 0

    def _update_schools_with_diff(self, new_rows: List[dict], region_code: str, limit: Optional[int] = None) -> Tuple[int, int, int]:
        try:
            region_name = REGION_NAMES.get(region_code, region_code)
        except NameError:
            region_name = region_code
            self.logger.warning("REGION_NAMES가 정의되지 않아 지역 코드를 이름으로 사용합니다.")

        existing = {}
        if os.path.exists(self.db_path):
            try:
                with get_db_reader(self.db_path) as conn:
                    cur = conn.execute(
                        "SELECT sc_code, address_hash, latitude, longitude FROM schools"
                    )
                    existing = {
                        row[0]: {"hash": row[1], "lat": row[2], "lon": row[3]}
                        for row in cur
                    }
            except Exception as e:
                self.logger.error(f"기존 데이터 조회 실패: {e}")
                return 0, 0, 0

        row_meta = {}
        for row in new_rows:
            sc_code = get_api_field(row, "school_code", "school", "")
            if not sc_code or not self._include_school(sc_code):
                continue
            full_address = get_api_field(row, "address", "school", "")
            new_hash = AddressFilter.hash(full_address) if full_address else ""
            row_meta[sc_code] = {
                "row": row,
                "full_address": full_address,
                "new_hash": new_hash,
                "old": existing.get(sc_code, {}),
            }

        if limit and len(row_meta) > limit:
            row_meta = dict(list(row_meta.items())[:limit])

        processed_count = 0
        skipped_count = 0
        start_time = time.time()
        last_update = start_time
        total_items = len(row_meta)

        for i, (sc_code, meta) in enumerate(row_meta.items(), 1):
            if meta["old"].get("hash") != meta["new_hash"] and meta["full_address"]:
                cleaned = AddressFilter.clean(meta["full_address"], level=self.LEVEL_GEOCODING)
                jibun = AddressFilter.extract_jibun(meta["full_address"])
                meta["cleaned_address"] = cleaned
                meta["jibun_address"] = jibun
                processed_count += 1
            else:
                meta["cleaned_address"] = None
                skipped_count += 1

            row = meta["row"]
            atpt_code = get_api_field(row, "region_code", "school", "")
            old = meta["old"]

            lat = old.get("lat")
            lon = old.get("lon")

            addr_ids = {}
            if meta.get("cleaned_address"):
                try:
                    addr_ids = self.meta_vocab.save_address(meta["cleaned_address"])
                except Exception as e:
                    self.logger.error(f"주소 변환 실패 {sc_code}: {e}")

            try:
                school_id = create_school_id(atpt_code, sc_code)
            except Exception as e:
                self.logger.error(f"school_id 생성 실패 {sc_code}: {e}")
                continue

            # 내부 필드명과 매핑된 값 조회 (모든 필드에 get_api_field 적용)
            self.enqueue([{
                "sc_code": sc_code,
                "school_id": school_id,
                "sc_name": get_api_field(row, "school_name", "school", ""),
                "eng_name": get_api_field(row, "eng_name", "school", ""),
                "sc_kind": get_api_field(row, "school_kind", "school", ""),
                "atpt_code": atpt_code,
                "address": meta["full_address"],
                "cleaned_address": meta.get("cleaned_address", ""),
                "address_hash": meta["new_hash"],
                "tel": get_api_field(row, "phone", "school", ""),
                "homepage": get_api_field(row, "homepage", "school", ""),
                "status": "운영",
                "last_seen": int(self.run_date),
                "load_dt": now_kst().isoformat(),
                "latitude": lat,
                "longitude": lon,
                "geocode_attempts": 0,
                "last_error": None,
                "city_id": addr_ids.get("city_id", 0),
                "district_id": addr_ids.get("district_id", 0),
                "street_id": addr_ids.get("street_id", 0),
                "number_type": addr_ids.get("number_type"),
                "number_value": addr_ids.get("number"),
                "number_start": addr_ids.get("number_start"),
                "number_end": addr_ids.get("number_end"),
                "number_bit": addr_ids.get("number_bit", 0),
                "jibun_address": meta.get("jibun_address"),
                "kakao_address": None,
                # NEIS 전체 필드 (매핑에 추가 필요시)
                "atpt_ofcdc_sc_nm": row.get("ATPT_OFCDC_SC_NM", ""),   # TODO: 매핑에 추가
                "lctn_sc_nm": row.get("LCTN_SC_NM", ""),
                "ju_org_nm": row.get("JU_ORG_NM", ""),
                "fond_sc_nm": row.get("FOND_SC_NM", ""),
                "org_rdnzc": row.get("ORG_RDNZC", "").strip(),
                "org_rdnda": row.get("ORG_RDNDA", ""),
                "org_faxno": row.get("ORG_FAXNO", ""),
                "coedu_sc_nm": row.get("COEDU_SC_NM", ""),
                "hs_sc_nm": row.get("HS_SC_NM", ""),
                "indst_specl_ccccl_exst_yn": row.get("INDST_SPECL_CCCCL_EXST_YN", ""),
                "hs_gnrl_busns_sc_nm": row.get("HS_GNRL_BUSNS_SC_NM", ""),
                "spcly_purps_hs_ord_nm": row.get("SPCLY_PURPS_HS_ORD_NM", ""),
                "ene_bfe_sehf_sc_nm": row.get("ENE_BFE_SEHF_SC_NM", ""),
                "dght_sc_nm": row.get("DGHT_SC_NM", ""),
                "fond_ymd": row.get("FOND_YMD", ""),
                "foas_memrd": row.get("FOAS_MEMRD", ""),
                "load_dtm": row.get("LOAD_DTM", ""),
            }])

            if time.time() - last_update >= 0.2 or i == total_items:
                self.print_progress(i, total_items, prefix=f"[{region_name}]")
                last_update = time.time()

        self.logger.info(f"[{region_name}] 처리 완료: 신규/변경 {processed_count}개, 스킵 {skipped_count}개")
        return processed_count, 0, skipped_count

    def _process_item(self, raw_item: dict) -> List[dict]:
        return []

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]):
        sql = """
            INSERT OR REPLACE INTO schools (
                sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                address, cleaned_address, address_hash, tel, homepage, status,
                last_seen, load_dt, latitude, longitude, geocode_attempts, last_error,
                city_id, district_id, street_id, number_type, number_value,
                number_start, number_end, number_bit, jibun_address, kakao_address,
                atpt_ofcdc_sc_nm, lctn_sc_nm, ju_org_nm, fond_sc_nm, org_rdnzc, org_rdnda,
                org_faxno, coedu_sc_nm, hs_sc_nm, indst_specl_ccccl_exst_yn, hs_gnrl_busns_sc_nm,
                spcly_purps_hs_ord_nm, ene_bfe_sehf_sc_nm, dght_sc_nm, fond_ymd, foas_memrd, load_dtm
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        rows = []
        for it in batch:
            rows.append((
                it['sc_code'], it['school_id'], it['sc_name'], it['eng_name'],
                it['sc_kind'], it['atpt_code'], it['address'], it['cleaned_address'],
                it['address_hash'], it['tel'], it['homepage'], it['status'],
                it['last_seen'], it['load_dt'], it['latitude'], it['longitude'],
                it['geocode_attempts'], it['last_error'], it['city_id'],
                it['district_id'], it['street_id'], it['number_type'],
                it['number_value'], it['number_start'], it['number_end'],
                it['number_bit'], it.get('jibun_address'), it.get('kakao_address'),
                it.get('atpt_ofcdc_sc_nm', ''),
                it.get('lctn_sc_nm', ''),
                it.get('ju_org_nm', ''),
                it.get('fond_sc_nm', ''),
                it.get('org_rdnzc', ''),
                it.get('org_rdnda', ''),
                it.get('org_faxno', ''),
                it.get('coedu_sc_nm', ''),
                it.get('hs_sc_nm', ''),
                it.get('indst_specl_ccccl_exst_yn', ''),
                it.get('hs_gnrl_busns_sc_nm', ''),
                it.get('spcly_purps_hs_ord_nm', ''),
                it.get('ene_bfe_sehf_sc_nm', ''),
                it.get('dght_sc_nm', ''),
                it.get('fond_ymd', ''),
                it.get('foas_memrd', ''),
                it.get('load_dtm', '')
            ))
        try:
            conn.executemany(sql, rows)
        except Exception as e:
            self.logger.error(f"배치 저장 실패: {e}")
            raise


if __name__ == "__main__":
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    parser = argparse.ArgumentParser(description="학교 기본정보 수집기")
    parser.add_argument("--regions", default="ALL", help="수집할 지역")
    parser.add_argument("--shard", choices=["none", "odd", "even"], default="none")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if is_github_actions and not args.quiet:
        args.quiet = True

    collector = NeisInfoCollector(
        shard=args.shard,
        debug_mode=args.debug,
        quiet_mode=args.quiet
    )

    print(f"📂 DB 경로: {collector.db_path}")

    if args.regions == "ALL":
        regions = ALL_REGIONS
    else:
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    for region in regions:
        collector.fetch_region(region, limit=args.limit)
        if args.limit:
            break

    collector.flush()
    time.sleep(2)
    collector.close()

    if os.path.exists(collector.db_path):
        count = sqlite3.connect(collector.db_path).execute("SELECT COUNT(*) FROM schools;").fetchone()[0]
        print(f"📊 DB 저장 완료: {count}건 (파일: {collector.db_path})")
    else:
        print(f"❌ DB 파일 없음: {collector.db_path}")
        