#!/usr/bin/env python3
"""
학교 기본정보 수집기 - 주소 원본 포함 버전
- meta_vocab으로 주소 ID화
- 원본 주소도 함께 저장 (백업용)
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.school_id import create_school_id
from core.backup import vacuum_into
from core.meta_vocab import MetaVocabManager
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/master")
os.makedirs(BASE_DIR, exist_ok=True)

TOTAL_DB = os.path.join(BASE_DIR, "school_master_TOTAL.db")
NEIS_URL = NEIS_ENDPOINTS['school']


class SchoolMasterCollector(BaseCollector):
    """학교 기본정보 수집기 - 주소 원본 포함"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False, compare: bool = False,
                 debug: bool = False):
        super().__init__("school", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug = debug
        self.total_db_path = TOTAL_DB
        self.run_date = now_kst().strftime("%Y%m%d")
        
        # MetaVocabManager 초기화
        self.meta_vocab = MetaVocabManager("data/global_vocab.db", debug_mode=debug)
        
        self.logger.info(f"🏫 SchoolMasterCollector 초기화 완료 (shard={shard}, range={school_range})")

    def _init_db(self):
        """schools 테이블 생성 (원본 주소 포함)"""
        with get_db_connection(self.db_path) as conn:
            # 교육청 vocab 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_office (
                    atpt_code   TEXT PRIMARY KEY,
                    atpt_name   TEXT,
                    last_updated TEXT
                )
            """)
            
            # 학교 메인 테이블 (원본 주소 포함!)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    sc_code     TEXT PRIMARY KEY,
                    school_id   INTEGER,
                    sc_name     TEXT,
                    eng_name    TEXT,
                    sc_kind     TEXT,
                    atpt_code   TEXT,
                    location    TEXT,
                    foundation  TEXT,
                    branch_type TEXT,
                    hs_category TEXT,
                    coed        TEXT,
                    address     TEXT,        -- 원본 주소 (백업용)
                    tel         TEXT,
                    homepage    TEXT,
                    status      TEXT DEFAULT '운영',
                    last_seen   INTEGER,
                    load_dt     TEXT,
                    
                    -- 주소 ID (검색용)
                    city_id     INTEGER,
                    district_id INTEGER,
                    street_id   INTEGER,
                    number_bit  INTEGER,
                    
                    FOREIGN KEY (city_id) REFERENCES meta_vocab(meta_id),
                    FOREIGN KEY (district_id) REFERENCES meta_vocab(meta_id),
                    FOREIGN KEY (street_id) REFERENCES meta_vocab(meta_id)
                )
            """)
            
            # 인덱스 생성
            conn.execute("CREATE INDEX IF NOT EXISTS idx_atpt ON schools(atpt_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_street ON schools(street_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_number ON schools(number_bit)")
            
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 1건 → 저장용 List[dict]"""
        atpt_code = raw_item.get("ATPT_OFCDC_SC_CODE", "")
        sc_code = raw_item.get("SD_SCHUL_CODE", "")
        if not sc_code:
            return []
        
        if not self._include_school(sc_code):
            return []

        school_id = create_school_id(atpt_code, sc_code)
        full_address = raw_item.get("ORG_RDNMA", "")  # 도로명주소
        
        # 주소 ID 변환
        addr_ids = self.meta_vocab.save_address(full_address)

        return [{
            "sc_code":      sc_code,
            "school_id":    school_id,
            "sc_name":      raw_item.get("SCHUL_NM", ""),
            "eng_name":     raw_item.get("ENG_SCHUL_NM", ""),
            "sc_kind":      raw_item.get("SCHUL_KND_SC_NM", ""),
            "atpt_code":    atpt_code,
            "atpt_name":    raw_item.get("ATPT_OFCDC_SC_NM", ""),
            "location":     raw_item.get("LCTN_SC_NM", ""),
            "foundation":   raw_item.get("FOND_SC_NM", ""),
            "branch_type":  raw_item.get("FO_SC_NM", "본교"),
            "hs_category":  raw_item.get("HS_GNRL_BUSI_SC_NM", "해당없음"),
            "coed":         raw_item.get("COEDU_SC_NM", ""),
            "address":      full_address,  # 원본 저장
            "tel":          raw_item.get("ORG_TELNO", ""),
            "homepage":     raw_item.get("HMPG_ADRES", ""),
            "status":       "운영",
            "last_seen":    int(self.run_date),
            "load_dt":      now_kst().isoformat(),
            
            # 주소 ID
            "city_id":      addr_ids.get("city_id", 0),
            "district_id":  addr_ids.get("district_id", 0),
            "street_id":    addr_ids.get("street_id", 0),
            "number_bit":   addr_ids.get("number_bit", 0)
        }]

    def _save_batch(self, batch: List[dict]):
        """배치 저장"""
        with get_db_connection(self.db_path) as conn:
            # vocab_office 저장
            office_data = list({
                (it['atpt_code'], it['atpt_name'], self.run_date)
                for it in batch
            })
            conn.executemany(
                "INSERT OR REPLACE INTO vocab_office VALUES (?,?,?)",
                office_data
            )
            
            # schools 저장 (원본 주소 포함)
            school_data = [
                (
                    it['sc_code'], it['school_id'], it['sc_name'],
                    it['eng_name'], it['sc_kind'], it['atpt_code'],
                    it['location'], it['foundation'], it['branch_type'],
                    it['hs_category'], it['coed'], it['address'],
                    it['tel'], it['homepage'], it['status'],
                    it['last_seen'], it['load_dt'],
                    it['city_id'], it['district_id'], it['street_id'],
                    it['number_bit']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO schools
                (sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                 location, foundation, branch_type, hs_category, coed,
                 address, tel, homepage, status, last_seen, load_dt,
                 city_id, district_id, street_id, number_bit)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, school_data)

    def fetch_region(self, region_code: str):
        """특정 교육청의 학교 목록 수집"""
        p_idx = 1
        consecutive_errors = 0
        max_page = 50
        
        self.logger.info(f"📡 {region_code} 수집 시작")
        
        while p_idx <= max_page:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region_code
            }
            
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "schoolInfo" not in res:
                    break
                    
                rows = res["schoolInfo"][1].get("row", [])
                if not rows:
                    break

                batch = []
                for r in rows:
                    sc_code = r.get("SD_SCHUL_CODE")
                    if not sc_code:
                        continue
                    
                    if not self._include_school(sc_code):
                        continue
                        
                    items = self._process_item(r)
                    if items:
                        batch.extend(items)

                if batch:
                    self.enqueue(batch)
                    
                self.logger.info(f"[{region_code}] p={p_idx} → {len(rows)}건 (수집: {len(batch)}개)")
                consecutive_errors = 0

                if len(rows) < 1000:
                    break
                    
                p_idx += 1
                time.sleep(0.1)

            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"[{region_code}] p={p_idx} 에러: {e}")
                
                if consecutive_errors >= 5:
                    self.logger.warning(f"[{region_code}] 연속 에러 5회 → 중단")
                    break
                    
                p_idx += 1
                time.sleep(2 ** min(consecutive_errors, 5))

    def close(self):
        """종료 처리"""
        self.meta_vocab.close()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="학교 기본정보 수집기 - 주소 원본 포함")
    parser.add_argument("--regions", help="B10,C10,... 또는 ALL (기본: ALL)")
    parser.add_argument("--shard", default="none", choices=["odd", "even", "none"])
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    regions = ALL_REGIONS if not args.regions or args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]

    school_range = None if args.school_range == "none" else args.school_range

    collector = SchoolMasterCollector(
        shard=args.shard,
        school_range=school_range,
        incremental=args.incremental,
        full=args.full,
        compare=args.compare,
        debug=args.debug
    )

    try:
        for region in regions:
            collector.logger.info(f"🚀 {region} 수집 시작")
            collector.fetch_region(region)
    finally:
        collector.close()


if __name__ == "__main__":
    main()
    