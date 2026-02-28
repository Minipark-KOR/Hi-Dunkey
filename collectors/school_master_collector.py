#!/usr/bin/env python3
"""
학교 기본정보 수집기 - 통합 버전
"""
import os
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
from core.meta_vocab import MetaVocabManager
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/master")
os.makedirs(BASE_DIR, exist_ok=True)

# ✅ 수정: 올바른 global_vocab.db 경로
GLOBAL_VOCAB_PATH = os.path.join(os.path.dirname(BASE_DIR), "active", "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']


class SchoolMasterCollector(BaseCollector):
    """학교 기본정보 수집기"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False, compare: bool = False,
                 debug_mode: bool = False):

        # 🔍 BASE_DIR이 실제로 어디인지 로그를 찍어봅니다.
        print(f"DEBUG: 현재 BASE_DIR은 {BASE_DIR} 입니다.")

        # 샤드 DB 경로
        if shard == "none":
            db_name = "school_master.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"school_master_{shard}{range_suffix}.db"
        
        db_path = os.path.join(BASE_DIR, db_name)
        
        # BaseCollector 초기화
        super().__init__("school", BASE_DIR, shard, school_range)

        # ✅ 추가: 경로 강제 고정
        self.db_path = db_path
        print(f"DEBUG: 최종 확정 DB 경로 -> {self.db_path}")

        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")
        
        # ✅ global_vocab.db 연결
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        
        self.logger.info(f"🏫 SchoolMasterCollector 초기화 완료 (shard={shard}, range={school_range})")
        if debug_mode:
            self.logger.info(f"  📍 global_vocab: {GLOBAL_VOCAB_PATH}")
    
    def _init_db(self):
        """schools 테이블 생성"""
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON schools(city_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_district ON schools(district_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_street ON schools(street_id)")
            
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = raw_item.get("SD_SCHUL_CODE", "")
        if not sc_code:
            return []
        
        if not self._include_school(sc_code):
            return []
        
        atpt_code = raw_item.get("ATPT_OFCDC_SC_CODE", "")
        school_id = create_school_id(atpt_code, sc_code)
        full_address = raw_item.get("ORG_RDNMA", "")
        
        # 주소 ID 변환
        addr_ids = {}
        if full_address:
            try:
                addr_ids = self.meta_vocab.save_address(full_address)
            except Exception as e:
                self.logger.error(f"주소 변환 실패 {sc_code}: {e}")
        
        return [{
            "sc_code": sc_code,
            "school_id": school_id,
            "sc_name": raw_item.get("SCHUL_NM", ""),
            "eng_name": raw_item.get("ENG_SCHUL_NM", ""),
            "sc_kind": raw_item.get("SCHUL_KND_SC_NM", ""),
            "atpt_code": atpt_code,
            "address": full_address,
            "tel": raw_item.get("ORG_TELNO", ""),
            "homepage": raw_item.get("HMPG_ADRES", ""),
            "status": "운영",
            "last_seen": int(self.run_date),
            "load_dt": now_kst().isoformat(),
            "city_id": addr_ids.get("city_id", 0),
            "district_id": addr_ids.get("district_id", 0),
            "street_id": addr_ids.get("street_id", 0),
            "number_bit": addr_ids.get("number_bit", 0)
        }]
    
    def _save_batch(self, batch: List[dict]):
        """배치 저장"""
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
    
    def fetch_region(self, region_code: str):
        p_idx = 1
        max_retries_per_page = 5

        # 첫 페이지 요청으로 total_count 획득
        params = {
            "KEY": NEIS_API_KEY,
            "Type": "json",
            "pIndex": p_idx,
            "pSize": 500,                     # ← 100 고정
            "ATPT_OFCDC_SC_CODE": region_code
        }

        res = safe_json_request(self.session, NEIS_URL, params, self.logger)
        if not res or "schoolInfo" not in res:
            self.logger.error(f"[{region_code}] 첫 페이지 응답 없음")
            return

        # total_count 추출
        try:
            total_count = res["schoolInfo"][0]["head"][0]["list_total_count"]
            total_pages = (total_count + 99) // 100   # 올림 계산
            self.logger.info(f"[{region_code}] 전체 {total_count}개, 총 {total_pages}페이지")
        except (KeyError, IndexError, TypeError) as e:
            self.logger.error(f"[{region_code}] total_count 파싱 실패: {e}")
            return

        # 첫 페이지 처리
        rows = res["schoolInfo"][1].get("row", [])
        if rows:
            batch = []
            for r in rows:
                items = self._process_item(r)
                if items:
                    batch.extend(items)
            if batch:
                self.enqueue(batch)
            self.logger.info(f"[{region_code}] p=1 → {len(rows)}건")

        # 나머지 페이지 처리
        for page in range(2, total_pages + 1):
            params["pIndex"] = page
            retry_count = 0
            success = False

            while retry_count < max_retries_per_page and not success:
                try:
                    res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                    if not res or "schoolInfo" not in res:
                        break

                    rows = res["schoolInfo"][1].get("row", [])
                    if not rows:
                        success = True
                        break

                    batch = []
                    for r in rows:
                        items = self._process_item(r)
                        if items:
                            batch.extend(items)
                    if batch:
                        self.enqueue(batch)

                    self.logger.info(f"[{region_code}] p={page} → {len(rows)}건")
                    success = True
                    time.sleep(0.1)   # 과도한 요청 방지

                except Exception as e:
                    retry_count += 1
                    self.logger.error(f"[{region_code}] p={page} 시도 {retry_count} 실패: {e}")
                    if retry_count >= max_retries_per_page:
                        self.logger.error(f"[{region_code}] p={page} 최종 실패, 다음 지역으로 이동")
                        return   # 현재 지역 포기
                    time.sleep(2 ** retry_count)  # 지수 백오프

        self.logger.info(f"[{region_code}] 수집 완료")
    
    def close(self):
        """종료 처리"""
        self.meta_vocab.close()
        super().close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="학교 기본정보 수집기")
    parser.add_argument("--regions", help="교육청 코드 (예: B10,C10 또는 ALL)")
    parser.add_argument("--shard", default="none", choices=["odd", "even", "none"])
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--compare", action="store_true", help="폐교 감지 (미구현)")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    
    args = parser.parse_args()
    
    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return
    
    # regions 처리
    if args.regions and args.regions.upper() == "ALL":
        regions = ALL_REGIONS
    elif args.regions:
        regions = [r.strip() for r in args.regions.split(",")]
    else:
        regions = ALL_REGIONS
    
    school_range = None if args.school_range == "none" else args.school_range
    
    collector = SchoolMasterCollector(
        shard=args.shard,
        school_range=school_range,
        compare=args.compare,
        debug_mode=args.debug
    )
    
    try:
        for region in regions:
            collector.fetch_region(region)
    finally:
        collector.close()


if __name__ == "__main__":
    main()
    