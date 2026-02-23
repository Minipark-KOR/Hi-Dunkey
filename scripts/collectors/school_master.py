#!/usr/bin/env python3
"""
학교 기본정보 수집기 (master)
- 샤딩 지원 (odd/even)
- --merge 옵션으로 샤드 병합
- --incremental / --full / --compare 옵션
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Optional

# 프로젝트 루트를 path에 추가 (실행 시)
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.logger import build_logger
from core.network import safe_json_request
from core.school_id import create_school_id
from core.backup import vacuum_into, move_files_by_age, cleanup_files_older_than, get_block_range
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS, API_CONFIG
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/master")
os.makedirs(BASE_DIR, exist_ok=True)

TOTAL_DB = os.path.join(BASE_DIR, "school_master_TOTAL.db")
NEIS_URL = NEIS_ENDPOINTS['school']


class SchoolMasterCollector(BaseCollector):
    def __init__(self, shard="none", incremental=False, full=False, compare=False):
        super().__init__("school", BASE_DIR, shard)
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.total_db_path = TOTAL_DB
        
        # 체크포인트 키 (오늘 날짜)
        self.run_date = now_kst().strftime("%Y%m%d")
    
    def _init_db(self):
        """schools 테이블 생성"""
        with get_db_connection(self.db_path) as conn:
            # 교육청 vocab (선택)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_office (
                    atpt_code TEXT PRIMARY KEY,
                    atpt_name TEXT,
                    last_updated TEXT
                )
            """)
            # schools 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    atpt_code    TEXT,
                    sc_code      TEXT PRIMARY KEY,
                    school_id    INTEGER,   -- 압축된 ID
                    sc_name      TEXT,
                    eng_name     TEXT,
                    sc_kind      TEXT,
                    location     TEXT,
                    foundation   TEXT,
                    branch_type  TEXT,
                    hs_category  TEXT,
                    coed         TEXT,
                    address      TEXT,
                    tel          TEXT,
                    homepage     TEXT,
                    status       TEXT DEFAULT '운영',
                    last_seen    INTEGER,
                    load_dt      TEXT
                )
            """)
            # 인덱스
            conn.execute("CREATE INDEX IF NOT EXISTS idx_atpt ON schools(atpt_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON schools(status)")
            # 체크포인트 테이블
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> dict:
        """API 응답 1건 → 저장용 dict"""
        atpt_code = raw_item.get("ATPT_OFCDC_SC_CODE", "")
        sc_code = raw_item.get("SD_SCHUL_CODE", "")
        if not sc_code:
            return {}
        
        school_id = create_school_id(atpt_code, sc_code)
        
        return {
            "atpt_code": atpt_code,
            "sc_code": sc_code,
            "school_id": school_id,
            "sc_name": raw_item.get("SCHUL_NM", ""),
            "eng_name": raw_item.get("ENG_SCHUL_NM", ""),
            "sc_kind": raw_item.get("SCHUL_KND_SC_NM", ""),
            "location": raw_item.get("LCTN_SC_NM", ""),
            "foundation": raw_item.get("FOND_SC_NM", ""),
            "branch_type": raw_item.get("FO_SC_NM", "본교"),
            "hs_category": raw_item.get("HS_GNRL_BUSI_SC_NM", "해당없음"),
            "coed": raw_item.get("COEDU_SC_NM", ""),
            "address": raw_item.get("ORG_RDNMA", ""),
            "tel": raw_item.get("ORG_TELNO", ""),
            "homepage": raw_item.get("HMPG_ADRES", ""),
            "status": "운영",
            "last_seen": int(self.run_date),
            "load_dt": now_kst().isoformat()
        }
    
    def _save_batch(self, batch: List[dict]):
        """배치 저장 (UPSERT)"""
        with get_db_connection(self.db_path) as conn:
            # vocab_office 저장
            office_data = list({(it['atpt_code'], it['atpt_code'][:2] + " 교육청", self.run_date) for it in batch})
            conn.executemany(
                "INSERT OR REPLACE INTO vocab_office VALUES (?,?,?)",
                office_data
            )
            # schools 저장
            school_data = [
                (
                    it['atpt_code'], it['sc_code'], it['school_id'],
                    it['sc_name'], it['eng_name'], it['sc_kind'],
                    it['location'], it['foundation'], it['branch_type'],
                    it['hs_category'], it['coed'], it['address'],
                    it['tel'], it['homepage'], it['status'],
                    it['last_seen'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO schools 
                (atpt_code, sc_code, school_id, sc_name, eng_name, sc_kind,
                 location, foundation, branch_type, hs_category, coed,
                 address, tel, homepage, status, last_seen, load_dt)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, school_data)
    
    def fetch_region(self, region_code: str):
        """특정 교육청의 학교 목록 수집"""
        p_idx = 1
        consecutive_errors = 0
        while p_idx <= 50:
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
                    # 샤드 필터링
                    from core.shard import should_include
                    if not should_include(self.shard, sc_code):
                        continue
                    
                    item = self._process_item(r)
                    if item:
                        batch.append(item)
                
                if batch:
                    self.enqueue(batch)
                self.logger.info(f"[{region_code}] p={p_idx} → {len(rows)}건")
                consecutive_errors = 0
                
                if len(rows) < 1000:
                    break
                p_idx += 1
                time.sleep(0.1)
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"[{region_code}] p={p_idx} 에러: {e}")
                if consecutive_errors >= 5:
                    break
                p_idx += 1
                time.sleep(2 ** min(consecutive_errors, 5))
    
    def merge_shards(self):
        """홀수/짝수 샤드 병합 (--merge)"""
        from core.backup import vacuum_into
        main_path = os.path.join(BASE_DIR, "school_master.db")
        even_path = os.path.join(BASE_DIR, "school_master_even.db")
        odd_path = os.path.join(BASE_DIR, "school_master_odd.db")
        
        with sqlite3.connect(main_path) as main_conn:
            main_conn.execute("PRAGMA foreign_keys = ON")
            # schools 테이블 생성 (shard 컬럼 추가)
            main_conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    atpt_code TEXT,
                    sc_code TEXT PRIMARY KEY,
                    school_id INTEGER,
                    sc_name TEXT,
                    eng_name TEXT,
                    sc_kind TEXT,
                    location TEXT,
                    foundation TEXT,
                    branch_type TEXT,
                    hs_category TEXT,
                    coed TEXT,
                    address TEXT,
                    tel TEXT,
                    homepage TEXT,
                    status TEXT DEFAULT '운영',
                    last_seen INTEGER,
                    load_dt TEXT,
                    shard TEXT GENERATED ALWAYS AS (
                        CASE WHEN CAST(SUBSTR(sc_code, -1) AS INTEGER) % 2 = 0 
                        THEN 'even' ELSE 'odd' END
                    ) STORED
                )
            """)
            # 각 샤드 데이터 병합
            for shard_name, shard_path in [('even', even_path), ('odd', odd_path)]:
                if os.path.exists(shard_path):
                    main_conn.execute(f"ATTACH DATABASE '{shard_path}' AS shard_db")
                    main_conn.execute("""
                        INSERT OR REPLACE INTO main.schools 
                        SELECT *, ? FROM shard_db.schools
                    """, (shard_name,))
                    main_conn.execute("DETACH DATABASE shard_db")
            main_conn.commit()
        self.logger.info(f"✅ 샤드 병합 완료: {main_path}")
        
        # 샤드 파일 백업
        backup_dir = os.path.join(BASE_DIR, "backup", str(now_kst().year))
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = now_kst().strftime("%Y%m%d_%H%M%S")
        for shard_path in [even_path, odd_path]:
            if os.path.exists(shard_path):
                dst = os.path.join(backup_dir, f"school_master_{os.path.basename(shard_path)}")
                vacuum_into(shard_path, dst)
                os.remove(shard_path)
                self.logger.info(f"📦 샤드 백업: {dst}")
    
    def close(self):
        """종료 시 날짜 백업 생성 (full 모드에서만)"""
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="학교 기본정보 수집기")
    parser.add_argument("--regions", help="B10,C10,... 또는 ALL (기본: ALL)")
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"])
    parser.add_argument("--incremental", action="store_true", help="변경사항만 저장")
    parser.add_argument("--full", action="store_true", help="전체 수집 후 날짜 백업 생성")
    parser.add_argument("--compare", action="store_true", help="이전 데이터와 비교하여 변경분만 저장 (incremental과 함께 사용)")
    parser.add_argument("--merge", action="store_true", help="샤드 병합 실행")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    args = parser.parse_args()

    if args.merge:
        collector = SchoolMasterCollector()
        collector.merge_shards()
        return

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    regions = ALL_REGIONS if not args.regions or args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]

    collector = SchoolMasterCollector(
        shard=args.shard,
        incremental=args.incremental,
        full=args.full,
        compare=args.compare
    )

    for region in regions:
        collector.logger.info(f"🚀 {region} 수집 시작")
        collector.fetch_region(region)

    collector.close()
    collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
    