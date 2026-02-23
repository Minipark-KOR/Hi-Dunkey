#!/usr/bin/env python3
"""
급식 정보 수집기 (meal)
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
from core.logger import build_logger
from core.network import safe_json_request
from core.id_generator import IDGenerator
from core.shard import should_include
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS, MEAL_TYPES
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['meal']


class MealCollector(BaseCollector):
    def __init__(self, shard="none", incremental=False, full=False):
        super().__init__("meal", BASE_DIR, shard)
        self.incremental = incremental
        self.full = full
        self.run_date = now_kst().strftime("%Y%m%d")
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            # 메뉴 vocab
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_meal (
                    menu_id INTEGER PRIMARY KEY,
                    menu_name TEXT NOT NULL UNIQUE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 급식 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal (
                    school_id    INTEGER NOT NULL,
                    meal_date    INTEGER NOT NULL,
                    meal_type    INTEGER NOT NULL,
                    menu_id      INTEGER NOT NULL,
                    allergy_info TEXT,
                    cal_info     TEXT,
                    ntr_info     TEXT,
                    load_dt      TEXT,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id),
                    FOREIGN KEY (menu_id) REFERENCES vocab_meal(menu_id)
                ) WITHOUT ROWID
            """)
            # 인덱스
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_menu ON meal(menu_id)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 1건 → 여러 개의 메뉴 데이터로 분할"""
        sc_code = raw_item.get('SD_SCHUL_CODE')
        if not sc_code:
            return []
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']
        
        meal_date = raw_item.get('MLSV_YMD')
        meal_type = raw_item.get('MMEAL_SC_CODE')
        if not meal_date or not meal_type:
            return []
        
        parsed = parse_meal_html(raw_item.get('DDISH_NM', ''))
        if not parsed["items"]:
            return []
        
        results = []
        base = {
            "school_id": school_id,
            "meal_date": int(meal_date),
            "meal_type": int(meal_type),
            "cal_info": raw_item.get('CAL_INFO', ''),
            "ntr_info": raw_item.get('NTR_INFO', ''),
            "load_dt": raw_item.get('LOAD_DTM') or now_kst().isoformat(),
            "vocab": parsed["vocab"]
        }
        for item in parsed["items"]:
            d = base.copy()
            d["menu_id"] = item["menu_id"]
            d["allergy_info"] = normalize_allergy_info(item["allergies"])
            results.append(d)
        return results
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            # vocab 저장
            vocab_set = set()
            for item in batch:
                for mid, name in item.get('vocab', {}).items():
                    vocab_set.add((mid, name))
            if vocab_set:
                conn.executemany(
                    "INSERT OR IGNORE INTO vocab_meal (menu_id, menu_name) VALUES (?, ?)",
                    list(vocab_set)
                )
            # meal 저장
            meal_data = [
                (
                    item['school_id'], item['meal_date'], item['meal_type'],
                    item['menu_id'], item['allergy_info'],
                    item['cal_info'], item['ntr_info'], item['load_dt']
                )
                for item in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO meal
                VALUES (?,?,?,?,?,?,?,?)
            """, meal_data)
    
    def fetch_region(self, region: str, date: str):
        """지역별 급식 수집"""
        p_idx = 1
        consecutive_errors = 0
        while p_idx <= 50:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region,
                "MLSV_YMD": date
            }
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "mealServiceDietInfo" not in res:
                    break
                rows = res["mealServiceDietInfo"][1].get("row", [])
                if not rows:
                    break
                
                batch = []
                for r in rows:
                    sc_code = r.get('SD_SCHUL_CODE')
                    if not sc_code:
                        continue
                    if not should_include(self.shard, sc_code):
                        continue
                    items = self._process_item(r)
                    batch.extend(items)
                
                if batch:
                    self.enqueue(batch)
                self.logger.info(f"[{region}] p={p_idx} → {len(rows)}건, 메뉴 {len(batch)}개")
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
    parser = argparse.ArgumentParser(description="급식 수집기")
    parser.add_argument("--regions", required=True, help="B10,C10,... 또는 ALL")
    parser.add_argument("--date", default=now_kst().strftime("%Y%m%d"))
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"])
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    regions = ALL_REGIONS if args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]

    collector = MealCollector(
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
    