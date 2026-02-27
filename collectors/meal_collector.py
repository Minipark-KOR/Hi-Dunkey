#!/usr/bin/env python3
"""
급식 정보 수집기 - 최종 통합 버전
"""
import os
import sqlite3
import time
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.vocab import VocabManager
from core.meta_vocab import MetaVocabManager
from core.meal_extractor import MealMetaExtractor
from core.filters import TextFilter  # ✅ 수정: text_filter → filters
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

GLOBAL_VOCAB_PATH = os.path.join(BASE_DIR, "global_vocab.db")
UNKNOWN_DB_PATH = os.path.join(BASE_DIR, "unknown_patterns.db")
NEIS_URL = NEIS_ENDPOINTS['meal']


class MealCollector(BaseCollector):
    """급식 수집기"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False, debug_mode: bool = False):
        if shard == "none":
            db_name = "meal.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"meal_{shard}{range_suffix}.db"
        
        db_path = os.path.join(BASE_DIR, db_name)
        
        super().__init__("meal", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")
        
        meal_normalizer = lambda x: re.sub(r'\([^)]*\)', '', 
                                          re.sub(r'[★☆◆◇]', '', 
                                                TextFilter.normalize_for_id(x)))
        self.menu_vocab = VocabManager(GLOBAL_VOCAB_PATH, 'meal', 
                                       normalize_func=meal_normalizer,
                                       debug_mode=debug_mode)
        
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.meta_extractor = MealMetaExtractor(UNKNOWN_DB_PATH, batch_size=100)
        
        self._init_meta_table()
        
        self.logger.info(f"🍽️ MealCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _init_meta_table(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal_meta (
                    school_id INTEGER NOT NULL,
                    meal_date INTEGER NOT NULL,
                    meal_type INTEGER NOT NULL,
                    menu_id INTEGER NOT NULL,
                    meta_id INTEGER NOT NULL,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id, meta_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_meta ON meal_meta(meta_id)")
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal (
                    school_id INTEGER NOT NULL,
                    meal_date INTEGER NOT NULL,
                    meal_type INTEGER NOT NULL,
                    menu_id INTEGER NOT NULL,
                    allergy_info TEXT,
                    original_menu TEXT,
                    cal_info TEXT,
                    ntr_info TEXT,
                    load_dt TEXT,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        school_code = raw_item.get('SD_SCHUL_CODE')
        if not school_code or not self._include_school(school_code):
            return []
        
        school_info = self.get_school_info(school_code)
        if not school_info:
            return []
        
        meal_date = raw_item.get('MLSV_YMD')
        meal_type = raw_item.get('MMEAL_SC_CODE')
        if not meal_date or not meal_type:
            return []
        
        original_menu = raw_item.get('DDISH_NM', '')
        parsed = parse_meal_html(original_menu)
        if not parsed["items"]:
            return []
        
        results = []
        for item in parsed["items"]:
            menu_id = self.menu_vocab.get_or_create(item["menu_name"])
            
            metas = self.meta_extractor.extract(item["menu_name"])
            for meta_type, meta_value in metas:
                meta_id = self.meta_vocab.get_or_create('meal', meta_type, meta_value)
                self._save_meta(school_info['school_id'], int(meal_date), 
                               int(meal_type), menu_id, meta_id)
            
            d = {
                "school_id": school_info['school_id'],
                "meal_date": int(meal_date),
                "meal_type": int(meal_type),
                "menu_id": menu_id,
                "allergy_info": normalize_allergy_info(item["allergies"]),
                "original_menu": original_menu,
                "cal_info": raw_item.get('CAL_INFO', ''),
                "ntr_info": raw_item.get('NTR_INFO', ''),
                "load_dt": raw_item.get('LOAD_DTM') or now_kst().isoformat()
            }
            results.append(d)
        
        return results
    
    def _save_meta(self, school_id: int, meal_date: int, meal_type: int, 
                   menu_id: int, meta_id: int):
        try:
            with get_db_connection(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO meal_meta 
                    VALUES (?, ?, ?, ?, ?)
                """, (school_id, meal_date, meal_type, menu_id, meta_id))
        except Exception as e:
            self.logger.error(f"메타 저장 실패: {e}")
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            meal_data = [
                (
                    it['school_id'], it['meal_date'], it['meal_type'],
                    it['menu_id'], it['allergy_info'], it['original_menu'],
                    it['cal_info'], it['ntr_info'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO meal VALUES (?,?,?,?,?,?,?,?,?)
            """, meal_data)
    
    def fetch_daily(self, region: str, target_date: str):
        d = date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:]))
        tomorrow = (d + timedelta(days=1)).strftime("%Y%m%d")
        self._fetch_date_range(region, target_date, target_date)
        self._fetch_date_range(region, tomorrow, tomorrow)
    
    def _fetch_date_range(self, region: str, date_from: str, date_to: str, max_page: int = 200):
        p_idx = 1
        while p_idx <= max_page:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region,
                "MLSV_FROM_YMD": date_from,
                "MLSV_TO_YMD": date_to,
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
        self.menu_vocab.close()
        self.meta_vocab.close()
        self.meta_extractor.close()
        super().close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="급식 수집기")
    parser.add_argument("--regions", required=True, help="교육청 코드")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()
    
    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return
    
    school_range = None if args.school_range == "none" else args.school_range
    
    collector = MealCollector(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug
    )
    
    regions = ALL_REGIONS if args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]
    
    today = now_kst().strftime("%Y%m%d")
    
    try:
        for region in regions:
            collector.fetch_daily(region, today)
    finally:
        collector.close()


if __name__ == "__main__":
    main()
    