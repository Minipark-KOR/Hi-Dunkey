#!/usr/bin/env python3
"""
급식 정보 수집기 - 학년도 전체 버전
"""
import calendar
from pathlib import Path
from typing import List
from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.vocab import VocabManager
from core.meta_vocab import MetaVocabManager
from core.meal_extractor import MealMetaExtractor
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR = PROJECT_ROOT / "data" / "active"
GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
UNKNOWN_DB_PATH = str(ACTIVE_DIR / "unknown_patterns.db")
NEIS_URL = NEIS_ENDPOINTS['meal']

class AnnualFullMealCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, debug_mode=False):
        super().__init__("meal", str(ACTIVE_DIR), shard, school_range)
        self.api_context = 'meal'
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")
        self.menu_vocab = VocabManager(GLOBAL_VOCAB_PATH, 'meal', debug_mode=debug_mode)
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.meta_extractor = MealMetaExtractor(UNKNOWN_DB_PATH, batch_size=100)
        self._init_meta_table()
    
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
    
    def fetch_region(self, region: str, **kwargs):
        year = kwargs.get("year", now_kst().year)
        self.fetch_year(region, year)
    
    def fetch_year(self, region: str, year: int):
        month_range = [(year, m) for m in range(3,13)] + [(year+1, m) for m in range(1,3)]
        self.iterate_schools_by_month(
            region, year, month_range,
            lambda sch_code, y, m: self._fetch_school_month(region, sch_code, y, m)
        )
    
    def _fetch_school_month(self, region: str, school_code: str, y: int, m: int):
        last_day = calendar.monthrange(y, m)[1]
        date_from = f"{y}{m:02d}01"
        date_to = f"{y}{m:02d}{last_day:02d}"
        base_params = {
            "ATPT_OFCDC_SC_CODE": region,
            "SD_SCHUL_CODE": school_code,
            "MLSV_FROM_YMD": date_from,
            "MLSV_TO_YMD": date_to,
        }
        rows = self._fetch_paginated(NEIS_URL, base_params, 'mealServiceDietInfo', page_size=100)
        for r in rows:
            items = self._process_item(r)
            if items:
                self.enqueue(items)
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        school_code = self._get_field(raw_item, 'school_code')
        if not school_code or not self._include_school(school_code):
            return []
        school_info = self.get_school_info(school_code)
        if not school_info:
            return []
        meal_date = self._get_field(raw_item, 'meal_date')
        meal_type = self._get_field(raw_item, 'meal_type')
        if not meal_date or not meal_type:
            return []
        original_menu = self._get_field(raw_item, 'menu', default='')
        parsed = parse_meal_html(original_menu)
        if not parsed.get("items"):
            return []
        results = []
        for item in parsed["items"]:
            if not isinstance(item, dict):
                continue
            menu_name = item.get("menu_name")
            if not menu_name:
                continue
            menu_id = self.menu_vocab.get_or_create(menu_name)
            metas = self.meta_extractor.extract(menu_name)
            meta_batch = []
            for meta_type, meta_value in metas:
                meta_id = self.meta_vocab.get_or_create('meal', meta_type, meta_value)
                meta_batch.append((school_info['school_id'], int(meal_date), int(meal_type), menu_id, meta_id))
            d = {
                "school_id": school_info['school_id'],
                "meal_date": int(meal_date),
                "meal_type": int(meal_type),
                "menu_id": menu_id,
                "allergy_info": normalize_allergy_info(item.get("allergies", [])),
                "original_menu": original_menu,
                "cal_info": self._get_field(raw_item, 'calories', default=''),
                "ntr_info": self._get_field(raw_item, 'nutrition', default=''),
                "load_dt": self._get_field(raw_item, 'load_dt') or now_kst().isoformat(),
                "_meta_batch": meta_batch
            }
            results.append(d)
        return results
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            # 메타 정보 추출
            all_meta = []
            for it in batch:
                all_meta.extend(it.pop('_meta_batch', []))
            
            # 본 데이터 (meal) 먼저 저장
            meal_data = [
                (
                    it['school_id'], it['meal_date'], it['meal_type'],
                    it['menu_id'], it['allergy_info'], it['original_menu'],
                    it['cal_info'], it['ntr_info'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO meal VALUES (?,?,?,?,?,?,?,?,?)",
                meal_data
            )
            
            # 그 다음 메타 데이터 저장
            if all_meta:
                conn.executemany(
                    "INSERT OR IGNORE INTO meal_meta VALUES (?,?,?,?,?)",
                    all_meta
                )
    
    def close(self):
        self.menu_vocab.close()
        self.meta_vocab.close()
        self.meta_extractor.close()
        super().close()
        