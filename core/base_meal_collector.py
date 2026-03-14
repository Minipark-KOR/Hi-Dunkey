#!/usr/bin/env python3
# core/base_meal_collector.py
# 개발 가이드: docs/developer_guide.md 참조

import re
from typing import List, Optional
import sqlite3

from core.collector_engine import CollectorEngine
from core.database import get_db_connection
from core.vocab import VocabManager
from core.meta_vocab import MetaVocabManager
from core.meal_extractor import MealMetaExtractor
from core.filters import TextFilter
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.paths import GLOBAL_VOCAB_DB_PATH, UNKNOWN_DB_PATH
from core.kst_time import now_kst


class BaseMealCollector(CollectorEngine):
    """급식 수집기 공통 베이스"""

    def __init__(self, name: str, base_dir: str, shard: str, school_range,
                 debug_mode: bool, **kwargs):
        # CollectorEngine에 quiet_mode 등 kwargs 전달
        super().__init__(name, base_dir, shard, school_range, **kwargs)
        self.api_context = 'meal'
        self.debug_mode = debug_mode
        self.run_date = now_kst().strftime("%Y%m%d")

        meal_normalizer = lambda x: re.sub(
            r'\([^)]*\)', '',
            re.sub(r'[★☆◆◇]', '', TextFilter.normalize_for_id(x))
        )
        self.menu_vocab = self.register_resource(
            VocabManager(GLOBAL_VOCAB_DB_PATH, 'meal',
                         normalize_func=meal_normalizer, debug_mode=debug_mode)
        )
        self.meta_vocab = self.register_resource(
            MetaVocabManager(GLOBAL_VOCAB_DB_PATH, debug_mode)
        )
        self.meta_extractor = self.register_resource(
            MealMetaExtractor(UNKNOWN_DB_PATH, batch_size=100)
        )

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal (
                    school_id     INTEGER NOT NULL,
                    meal_date     INTEGER NOT NULL,
                    meal_type     INTEGER NOT NULL,
                    menu_id       INTEGER NOT NULL,
                    allergy_info  TEXT,
                    original_menu TEXT,
                    cal_info      TEXT,
                    ntr_info      TEXT,
                    load_dt       TEXT,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal_meta (
                    school_id INTEGER NOT NULL,
                    meal_date INTEGER NOT NULL,
                    meal_type INTEGER NOT NULL,
                    menu_id   INTEGER NOT NULL,
                    meta_id   INTEGER,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (school_id, meal_date, meal_type, menu_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_meta ON meal_meta(meta_id)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def _parse_meal_raw(self, raw_item: dict) -> Optional[dict]:
        school_code = (
            self._get_field(raw_item, 'school_code')
            or raw_item.get('SD_SCHUL_CODE')
        )
        if not school_code or not self._include_school(school_code):
            return None
        school_info = self.get_school_info(school_code)
        if not school_info:
            return None
        meal_date = (
            self._get_field(raw_item, 'meal_date')
            or raw_item.get('MLSV_YMD')
        )
        meal_type = (
            self._get_field(raw_item, 'meal_type')
            or raw_item.get('MMEAL_SC_CODE')
        )
        if not meal_date or not meal_type:
            return None
        original_menu = (
            self._get_field(raw_item, 'menu', default='')
            or raw_item.get('DDISH_NM', '')
        )
        return {
            'school_info': school_info,
            'meal_date': int(meal_date),
            'meal_type': int(meal_type),
            'original_menu': original_menu,
            'cal_info': (
                self._get_field(raw_item, 'calories', default='')
                or raw_item.get('CAL_INFO', '')
            ),
            'ntr_info': (
                self._get_field(raw_item, 'nutrition', default='')
                or raw_item.get('NTR_INFO', '')
            ),
            'load_dt': (
                self._get_field(raw_item, 'load_dt')
                or raw_item.get('LOAD_DTM')
                or now_kst().isoformat()
            ),
        }

    def _process_item(self, raw_item: dict) -> List[dict]:
        base = self._parse_meal_raw(raw_item)
        if base is None:
            return []
        parsed = parse_meal_html(base['original_menu'])
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
                meta_batch.append((
                    base['school_info']['school_id'],
                    base['meal_date'],
                    base['meal_type'],
                    menu_id,
                    meta_id
                ))
            results.append({
                "school_id":    base['school_info']['school_id'],
                "meal_date":    base['meal_date'],
                "meal_type":    base['meal_type'],
                "menu_id":      menu_id,
                "allergy_info": normalize_allergy_info(item.get("allergies", [])),
                "original_menu": base['original_menu'],
                "cal_info":     base['cal_info'],
                "ntr_info":     base['ntr_info'],
                "load_dt":      base['load_dt'],
                "_meta_batch":  meta_batch,
            })
        return results

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]):
        all_meta = []
        for it in batch:
            all_meta.extend(it.pop('_meta_batch', []))
        meal_data = [
            (it['school_id'], it['meal_date'], it['meal_type'], it['menu_id'],
             it['allergy_info'], it['original_menu'],
             it['cal_info'], it['ntr_info'], it['load_dt'])
            for it in batch
        ]
        conn.executemany(
            "INSERT OR REPLACE INTO meal VALUES (?,?,?,?,?,?,?,?,?)", meal_data
        )
        if all_meta:
            conn.executemany(
                "INSERT OR REPLACE INTO meal_meta (school_id, meal_date, meal_type, menu_id, meta_id) VALUES (?,?,?,?,?)",
                all_meta
            )
            