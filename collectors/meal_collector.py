#!/usr/bin/env python3
"""
급식 정보 수집기 - 최종 통합 버전
- 제네릭 VocabManager 사용
- 통합 MetaVocabManager 사용
- original_menu 보존
- unknown 패턴 수집
"""
import os
import sqlite3
import time
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Set, Tuple, Optional

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.vocab import VocabManager
from core.meta_vocab import MetaVocabManager
from core.meal_extractor import MealMetaExtractor
from core.text_filter import TextFilter
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

# 공통 vocab DB 경로
GLOBAL_VOCAB_PATH = os.path.join(BASE_DIR, "global_vocab.db")
# unknown 패턴 저장 DB
UNKNOWN_DB_PATH = os.path.join(BASE_DIR, "unknown_patterns.db")

NEIS_URL = NEIS_ENDPOINTS['meal']


class MealCollector(BaseCollector):
    """급식 수집기 - 최종 통합 버전"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 debug_mode: bool = False):
        # 샤드 DB 경로
        if shard == "none":
            db_name = "meal.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"meal_{shard}{range_suffix}.db"
        
        db_path = os.path.join(BASE_DIR, db_name)
        
        # BaseCollector 초기화
        super().__init__("meal", BASE_DIR, shard, school_range)
        
        # 제네릭 VocabManager (급식용 정규화 함수 주입)
        meal_normalizer = lambda x: re.sub(r'\([^)]*\)', '', 
                                          re.sub(r'[★☆◆◇]', '', 
                                                TextFilter.normalize_for_id(x)))
        self.menu_vocab = VocabManager(
            GLOBAL_VOCAB_PATH, 
            'meal', 
            normalize_func=meal_normalizer,
            debug_mode=debug_mode
        )
        
        # 통합 MetaVocabManager
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        
        # 메타데이터 추출기 (unknown 패턴 저장 기능 포함)
        self.meta_extractor = MealMetaExtractor(UNKNOWN_DB_PATH, batch_size=100)
        
        # meal_meta 테이블 생성 (샤드 DB에)
        self._init_meta_table()
        
        self.run_date = now_kst().strftime("%Y%m%d")
        
        self.logger.info(f"🍽️ MealCollector 초기화 완료 (shard={shard}, range={school_range})")
        if debug_mode:
            self.logger.info(f"  - unknown 패턴 DB: {UNKNOWN_DB_PATH}")
    
    def _init_meta_table(self):
        """샤드 DB에 meal_meta 테이블 생성"""
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal_meta (
                    school_id  INTEGER NOT NULL,
                    meal_date  INTEGER NOT NULL,
                    meal_type  INTEGER NOT NULL,
                    menu_id    INTEGER NOT NULL,
                    meta_id    INTEGER NOT NULL,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id, meta_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_meta ON meal_meta(meta_id)")
    
    def _init_db(self):
        """메인 테이블 초기화 (original_menu 컬럼 포함)"""
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal (
                    school_id    INTEGER NOT NULL,
                    meal_date    INTEGER NOT NULL,
                    meal_type    INTEGER NOT NULL,
                    menu_id      INTEGER NOT NULL,
                    allergy_info TEXT,
                    original_menu TEXT,  -- 원본 메뉴 저장!
                    cal_info     TEXT,
                    ntr_info     TEXT,
                    load_dt      TEXT,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_original ON meal(original_menu)")
            
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return self.run_date
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 아이템 처리"""
        school_code = raw_item.get('SD_SCHUL_CODE')
        if not school_code:
            return []
        
        # 샤드/범위 필터링
        if not self._include_school(school_code):
            return []
        
        school_info = self.get_school_info(school_code)
        if not school_info:
            return []
        school_id = school_info['school_id']
        
        meal_date = raw_item.get('MLSV_YMD')
        meal_type = raw_item.get('MMEAL_SC_CODE')
        if not meal_date or not meal_type:
            return []
        
        # 원본 메뉴 저장
        original_menu = raw_item.get('DDISH_NM', '')
        
        # 메뉴 파싱
        parsed = parse_meal_html(original_menu)
        if not parsed["items"]:
            return []
        
        results = []
        
        for item in parsed["items"]:
            # 1. 메뉴 ID 생성/조회 (개별 메뉴명 사용)
            menu_id = self.menu_vocab.get_or_create(item["menu_name"])
            
            # 2. 개별 메뉴명에서 메타데이터 추출
            metas = self.meta_extractor.extract(item["menu_name"])
            for meta_type, meta_value in metas:
                meta_id = self.meta_vocab.get_or_create('meal', meta_type, meta_value)
                self._save_meta(school_id, int(meal_date), int(meal_type), menu_id, meta_id)
            
            # 3. 급식 데이터 생성
            d = {
                "school_id": school_id,
                "meal_date": int(meal_date),
                "meal_type": int(meal_type),
                "menu_id": menu_id,
                "allergy_info": normalize_allergy_info(item["allergies"]),
                "original_menu": original_menu,  # 전체 원본 저장!
                "cal_info": raw_item.get('CAL_INFO', ''),
                "ntr_info": raw_item.get('NTR_INFO', ''),
                "load_dt": raw_item.get('LOAD_DTM') or now_kst().isoformat()
            }
            results.append(d)
        
        return results
    
    def _save_meta(self, school_id: int, meal_date: int, meal_type: int, 
                   menu_id: int, meta_id: int):
        """메타데이터 연결 저장"""
        try:
            with get_db_connection(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO meal_meta 
                    (school_id, meal_date, meal_type, menu_id, meta_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (school_id, meal_date, meal_type, menu_id, meta_id))
        except Exception as e:
            self.logger.error(f"메타 저장 실패: {e}")
    
    def _save_batch(self, batch: List[dict]):
        """배치 데이터 저장"""
        with get_db_connection(self.db_path) as conn:
            meal_data = [
                (
                    item['school_id'], item['meal_date'], item['meal_type'],
                    item['menu_id'], item['allergy_info'], item['original_menu'],
                    item['cal_info'], item['ntr_info'], item['load_dt']
                )
                for item in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO meal 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, meal_data)
    
    # --------------------------------------------------------
    # 날짜별 수집 메서드들
    # --------------------------------------------------------
    def fetch_daily(self, region: str, target_date: str):
        """오늘 + 내일 수집"""
        d = date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:]))
        tomorrow_str = (d + timedelta(days=1)).strftime("%Y%m%d")
        
        self._fetch_date_range(region, target_date, target_date, max_page=50)
        self.logger.info(f"[{region}] {target_date} 오늘 수집 완료")
        
        self._fetch_date_range(region, tomorrow_str, tomorrow_str, max_page=50)
        self.logger.info(f"[{region}] {tomorrow_str} 내일 수집 완료")
    
    def fetch_monthly_incremental(self, region: str, month: str):
        """매주 월요일 수집"""
        year, m = int(month[:4]), int(month[4:])
        self._collect_two_months_diff(region, year, m, label="월요일")
    
    def _fetch_date_range(self, region: str, date_from: str, date_to: str, max_page: int = 200):
        """날짜 범위 수집"""
        p_idx = 1
        consecutive_errors = 0
        single = (date_from == date_to)
        
        while p_idx <= max_page:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region,
            }
            if single:
                params["MLSV_YMD"] = date_from
            else:
                params["MLSV_FROM_YMD"] = date_from
                params["MLSV_TO_YMD"] = date_to
            
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "mealServiceDietInfo" not in res:
                    break
                
                rows = res["mealServiceDietInfo"][1].get("row", [])
                if not rows:
                    break
                
                batch = []
                for r in rows:
                    parsed_items = self._process_item(r)
                    if parsed_items:
                        batch.extend(parsed_items)
                
                if batch:
                    self.enqueue(batch)
                
                self.logger.info(
                    f"[{region}] {date_from}~{date_to} p={p_idx} → {len(rows)}건, 메뉴 {len(batch)}개"
                )
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
    
    def _collect_two_months_diff(self, region: str, year: int, m: int, label: str):
        """두 달치 diff 수집 (기존 로직 유지)"""
        # ... 기존 코드와 동일 (생략)
        pass
    
    def close(self):
        """정리 작업"""
        self.menu_vocab.close()
        self.meta_vocab.close()
        self.meta_extractor.close()
        super().close()


# ========================================================
# MAIN
# ========================================================
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="급식 수집기 - 최종 통합 버전")
    parser.add_argument("--regions", required=True, help="교육청 코드 (콤마 구분, 예: B10,C10 또는 ALL)")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="하루 수집 YYYYMMDD")
    group.add_argument("--month", help="월간 수집 YYYYMM")
    group.add_argument("--endmonth", help="말일 수집 YYYYMM")
    
    args = parser.parse_args()
    
    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return
    
    # 'none'이면 None으로 변환
    school_range = None if args.school_range == "none" else args.school_range
    
    collector = MealCollector(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug
    )
    
    regions = ALL_REGIONS if args.regions.upper() == "ALL" else \
              [r.strip() for r in args.regions.split(",")]
    
    try:
        for region in regions:
            collector.logger.info(f"🚀 {region} 수집 시작")
            
            if args.month:
                collector.fetch_monthly_incremental(region, args.month)
            elif args.endmonth:
                # collector.fetch_end_of_month(region, args.endmonth)
                pass
            else:
                collector.fetch_daily(region, args.date)
    finally:
        collector.close()


if __name__ == "__main__":
    main()
    