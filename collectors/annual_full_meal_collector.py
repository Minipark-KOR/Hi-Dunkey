#!/usr/bin/env python3
"""
급식 정보 수집기 - 학년도 전체 버전 (학교별 순회, 샤딩 지원)
- 지정한 학년도(3월~익년 2월) 전체 데이터를 수집합니다.
- --year 미지정 시 실행 시점의 학년도를 자동으로 사용합니다.
- 수집 완료 후 --metrics 옵션으로 메트릭 요약 생성 가능
"""
import os
import sys
import json
import argparse
import time
import calendar
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List

sys.path.append(str(Path(__file__).resolve().parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.vocab import VocabManager
from core.meta_vocab import MetaVocabManager
from core.meal_extractor import MealMetaExtractor
from core.filters import TextFilter
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

try:
    from core.metrics import build_summary_markdown, save_summary, collect_domain_metrics
    from constants.domains import DOMAIN_CONFIG, GLOBAL_DBS
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR   = PROJECT_ROOT / "data" / "active"
METRICS_DIR  = PROJECT_ROOT / "data" / "metrics"
MASTER_DB    = PROJECT_ROOT / "data" / "master" / "school_master.db"

GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
UNKNOWN_DB_PATH   = str(ACTIVE_DIR / "unknown_patterns.db")
NEIS_URL          = NEIS_ENDPOINTS["meal"]

os.makedirs(str(ACTIVE_DIR), exist_ok=True)


class AnnualFullMealCollector(BaseCollector):
    """급식 수집기 (학년도 전체, 학교별 순회, 샤딩 지원)"""

    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 debug_mode: bool = False):
        if shard == "none":
            db_name = "meal.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"meal_{shard}{range_suffix}.db"

        super().__init__("meal", str(ACTIVE_DIR), shard, school_range)

        self.debug_mode = debug_mode
        self.run_date   = now_kst().strftime("%Y%m%d")

        meal_normalizer = lambda x: re.sub(
            r'\([^)]*\)', '',
            re.sub(r'[★☆◆◇]', '', TextFilter.normalize_for_id(x))
        )
        self.menu_vocab    = VocabManager(
            GLOBAL_VOCAB_PATH, "meal",
            normalize_func=meal_normalizer,
            debug_mode=debug_mode
        )
        self.meta_vocab    = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.meta_extractor = MealMetaExtractor(UNKNOWN_DB_PATH, batch_size=100)

        self._init_meta_table()
        self.logger.info(
            f"🍽️ AnnualFullMealCollector 초기화 완료 "
            f"(shard={shard}, range={school_range})"
        )

    def _init_meta_table(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meal_meta (
                    school_id INTEGER NOT NULL,
                    meal_date INTEGER NOT NULL,
                    meal_type INTEGER NOT NULL,
                    menu_id   INTEGER NOT NULL,
                    meta_id   INTEGER NOT NULL,
                    PRIMARY KEY (school_id, meal_date, meal_type, menu_id, meta_id)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_meal_meta ON meal_meta(meta_id)"
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)"
            )
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def fetch_region(self, region: str, **kwargs):
        year = kwargs.get("year", now_kst().year)
        self.fetch_year(region, year)

    def fetch_year(self, region: str, year: int):
        """해당 지역의 모든 학교를 DB에서 가져와 학교별로 순회 수집 (샤딩/범위 필터 적용)"""
        if not MASTER_DB.exists():
            self.logger.error(f"❌ 학교 마스터 DB가 없습니다: {MASTER_DB}")
            return

        with get_db_connection(str(MASTER_DB)) as conn:
            schools = conn.execute(
                """
                SELECT sc_code, sc_name
                FROM schools
                WHERE atpt_code = ? AND status = '운영'
                """,
                (region,)
            ).fetchall()

        if not schools:
            self.logger.warning(f"⚠️ [{region}] 운영 중인 학교 정보 없음")
            return

        target_schools = []
        for sc_code, sc_name in schools:
            if self._include_school(sc_code):
                target_schools.append((sc_code, sc_name))

        self.logger.info(
            f"🚀 [{region}] 전체 {len(schools)}개 학교 중 "
            f"{len(target_schools)}개 수집 대상 (학년도 {year})"
        )

        if not target_schools:
            self.logger.info(f"  [{region}] 수집 대상 학교 없음")
            return

        months = [
            (year, 3), (year, 4), (year, 5), (year, 6),
            (year, 7), (year, 8), (year, 9), (year, 10),
            (year, 11), (year, 12),
            (year + 1, 1), (year + 1, 2)
        ]

        for idx, (sch_code, sch_name) in enumerate(target_schools, 1):
            self.logger.info(f"  [{region}] 진행 {idx}/{len(target_schools)}: {sch_name} ({sch_code})")
            for y, m in months:
                last_day  = calendar.monthrange(y, m)[1]
                date_from = f"{y}{m:02d}01"
                date_to   = f"{y}{m:02d}{last_day:02d}"
                try:
                    self._fetch_date_range(region, date_from, date_to, sch_code)
                except Exception as e:
                    self.logger.error(f"    ❌ {y}년 {m}월 수집 실패: {e}")
                time.sleep(0.05)
            time.sleep(0.1)

        self.logger.info(f"✅ [{region}] 모든 학교 수집 완료")

    def fetch_daily(self, region: str, target_date: str, school_code: str = None):
        if school_code is None:
            self.logger.error("fetch_daily에는 school_code가 필요합니다.")
            return
        self._fetch_date_range(region, target_date, target_date, school_code)

    def _fetch_date_range(self, region: str, date_from: str, date_to: str,
                          school_code: str = None, max_page: int = 200):
        if school_code is None:
            self.logger.error("_fetch_date_range: school_code 필수")
            return

        p_idx = 1
        while p_idx <= max_page:
            params = {
                "KEY":               NEIS_API_KEY,
                "Type":              "json",
                "pIndex":            p_idx,
                "pSize":             100,
                "ATPT_OFCDC_SC_CODE": region,
                "SD_SCHUL_CODE":      school_code,
                "MLSV_FROM_YMD":     date_from,
                "MLSV_TO_YMD":       date_to,
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

                self.logger.debug(
                    f"    [{region}] {school_code} {date_from}~{date_to} "
                    f"p={p_idx} → {len(rows)}건"
                )

                if len(rows) < 100:
                    break
                p_idx += 1
                time.sleep(0.05)

            except Exception as e:
                self.logger.error(f"    [{region}] {school_code} p={p_idx} 에러: {e}")
                if p_idx > 5:
                    break
                p_idx += 1
                time.sleep(2)

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
        if not parsed.get("items"):
            return []

        results = []
        for item in parsed["items"]:
            if not isinstance(item, dict):
                self.logger.warning(f"  예상치 못한 item 타입: {type(item)} - {item}")
                continue
            menu_name = item.get("menu_name")
            if not menu_name:
                self.logger.warning(f"  menu_name 없는 item: {item}")
                continue

            menu_id = self.menu_vocab.get_or_create(menu_name)

            metas = self.meta_extractor.extract(menu_name)
            for meta_type, meta_value in metas:
                meta_id = self.meta_vocab.get_or_create('meal', meta_type, meta_value)
                self._save_meta(school_info['school_id'], int(meal_date),
                               int(meal_type), menu_id, meta_id)

            d = {
                "school_id": school_info['school_id'],
                "meal_date": int(meal_date),
                "meal_type": int(meal_type),
                "menu_id": menu_id,
                "allergy_info": normalize_allergy_info(item.get("allergies", [])),
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

    def close(self):
        self.menu_vocab.close()
        self.meta_vocab.close()
        self.meta_extractor.close()
        super().close()


def _save_meal_metrics():
    if not METRICS_AVAILABLE:
        print("⚠️ metrics 모듈 없음 (core.metrics 또는 constants.domains 확인)")
        return

    backup_date = now_kst().strftime("%Y%m%d")
    os.makedirs(str(METRICS_DIR), exist_ok=True)

    meal_cfg = DOMAIN_CONFIG.get("meal", {
        "db_path": "data/active/meal/meal.db",
        "table":   "meal",
        "enabled": True,
    })
    domain_subset = {"meal": meal_cfg}

    markdown = build_summary_markdown(
        backup_date=backup_date,
        base_dir=str(PROJECT_ROOT),
        domain_config=domain_subset,
        global_dbs=GLOBAL_DBS,
        include_geo=False,
        include_global_tables=True,
    )

    summary_path = save_summary(markdown, str(METRICS_DIR), backup_date)
    print(f"📄 메트릭 요약 저장: {summary_path}")

    db_path = PROJECT_ROOT / meal_cfg["db_path"]
    metrics = {"meal": collect_domain_metrics(str(db_path), meal_cfg["table"])}
    metrics_path = METRICS_DIR / f"metrics_{backup_date}.json"
    with open(str(metrics_path), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"📊 JSON 메트릭 저장: {metrics_path}")


def main():
    parser = argparse.ArgumentParser(description="급식 수집기 (학년도 전체, 학교별 순회, 샤딩 지원)")
    parser.add_argument("--regions", required=True,
                        help="교육청 코드 (예: B10,C10 또는 ALL)")
    parser.add_argument("--year", type=int,
                        default=get_current_school_year(now_kst()),   # ✅ 자동 학년도
                        help="수집할 학년도 (기본: 현재 학년도)")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none",
                        help="샤드 구분 (odd/even)")
    parser.add_argument("--school_range", choices=["A", "B"], default=None,
                        help="학교 범위 (생략 시 전체)")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--metrics", action="store_true",
                        help="수집 후 메트릭 요약 생성 (data/metrics/)")
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수 없음")
        return

    collector = AnnualFullMealCollector(
        shard=args.shard,
        school_range=args.school_range,
        debug_mode=args.debug,
    )

    regions = (
        ALL_REGIONS if args.regions.upper() == "ALL"
        else [r.strip() for r in args.regions.split(",")]
    )

    try:
        for region in regions:
            collector.fetch_year(region, args.year)
    finally:
        collector.close()

    if args.metrics:
        _save_meal_metrics()


if __name__ == "__main__":
    main()
    