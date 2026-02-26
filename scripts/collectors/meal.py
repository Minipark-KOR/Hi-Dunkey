#!/usr/bin/env python3
"""
급식 정보 수집기 (meal)
- 매일 (--date):          오늘 + 내일 수집
- 매주 월요일 (--month):   이번 달 + 다음 달 diff 증분
- 말일 -3일 (--endmonth):  이번 달 + 다음 달 마지막 diff
"""
import os
import argparse
import sqlite3
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Set, Tuple

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from core.shard import should_include
from parsers.meal_parser import parse_meal_html, normalize_allergy_info
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['meal']


# --------------------------------------------------------
# 날짜 헬퍼
# --------------------------------------------------------
def _month_last_day(year: int, month: int) -> date:
    """해당 월의 말일 date 반환"""
    next_month = month % 12 + 1
    next_year  = year + (1 if month == 12 else 0)
    return date(next_year, next_month, 1) - timedelta(days=1)

def _next_month(year: int, month: int) -> Tuple[int, int]:
    """다음 달 (year, month) 반환"""
    if month == 12:
        return year + 1, 1
    return year, month + 1


class MealCollector(BaseCollector):
    def __init__(self, shard="none", incremental=False, full=False):
        super().__init__("meal", BASE_DIR, shard)
        self.incremental = incremental
        self.full = full
        self.run_date = now_kst().strftime("%Y%m%d")

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_meal (
                    menu_id INTEGER PRIMARY KEY,
                    menu_name TEXT NOT NULL UNIQUE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_menu ON meal(menu_id)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def _process_item(self, raw_item: dict) -> List[dict]:
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
            "cal_info":  raw_item.get('CAL_INFO', ''),
            "ntr_info":  raw_item.get('NTR_INFO', ''),
            "load_dt":   raw_item.get('LOAD_DTM') or now_kst().isoformat(),
            "vocab":     parsed["vocab"]
        }
        for item in parsed["items"]:
            d = base.copy()
            d["menu_id"]      = item["menu_id"]
            d["allergy_info"] = normalize_allergy_info(item["allergies"])
            results.append(d)
        return results

    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            vocab_set = set()
            for item in batch:
                for mid, name in item.get('vocab', {}).items():
                    vocab_set.add((mid, name))
            if vocab_set:
                conn.executemany(
                    "INSERT OR IGNORE INTO vocab_meal (menu_id, menu_name) VALUES (?, ?)",
                    list(vocab_set)
                )
            meal_data = [
                (
                    item['school_id'], item['meal_date'], item['meal_type'],
                    item['menu_id'],   item['allergy_info'],
                    item['cal_info'],  item['ntr_info'], item['load_dt']
                )
                for item in batch
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO meal VALUES (?,?,?,?,?,?,?,?)",
                meal_data
            )

    # --------------------------------------------------------
    # 하루 수집 (매일): 오늘 + 내일
    # --------------------------------------------------------
    def fetch_daily(self, region: str, target_date: str):
        """오늘 + 내일 수집"""
        d = date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:]))
        tomorrow_str = (d + timedelta(days=1)).strftime("%Y%m%d")

        self._fetch_date_range(region, target_date, target_date, max_page=50)
        self.logger.info(f"[{region}] {target_date} 오늘 수집 완료")

        self._fetch_date_range(region, tomorrow_str, tomorrow_str, max_page=50)
        self.logger.info(f"[{region}] {tomorrow_str} 내일 수집 완료")

    # --------------------------------------------------------
    # 공통 diff 수집: 이번 달 + 다음 달
    # --------------------------------------------------------
    def _collect_two_months_diff(self, region: str, year: int, m: int, label: str):
        """이번 달 + 다음 달 diff 수집 공통 로직"""
        for y, mo in [(year, m), _next_month(year, m)]:
            month_str = f"{y}{mo:02d}"
            date_from = f"{month_str}01"
            date_to   = _month_last_day(y, mo).strftime("%Y%m%d")

            existing = self._load_existing_month(region, date_from, date_to)
            self.logger.info(f"[{region}][{label}] {month_str} 기존 DB: {len(existing)}건")

            fetched = self._fetch_all_month(region, date_from, date_to)
            self.logger.info(f"[{region}][{label}] {month_str} API 수집: {len(fetched)}건")

            def _cmp_key(d: dict):
                return (d['allergy_info'], d['cal_info'], d['ntr_info'])

            existing_keys = set(existing.keys())
            fetched_keys  = set(fetched.keys())

            to_insert = fetched_keys - existing_keys
            to_delete = existing_keys - fetched_keys
            to_update = {
                k for k in existing_keys & fetched_keys
                if _cmp_key(existing[k]) != _cmp_key(fetched[k])
            }

            self.logger.info(
                f"[{region}][{label}] {month_str} diff → "
                f"신규:{len(to_insert)} 변경:{len(to_update)} 삭제:{len(to_delete)}"
            )

            # 저장
            if to_insert or to_update:
                self._save_batch([fetched[k] for k in to_insert | to_update])

            # 🛡️ 삭제 안전장치: API 응답이 기존 DB의 30% 미만이면 삭제 중단
            if to_delete:
                if len(fetched) < len(existing) * 0.3 and len(existing) > 50:
                    self.logger.warning(
                        f"[{region}][{label}] {month_str} ⚠️ API 응답 이상 "
                        f"(기존:{len(existing)} API:{len(fetched)}) → 삭제 건너뜀"
                    )
                else:
                    self._delete_batch(to_delete)

            self.logger.info(f"[{region}][{label}] {month_str} 완료")

    def fetch_monthly_incremental(self, region: str, month: str):
        """매주 월요일 - 이번 달 + 다음 달 diff"""
        year, m = int(month[:4]), int(month[4:])
        self._collect_two_months_diff(region, year, m, label="월요일")

    def fetch_end_of_month(self, region: str, month: str):
        """말일 -3일 - 이번 달 + 다음 달 마지막 diff"""
        year, m = int(month[:4]), int(month[4:])
        self._collect_two_months_diff(region, year, m, label="말일마감")

    # --------------------------------------------------------
    # 공통 날짜 범위 수집 (enqueue)
    # --------------------------------------------------------
    def _fetch_date_range(self, region: str, date_from: str, date_to: str, max_page: int = 200):
        """날짜 범위 수집 → enqueue"""
        p_idx = 1
        consecutive_errors = 0
        single = (date_from == date_to)

        while p_idx <= max_page:
            params = {
                "KEY":  NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize":  1000,
                "ATPT_OFCDC_SC_CODE": region,
            }
            if single:
                params["MLSV_YMD"] = date_from
            else:
                params["MLSV_FROM_YMD"] = date_from
                params["MLSV_TO_YMD"]   = date_to

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
                    if not sc_code or not should_include(self.shard, sc_code):
                        continue
                    batch.extend(self._process_item(r))

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

    # --------------------------------------------------------
    # 증분용 DB 로드 (900개 청크)
    # --------------------------------------------------------
    def _load_existing_month(self, region: str, date_from: str, date_to: str) -> Dict:
        """DB에서 해당 월 해당 지역 데이터 로드 (SQLite 999 파라미터 제한 대응)"""
        result = {}
        try:
            atpt_school_ids = [
                info['school_id']
                for info in self.school_cache.values()
                if info['atpt_code'] == region
            ]
            if not atpt_school_ids:
                return result

            with get_db_connection(self.db_path) as conn:
                chunk_size = 900
                for i in range(0, len(atpt_school_ids), chunk_size):
                    chunk = atpt_school_ids[i:i + chunk_size]
                    placeholders = ",".join("?" * len(chunk))
                    cur = conn.execute(f"""
                        SELECT school_id, meal_date, meal_type, menu_id,
                               allergy_info, cal_info, ntr_info
                        FROM meal
                        WHERE meal_date BETWEEN ? AND ?
                          AND school_id IN ({placeholders})
                    """, (int(date_from), int(date_to), *chunk))

                    for row in cur:
                        key = (row[0], row[1], row[2], row[3])
                        result[key] = {
                            "school_id":    row[0],
                            "meal_date":    row[1],
                            "meal_type":    row[2],
                            "menu_id":      row[3],
                            "allergy_info": row[4],
                            "cal_info":     row[5],
                            "ntr_info":     row[6],
                            "load_dt":      now_kst().isoformat(),
                            "vocab":        {}
                        }
        except Exception as e:
            self.logger.error(f"기존 데이터 로드 실패: {e}")
        return result

    def _fetch_all_month(self, region: str, date_from: str, date_to: str) -> Dict:
        """API에서 한 달치 전체 수집 → dict (큐 사용 안 함, diff용)"""
        result = {}
        p_idx = 1
        consecutive_errors = 0
        while p_idx <= 200:
            params = {
                "KEY":  NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize":  1000,
                "ATPT_OFCDC_SC_CODE": region,
                "MLSV_FROM_YMD": date_from,
                "MLSV_TO_YMD":   date_to,
            }
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "mealServiceDietInfo" not in res:
                    break
                rows = res["mealServiceDietInfo"][1].get("row", [])
                if not rows:
                    break

                for r in rows:
                    sc_code = r.get('SD_SCHUL_CODE')
                    if not sc_code or not should_include(self.shard, sc_code):
                        continue
                    for item in self._process_item(r):
                        key = (item['school_id'], item['meal_date'],
                               item['meal_type'],  item['menu_id'])
                        result[key] = item

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
        return result

    def _delete_batch(self, keys: Set[Tuple]):
        """삭제된 급식 데이터 제거"""
        try:
            with get_db_connection(self.db_path) as conn:
                conn.executemany("""
                    DELETE FROM meal
                    WHERE school_id=? AND meal_date=? AND meal_type=? AND menu_id=?
                """, list(keys))
            self.logger.info(f"🗑️ {len(keys)}건 삭제 완료")
        except Exception as e:
            self.logger.error(f"삭제 실패: {e}")

    def close(self):
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="급식 수집기")
    parser.add_argument("--regions", required=True, help="B10,C10,... 또는 ALL")
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"])
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--full", action="store_true")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date",     help="하루 수집 YYYYMMDD (매일)")
    group.add_argument("--month",    help="월간 증분 수집 YYYYMM (매주 월요일)")
    group.add_argument("--endmonth", help="말일 마지막 수집 YYYYMM (말일 -3일)")

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
        if args.month:
            collector.fetch_monthly_incremental(region, args.month)
        elif args.endmonth:
            collector.fetch_end_of_month(region, args.endmonth)
        else:
            collector.fetch_daily(region, args.date)

    collector.close()
    collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
