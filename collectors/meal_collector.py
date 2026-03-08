#!/usr/bin/env python3
# 개발 가이드: docs/developer_guide.md 참조
"""
급식 정보 수집기 - 일별 수집 버전
"""
import random
import time
from datetime import date, timedelta

from core.base_meal_collector import BaseMealCollector
from constants.codes import NEIS_ENDPOINTS
from constants.paths import ACTIVE_DIR

BASE_DIR = str(ACTIVE_DIR)
NEIS_URL = NEIS_ENDPOINTS['meal']


class MealCollector(BaseMealCollector):
    # ----- 메타데이터 -----
    description = "급식 정보 (NEIS)"
    table_name = "meal"
    merge_script = "scripts/merge_meal_dbs.py"
    timeout_seconds = 1800
    parallel_timeout_seconds = 3600
    merge_timeout_seconds = 1800
    metrics_config = {"enabled": True}
    parallel_config = {"max_workers": 2, "cpu_factor": 0.8, "max_by_api": 5, "absolute_max": 8}
    # ---------------------

    def __init__(self, shard: str = "none", school_range=None,
                 incremental: bool = False, full: bool = False, debug_mode: bool = False):
        super().__init__("meal", BASE_DIR, shard, school_range, debug_mode)
        self.incremental = incremental
        self.full = full

    def fetch_daily(self, region: str, target_date: str):
        d = date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:]))
        tomorrow = (d + timedelta(days=1)).strftime("%Y%m%d")
        self._fetch_date_range(region, target_date, target_date)
        self._fetch_date_range(region, tomorrow, tomorrow)

    def _fetch_date_range(self, region: str, date_from: str, date_to: str):
        params = {
            "ATPT_OFCDC_SC_CODE": region,
            "MLSV_FROM_YMD": date_from,
            "MLSV_TO_YMD": date_to,
        }
        rows = self._fetch_paginated(
            NEIS_URL, params, 'mealServiceDietInfo', page_size=1000,
            region=region,
            year=int(date_from[:4])
        )
        if not rows:
            self.logger.warning(f"[{region}] {date_from} 수집 결과 없음")
            return

        batch = []
        for r in rows:
            items = self._process_item(r)
            if items:
                batch.extend(items)
        if batch:
            self.enqueue(batch)

        self.logger.info(f"[{region}] {date_from}~{date_to} → {len(rows)}건")
        time.sleep(random.uniform(0.1, 0.3))


if __name__ == "__main__":
    from core.collector_cli import run_collector

    def _fetch_daily(collector, region, **kwargs):
        collector.fetch_daily(region, kwargs['date'])

    run_collector(
        MealCollector,
        _fetch_daily,
        "일별 급식 수집기",
    )
    