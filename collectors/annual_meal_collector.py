#!/usr/bin/env python3
"""
급식 정보 수집기 - 학년도 전체 버전
"""
import calendar
from typing import List

from core.base_meal_collector import BaseMealCollector
from constants.codes import NEIS_ENDPOINTS
from constants.paths import ACTIVE_DIR

NEIS_URL = NEIS_ENDPOINTS['meal']


class AnnualMealCollector(BaseMealCollector):
    def __init__(self, shard="none", school_range=None, debug_mode=False):
        super().__init__("meal", str(ACTIVE_DIR), shard, school_range, debug_mode)

    def fetch_year(self, region: str, year: int):
        month_range = [(year, m) for m in range(3, 13)] + [(year+1, m) for m in range(1, 3)]
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
        rows = self._fetch_paginated(
            NEIS_URL, base_params, 'mealServiceDietInfo', page_size=100,
            region=region,   # ✅ 이렇게 함수 인자로 포함
            year=y           # ✅
        )
        for r in rows:
            items = self._process_item(r)
            if items:
                self.enqueue(items)


if __name__ == "__main__":
    from core.collector_cli import run_collector

    def _fetch(collector, region, **kwargs):
        collector.fetch_year(region, kwargs['year'])

    run_collector(
        AnnualMealCollector,
        _fetch,
        "급식 연간 수집기",
    )
    