#!/usr/bin/env python3
# collectors/annual_meal_collector.py
# 개발 가이드: docs/developer_guide.md 참조

import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import calendar
from typing import List

from core.base_meal_collector import BaseMealCollector
from core.config import config
from constants.codes import NEIS_ENDPOINTS
from constants.paths import ACTIVE_DIR

NEIS_URL = NEIS_ENDPOINTS['meal']


class AnnualMealCollector(BaseMealCollector):
    # ----- 메타데이터 (선택 사항, 필요시 config 사용) -----
    description = "급식 정보 (연간)"
    table_name = "meal"
    merge_script = "scripts/merge_meal_dbs.py"
    
    _cfg = config.get_collector_config("meal")  # meal 설정 공유
    timeout_seconds = _cfg.get("timeout_seconds", 1800)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 3600)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 1800)
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    parallel_config = {
        "max_workers": _cfg.get("max_workers", 2),
        "cpu_factor": _cfg.get("cpu_factor", 0.8),
        "max_by_api": _cfg.get("max_by_api", 5),
        "absolute_max": _cfg.get("absolute_max", 8),
    }
    # ---------------------

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
            region=region,
            year=y
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
    