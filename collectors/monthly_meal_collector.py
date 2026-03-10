#!/usr/bin/env python3
# collectors/monthly_meal_collector.py
# 개발 가이드: docs/developer_guide.md 참조

import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import calendar
from typing import List

from core.base_meal_collector import BaseMealCollector
from core.config import config
from core.kst_time import now_kst
from constants.codes import NEIS_ENDPOINTS
from constants.paths import ACTIVE_DIR

NEIS_URL = NEIS_ENDPOINTS['meal']


class MonthlyMealCollector(BaseMealCollector):
    # ----- 메타데이터 -----
    description = "급식 정보 (월간)"
    table_name = "meal"
    merge_script = "scripts/merge_meal_dbs.py"
    
    _cfg = config.get_collector_config("meal")  # meal 설정 공유
    timeout_seconds = _cfg.get("timeout_seconds", 1800)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 3600)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 1800)
    parallel_script = _cfg.get("parallel_script", "scripts/run_pipeline.py")
    modes = _cfg.get("modes", ["통합", "odd 샤드", "even 샤드", "병렬 실행"])
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    parallel_config = {
        "max_workers": _cfg.get("max_workers", 2),
        "cpu_factor": _cfg.get("cpu_factor", 0.8),
        "max_by_api": _cfg.get("max_by_api", 5),
        "absolute_max": _cfg.get("absolute_max", 8),
    }
    # ---------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False, **kwargs):
        super().__init__("meal", str(ACTIVE_DIR), shard, school_range, debug_mode, **kwargs)
        self.api_context = 'meal'

    def fetch_month(self, region: str, year: int, month: int):
        """특정 년-월의 1일 급식 데이터를 수집합니다."""
        if self.debug_mode:
            self.print(f"📡 [{region}] {year}년 {month}월 1일 급식 수집 시작", level="debug")

        # 1일 날짜
        day = 1
        target_date = f"{year}{month:02d}{day:02d}"
        self._fetch_date_range(region, target_date, target_date)

    def fetch_region(self, region: str, **kwargs):
        """CLI 호환을 위해 fetch_region 구현 (year, month 사용)"""
        year = kwargs.get("year")
        month = kwargs.get("month")
        if year is None or month is None:
            raise ValueError("year and month are required")
        self.fetch_month(region, year, month)

    def _fetch_date_range(self, region: str, date_from: str, date_to: str):
        """주어진 날짜 범위의 급식 데이터를 가져옵니다."""
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
            self.logger.warning(f"[{region}] {date_from} ~ {date_to} 수집 결과 없음")
            return

        batch = []
        for r in rows:
            items = self._process_item(r)
            if items:
                batch.extend(items)
        if batch:
            self.enqueue(batch)

        self.logger.info(f"[{region}] {date_from}~{date_to} → {len(rows)}건")


if __name__ == "__main__":
    from core.collector_cli import run_collector

    def _add_month(parser):
        parser.add_argument("--month", type=int, required=True, help="수집할 월 (1-12)")

    def _fetch(collector, region, **kwargs):
        collector.fetch_month(region, kwargs['year'], kwargs['month'])

    run_collector(
        MonthlyMealCollector,
        _fetch,
        "월간 급식 수집기 (매월 1일)",
        extra_args_fn=_add_month,
    )
    