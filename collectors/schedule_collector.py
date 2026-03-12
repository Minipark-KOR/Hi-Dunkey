#!/usr/bin/env python3
# collectors/school_info_collector.py
# 최종 수정: _init_db 제거, _process_item 제거, fetch_region에서 변환 후 enqueue

import os
import sys
import time
import sqlite3
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Any

sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.config import config
from constants.codes import REGION_NAMES, ALL_REGIONS
from constants.paths import MASTER_DIR
from constants.api_mappings import get_api_field

API_URL = "https://open.neis.go.kr/hub/schoolInfo"


class SchoolInfoCollector(BaseCollector):
    description = "학교 기본정보 (학교알리미)"
    table_name = "schools_info"
    merge_script = "scripts/merge_school_info_dbs.py"
    _cfg = config.get_collector_config("school_info")

    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)

    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    parallel_config = _cfg.get("parallel_config", {"max_workers": 4})

    schema_name = "school_info"

    def __init__(self, shard="none", school_range=None, debug_mode=False,
                 quiet_mode=False, **kwargs):
        super().__init__("school_info", str(MASTER_DIR), shard, school_range,
                         quiet_mode=quiet_mode)
        self.debug_mode = debug_mode
        self.quiet_mode = quiet_mode

    def _get_target_key(self) -> str:
        return "school_code"

    def fetch_region(self, region_code: str, year: Optional[int] = None,
                     date: Optional[str] = None, limit: Optional[int] = None, **kwargs) -> int:
        if year is None:
            year = get_current_school_year(now_kst())

        region_name = REGION_NAMES.get(region_code, region_code)

        if not self.quiet_mode:
            self.print(f"📡 [{region_name}] 학년도 {year} 수집 시작", level="debug")

        params = {
            "ATPT_OFCDC_SC_CODE": region_code,
            "pSize": 100,
            "pIndex": 1,
            "KEY": config.get_api_key('school_info'),
            "Type": "json"
        }

        rows = self._fetch_paginated(
            API_URL, params, "schoolInfo",
            region=region_code, year=year
        )

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return 0

        school_count = 0
        for row in rows:
            school_code = get_api_field(row, "school_code", "school_info")
            if not school_code:
                continue

            if self.shard != "none" and not should_include_school(
                self.shard, self.school_range, school_code
            ):
                continue

            # 변환된 dict enqueue
            self.enqueue([self._transform_row(row, region_code)])
            school_count += 1

            if limit and school_count >= limit:
                break

        return school_count

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        now = now_kst().isoformat()
        return {
            "school_code": get_api_field(row, "school_code", "school_info", ""),
            "school_name": get_api_field(row, "school_name", "school_info", ""),
            "region_code": region_code,
            "atpt_ofcdc_org_nm": get_api_field(row, "atpt_ofcdc_org_nm", "school_info", ""),
            "atpt_ofcdc_org_code": get_api_field(row, "atpt_ofcdc_org_code", "school_info", ""),
            "ju_org_nm": get_api_field(row, "ju_org_nm", "school_info", ""),
            "ju_org_code": get_api_field(row, "ju_org_code", "school_info", ""),
            "adrcd_nm": get_api_field(row, "adrcd_nm", "school_info", ""),
            "adrcd_cd": get_api_field(row, "adrcd_cd", "school_info", ""),
            "lctn_sc_code": get_api_field(row, "lctn_sc_code", "school_info", ""),
            "schul_nm": get_api_field(row, "schul_nm", "school_info", ""),
            "schul_knd_sc_code": get_api_field(row, "schul_knd_sc_code", "school_info", ""),
            "fond_sc_code": get_api_field(row, "fond_sc_code", "school_info", ""),
            "hs_knd_sc_nm": get_api_field(row, "hs_knd_sc_nm", "school_info", ""),
            "bnhh_yn": get_api_field(row, "bnhh_yn", "school_info", ""),
            "schul_fond_typ_code": get_api_field(row, "schul_fond_typ_code", "school_info", ""),
            "dght_sc_code": get_api_field(row, "dght_sc_code", "school_info", ""),
            "foas_memrd": get_api_field(row, "foas_memrd", "school_info", ""),
            "fond_ymd": get_api_field(row, "fond_ymd", "school_info", ""),
            "adres_brkdn": get_api_field(row, "adres_brkdn", "school_info", ""),
            "dtlad_brkdn": get_api_field(row, "dtlad_brkdn", "school_info", ""),
            "zip_code": get_api_field(row, "zip_code", "school_info", ""),
            "schul_rdnzc": get_api_field(row, "schul_rdnzc", "school_info", ""),
            "schul_rdnma": get_api_field(row, "schul_rdnma", "school_info", ""),
            "schul_rdnda": get_api_field(row, "schul_rdnda", "school_info", ""),
            "lttud": self._parse_float(get_api_field(row, "lttud", "school_info")),
            "lgtud": self._parse_float(get_api_field(row, "lgtud", "school_info")),
            "user_telno": get_api_field(row, "user_telno", "school_info", ""),
            "user_telno_sw": get_api_field(row, "user_telno_sw", "school_info", ""),
            "user_telno_ga": get_api_field(row, "user_telno_ga", "school_info", ""),
            "perc_faxno": get_api_field(row, "perc_faxno", "school_info", ""),
            "hmpg_adres": get_api_field(row, "hmpg_adres", "school_info", ""),
            "coedu_sc_code": get_api_field(row, "coedu_sc_code", "school_info", ""),
            "absch_yn": get_api_field(row, "absch_yn", "school_info", ""),
            "absch_ymd": get_api_field(row, "absch_ymd", "school_info", ""),
            "close_yn": get_api_field(row, "close_yn", "school_info", ""),
            "schul_crse_sc_value": get_api_field(row, "schul_crse_sc_value", "school_info", ""),
            "schul_crse_sc_value_nm": get_api_field(row, "schul_crse_sc_value_nm", "school_info", ""),
            "collected_at": now,
            "updated_at": now,
            "is_active": 1,
            "in_neis": 0,
        }

    def _parse_float(self, val: Any) -> Optional[float]:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    # ✅ _process_item 제거


if __name__ == "__main__":
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'

    parser = argparse.ArgumentParser(description="학교알리미 정보 수집기")
    parser.add_argument("--regions", default="ALL", help="수집할 지역")
    parser.add_argument("--shard", choices=["none", "odd", "even"], default="none")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if is_github_actions and not args.quiet:
        args.quiet = True

    collector = SchoolInfoCollector(
        shard=args.shard,
        debug_mode=args.debug,
        quiet_mode=args.quiet
    )

    print(f"📂 DB 경로: {collector.db_path}")

    if args.regions == "ALL":
        regions = ALL_REGIONS
    else:
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    for region in regions:
        collector.fetch_region(region, limit=args.limit)
        if args.limit:
            break

    collector.flush()
    time.sleep(2)
    collector.close()

    if os.path.exists(collector.db_path):
        count = sqlite3.connect(collector.db_path).execute(
            "SELECT COUNT(*) FROM schools_info;"
        ).fetchone()[0]
        print(f"📊 DB 저장 완료: {count}건 (파일: {collector.db_path})")
    else:
        print(f"❌ DB 파일 없음: {collector.db_path}")
        