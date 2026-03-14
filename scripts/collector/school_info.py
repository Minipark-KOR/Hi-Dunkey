#!/usr/bin/env python3
# scripts/collector/school_info.py
# 학교알리미 학교 기본정보 원시 수집기

import sys
from pathlib import Path
from typing import Dict, Optional, Any

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

from core.engine.entry_collector import CollectorEngine
from core.engine.shard import should_include_school
from core.kst_time import now_kst
from core.school.year import get_current_school_year
from core.config import config
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR
from constants.map_apis import get_api_field
from constants.collector_names import SCHOOL_INFO

API_URL = "http://www.schoolinfo.go.kr/openApi.do"


class SchoolInfoCollector(CollectorEngine):
    collector_name = SCHOOL_INFO
    description = "학교 기본정보 (학교알리미)"
    table_name = "schools_info"
    merge_script = "scripts/merge_school_info_dbs.py"
    _cfg = config.get_collector_config(collector_name)
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    schema_name = "school_info"
    api_context = "school_info"

    def __init__(self, shard="none", school_range=None, debug_mode=False,
                 quiet_mode=False, **kwargs):
        super().__init__(self.collector_name, str(MASTER_DIR), shard, school_range,
                         quiet_mode=quiet_mode)
        self.debug_mode = debug_mode

    def fetch_region(self, region_code: str, year: Optional[int] = None,
                     date: Optional[str] = None, limit: Optional[int] = None, **kwargs) -> int:
        if year is None:
            year = get_current_school_year(now_kst())

        region_name = REGION_NAMES.get(region_code, region_code)
        self.print(f"📡 [{region_name}] 학년도 {year} 수집 시작", level="debug")

        params = {
            "KEY": config.get_api_key("school_info"),
            "apiType": "json",
            "pbanYr": str(year),
            "sidoCode": region_code,
        }

        try:
            rows = self._fetch_paginated(
                API_URL, params, "schoolInfo",
                region=region_code, year=year
            )
        except Exception as e:
            self.logger.error(f"[{region_name}] API 호출 실패: {e}")
            return 0

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return 0

        # 순차 처리: API 페이지를 순서대로 읽고, 유효 데이터만 배치 저장
        school_count = 0
        batch = []
        for row in rows:
            school_code = get_api_field(row, "school_code", self.api_context)
            if not school_code:
                continue

            if self.shard != "none" and not should_include_school(
                self.shard, self.school_range, school_code
            ):
                continue

            batch.append(self._transform_row(row, region_code))
            school_count += 1

            if limit and school_count >= limit:
                break

        if batch:
            self.enqueue(batch)

        return school_count

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        now = now_kst().isoformat()
        return {
            "school_code": get_api_field(row, "school_code", self.api_context, ""),
            "school_name": get_api_field(row, "school_name", self.api_context, ""),
            "region_code": region_code,
            "atpt_ofcdc_org_nm": get_api_field(row, "atpt_ofcdc_org_nm", self.api_context, ""),
            "atpt_ofcdc_org_code": get_api_field(row, "atpt_ofcdc_org_code", self.api_context, ""),
            "ju_org_nm": get_api_field(row, "ju_org_nm", self.api_context, ""),
            "ju_org_code": get_api_field(row, "ju_org_code", self.api_context, ""),
            "adrcd_nm": get_api_field(row, "adrcd_nm", self.api_context, ""),
            "adrcd_cd": get_api_field(row, "adrcd_cd", self.api_context, ""),
            "lctn_sc_code": get_api_field(row, "lctn_sc_code", self.api_context, ""),
            "schul_nm": get_api_field(row, "schul_nm", self.api_context, ""),
            "schul_knd_sc_code": get_api_field(row, "schul_knd_sc_code", self.api_context, ""),
            "fond_sc_code": get_api_field(row, "fond_sc_code", self.api_context, ""),
            "hs_knd_sc_nm": get_api_field(row, "hs_knd_sc_nm", self.api_context, ""),
            "bnhh_yn": get_api_field(row, "bnhh_yn", self.api_context, ""),
            "schul_fond_typ_code": get_api_field(row, "schul_fond_typ_code", self.api_context, ""),
            "dght_sc_code": get_api_field(row, "dght_sc_code", self.api_context, ""),
            "foas_memrd": get_api_field(row, "foas_memrd", self.api_context, ""),
            "fond_ymd": get_api_field(row, "fond_ymd", self.api_context, ""),
            "adres_brkdn": get_api_field(row, "adres_brkdn", self.api_context, ""),
            "dtlad_brkdn": get_api_field(row, "dtlad_brkdn", self.api_context, ""),
            "zip_code": get_api_field(row, "zip_code", self.api_context, ""),
            "schul_rdnzc": get_api_field(row, "schul_rdnzc", self.api_context, ""),
            "schul_rdnma": get_api_field(row, "schul_rdnma", self.api_context, ""),
            "schul_rdnda": get_api_field(row, "schul_rdnda", self.api_context, ""),
            "lttud": self._parse_float(get_api_field(row, "lttud", self.api_context)),
            "lgtud": self._parse_float(get_api_field(row, "lgtud", self.api_context)),
            "user_telno": get_api_field(row, "user_telno", self.api_context, ""),
            "user_telno_sw": get_api_field(row, "user_telno_sw", self.api_context, ""),
            "user_telno_ga": get_api_field(row, "user_telno_ga", self.api_context, ""),
            "perc_faxno": get_api_field(row, "perc_faxno", self.api_context, ""),
            "hmpg_adres": get_api_field(row, "hmpg_adres", self.api_context, ""),
            "coedu_sc_code": get_api_field(row, "coedu_sc_code", self.api_context, ""),
            "absch_yn": get_api_field(row, "absch_yn", self.api_context, ""),
            "absch_ymd": get_api_field(row, "absch_ymd", self.api_context, ""),
            "close_yn": get_api_field(row, "close_yn", self.api_context, ""),
            "schul_crse_sc_value": get_api_field(row, "schul_crse_sc_value", self.api_context, ""),
            "schul_crse_sc_value_nm": get_api_field(row, "schul_crse_sc_value_nm", self.api_context, ""),
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


if __name__ == "__main__":
    from collector_cli import run_collector_cli

    run_collector_cli(
        name="school_info",
        regions="ALL",
    )
