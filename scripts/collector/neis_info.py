#!/usr/bin/env python3
# scripts/collector/neis_info.py
# NEIS 학교 기본정보 원시 데이터 수집기
# 최종 수정: 2026-03-14
# - 순차처리 전용 (병렬/머지 관련 메타데이터 제거)
# - 원시 데이터만 저장 (보강 필드는 None)

import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

from core.engine.entry_collector import CollectorEngine
from core.kst_time import now_kst
from core.school.year import get_current_school_year
from core.config import config
from core.engine.shard import should_include_school
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS, REGION_NAMES
from constants.paths import MASTER_DIR
from constants.map_apis import get_api_field
from constants.collector_names import NEIS_INFO

API_URL = NEIS_ENDPOINTS['school']


class NeisInfoCollector(CollectorEngine):
    """NEIS 학교 기본정보 수집기 (원시 데이터, 순차처리)"""

    collector_name = NEIS_INFO
    description = "NEIS 학교 기본정보 (원시 데이터)"
    table_name = "schools_neis"
    schema_name = "neis_info"
    api_context = "school"

    # 설정 로딩 (config.yaml의 collectors.neis_info 섹션)
    _cfg = config.get_collector_config(collector_name)
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    
    # 통계 수집 활성화 (선택)
    metrics_config = _cfg.get("metrics_config", {"enabled": True})

    def __init__(self, shard="none", school_range=None, debug_mode=False, quiet_mode=False, **kwargs):
        super().__init__(self.collector_name, str(MASTER_DIR), shard, school_range,
                         quiet_mode=quiet_mode, debug_mode=debug_mode)
        self.run_date = now_kst().strftime("%Y%m%d")
        if not quiet_mode:
            self.print(f"🏫 NeisInfoCollector (원시) 초기화 완료 (샤드: {shard})")
        self.logger.info(f"NeisInfoCollector (원시) 초기화 완료 (샤드: {shard})")

    def fetch_region(self, region_code: str, year: int = None, limit: int = None, **kwargs):
        """특정 지역의 학교 정보를 수집하여 저장합니다."""
        region_name = REGION_NAMES.get(region_code, region_code)
        if year is None:
            year = get_current_school_year(now_kst())

        self.print(f"📡 [{region_name}({region_code})] 학년도 {year} 수집 시작...", level="debug")

        base_params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(
            API_URL, base_params, 'schoolInfo',
            page_size=100,
            region=region_code,
            year=year
        )

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return

        self.logger.info(f"[{region_name}] 전체 {len(rows)}건 수집")

        # 유효한 학교만 필터링하여 배치 구성
        batch = []
        for row in rows:
            sc_code = get_api_field(row, "school_code", self.api_context, "")
            if not sc_code or not should_include_school(self.shard, self.school_range, sc_code):
                continue

            batch.append(self._transform_row(row, region_code))
            if limit and len(batch) >= limit:
                break

        if batch:
            self.enqueue(batch)   # 한 번에 enqueue
            self.logger.info(f"[{region_name}] {len(batch)}건 enqueue 완료")

    def _transform_row(self, row: dict, region_code: str) -> dict:
        """API 응답 row를 DB 레코드로 변환 (보강 필드는 None)"""
        now = now_kst().isoformat()
        return {
            # API 응답에서 직접 가져오는 필드 (strip 처리)
            "sc_code": get_api_field(row, "school_code", self.api_context, ""),
            "sc_name": get_api_field(row, "school_name", self.api_context, ""),
            "eng_name": get_api_field(row, "eng_name", self.api_context, ""),
            "sc_kind": get_api_field(row, "school_kind", self.api_context, ""),
            "atpt_code": region_code,   # API 응답과 동일함이 보장됨
            "address": get_api_field(row, "address", self.api_context, ""),
            "tel": get_api_field(row, "phone", self.api_context, ""),
            "homepage": get_api_field(row, "homepage", self.api_context, ""),
            "atpt_ofcdc_sc_nm": row.get("ATPT_OFCDC_SC_NM", "").strip(),
            "lctn_sc_nm": row.get("LCTN_SC_NM", "").strip(),
            "ju_org_nm": row.get("JU_ORG_NM", "").strip(),
            "fond_sc_nm": row.get("FOND_SC_NM", "").strip(),
            "org_rdnzc": row.get("ORG_RDNZC", "").strip(),
            "org_rdnda": row.get("ORG_RDNDA", "").strip(),
            "org_faxno": row.get("ORG_FAXNO", "").strip(),
            "coedu_sc_nm": row.get("COEDU_SC_NM", "").strip(),
            "hs_sc_nm": row.get("HS_SC_NM", "").strip(),
            "indst_specl_ccccl_exst_yn": row.get("INDST_SPECL_CCCCL_EXST_YN", "").strip(),
            "hs_gnrl_busns_sc_nm": row.get("HS_GNRL_BUSNS_SC_NM", "").strip(),
            "spcly_purps_hs_ord_nm": row.get("SPCLY_PURPS_HS_ORD_NM", "").strip(),
            "ene_bfe_sehf_sc_nm": row.get("ENE_BFE_SEHF_SC_NM", "").strip(),
            "dght_sc_nm": row.get("DGHT_SC_NM", "").strip(),
            "fond_ymd": row.get("FOND_YMD", "").strip(),
            "foas_memrd": row.get("FOAS_MEMRD", "").strip(),
            "load_dtm": row.get("LOAD_DTM", "").strip(),

            # 수집 시점 정보
            "last_seen": int(self.run_date),
            "load_dt": now,

            # 보강 필드 (모두 None 또는 기본값)
            "school_id": None,
            "cleaned_address": None,
            "address_hash": None,
            "latitude": None,
            "longitude": None,
            "geocode_attempts": 0,
            "last_error": None,
            "city_id": 0,
            "district_id": 0,
            "street_id": 0,
            "number_type": None,
            "number_value": None,
            "number_start": None,
            "number_end": None,
            "number_bit": 0,
            "kakao_address": None,
            "jibun_address": None,
            "status": "운영",
        }


if __name__ == "__main__":
    from collector_cli import run_collector_cli

    run_collector_cli(
        name="neis_info",
        regions="ALL",
    )
    