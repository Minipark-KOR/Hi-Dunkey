#!/usr/bin/env python3
# collectors/school_info_collector.py
# 최종 수정: 모든 API 필드 저장, NOT NULL 컬럼(school_name, region_code) 추가
# api_mappings 활용, school_code 키 오류 수정, 중복 저장 로직 제거

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
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR
from constants.api_mappings import get_api_field   # ✅ 매핑 함수 임포트

API_URL = "https://open.neis.go.kr/hub/schoolInfo"  # 학교알리미 API 엔드포인트

class SchoolInfoCollector(BaseCollector):
    description = "학교 기본정보 (학교알리미)"
    table_name = "schools"
    merge_script = "scripts/merge_school_info_dbs.py"
    _cfg = config.get_collector_config("school_info")
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    parallel_config = _cfg.get("parallel_config", {"max_workers": 4})

    def __init__(self, shard="none", school_range=None, debug_mode=False, quiet_mode=False, **kwargs):
        super().__init__("school_info", str(MASTER_DIR), shard, school_range, quiet_mode=quiet_mode)
        self.debug_mode = debug_mode
        self.quiet_mode = quiet_mode
        self._init_db()

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    school_code TEXT PRIMARY KEY,
                    school_name TEXT NOT NULL,
                    region_code TEXT NOT NULL,
                    atpt_ofcdc_org_nm TEXT,
                    atpt_ofcdc_org_code TEXT,
                    ju_org_nm TEXT,
                    ju_org_code TEXT,
                    adrcd_nm TEXT,
                    adrcd_cd TEXT,
                    lctn_sc_code TEXT,
                    schul_nm TEXT,
                    schul_knd_sc_code TEXT,
                    fond_sc_code TEXT,
                    hs_knd_sc_nm TEXT,
                    bnhh_yn TEXT,
                    schul_fond_typ_code TEXT,
                    dght_sc_code TEXT,
                    foas_memrd TEXT,
                    fond_ymd TEXT,
                    adres_brkdn TEXT,
                    dtlad_brkdn TEXT,
                    zip_code TEXT,
                    schul_rdnzc TEXT,
                    schul_rdnma TEXT,
                    schul_rdnda TEXT,
                    lttud REAL,
                    lgtud REAL,
                    user_telno TEXT,
                    user_telno_sw TEXT,
                    user_telno_ga TEXT,
                    perc_faxno TEXT,
                    hmpg_adres TEXT,
                    coedu_sc_code TEXT,
                    absch_yn TEXT,
                    absch_ymd TEXT,
                    close_yn TEXT,
                    schul_crse_sc_value TEXT,
                    schul_crse_sc_value_nm TEXT,
                    collected_at TEXT NOT NULL,
                    updated_at TEXT,
                    is_active INTEGER DEFAULT 1,
                    in_neis INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(atpt_ofcdc_org_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(schul_knd_sc_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_in_neis ON schools(in_neis)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return "school_code"

    def fetch_region(self, region_code: str, year: Optional[int] = None, date: Optional[str] = None, **kwargs) -> int:
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
        rows = self._fetch_paginated(API_URL, params, "schoolInfo", region=region_code, year=year)

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return 0

        school_count = 0
        for row in rows:
            school_code = get_api_field(row, "school_code", "school_info")
            if not school_code:
                continue
            if self.shard != "none" and not should_include_school(self.shard, self.school_range, school_code):
                continue
            self.enqueue([self._transform_row(row, region_code)])
            school_count += 1

        return school_count

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        now = now_kst().isoformat()
        # 매핑된 모든 필드를 한 번에 가져오는 방법 (선택)
        # 여기서는 모든 필드를 개별적으로 매핑
        result = {
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
        return result

    def _parse_float(self, val: Any) -> Optional[float]:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[Dict[str, Any]]) -> None:
        placeholders = ', '.join(['?'] * 42)
        sql = f"""
            INSERT OR REPLACE INTO schools (
                school_code,
                school_name, region_code,
                atpt_ofcdc_org_nm, atpt_ofcdc_org_code, ju_org_nm, ju_org_code,
                adrcd_nm, adrcd_cd, lctn_sc_code,
                schul_nm, schul_knd_sc_code, fond_sc_code, hs_knd_sc_nm,
                bnhh_yn, schul_fond_typ_code, dght_sc_code,
                foas_memrd, fond_ymd,
                adres_brkdn, dtlad_brkdn, zip_code, schul_rdnzc, schul_rdnma, schul_rdnda,
                lttud, lgtud,
                user_telno, user_telno_sw, user_telno_ga, perc_faxno, hmpg_adres,
                coedu_sc_code, absch_yn, absch_ymd, close_yn,
                schul_crse_sc_value, schul_crse_sc_value_nm,
                collected_at, updated_at, is_active, in_neis
            ) VALUES ({placeholders})
        """
        rows = []
        for r in batch:
            rows.append((
                r["school_code"],
                r["school_name"],
                r["region_code"],
                r["atpt_ofcdc_org_nm"], r["atpt_ofcdc_org_code"], r["ju_org_nm"], r["ju_org_code"],
                r["adrcd_nm"], r["adrcd_cd"], r["lctn_sc_code"],
                r["schul_nm"], r["schul_knd_sc_code"], r["fond_sc_code"], r["hs_knd_sc_nm"],
                r["bnhh_yn"], r["schul_fond_typ_code"], r["dght_sc_code"],
                r["foas_memrd"], r["fond_ymd"],
                r["adres_brkdn"], r["dtlad_brkdn"], r["zip_code"], r["schul_rdnzc"], r["schul_rdnma"], r["schul_rdnda"],
                r["lttud"], r["lgtud"],
                r["user_telno"], r["user_telno_sw"], r["user_telno_ga"], r["perc_faxno"], r["hmpg_adres"],
                r["coedu_sc_code"], r["absch_yn"], r["absch_ymd"], r["close_yn"],
                r["schul_crse_sc_value"], r["schul_crse_sc_value_nm"],
                r["collected_at"], r["updated_at"], r["is_active"], r["in_neis"]
            ))

        print(f"\n🔥 [SAVE] 배치 크기: {len(batch)}")
        if rows:
            print(f"🔥 첫 행 샘플: {rows[0][:5]}...")

        try:
            cursor = conn.cursor()
            cursor.executemany(sql, rows)
            affected = cursor.rowcount
            print(f"🔥 executemany 후 rowcount: {affected}")
            conn.commit()
            count = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
            print(f"🔥 저장 후 총 레코드 수: {count}")
        except Exception as e:
            print(f"🔥 저장 실패: {e}")
            raise

        print("✅ [_save_batch] 저장 완료 (디버그용)")

    def _process_item(self, raw_item: dict) -> List[dict]:
        # 이 메서드는 BaseCollector의 추상 메서드로, 단일 아이템 처리를 위해 사용됩니다.
        # fetch_region에서 직접 enqueue하므로 여기서는 사용되지 않지만, 구현해야 함.
        return [self._transform_row(raw_item, "")]
        