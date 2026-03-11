#!/usr/bin/env python3
# collectors/school_info_collector.py
# 최종 수정: 모든 API 필드 저장, 좌표 포함

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
from core.network import safe_json_request, build_session
from core.config import config
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR

API_URL = "https://open.neis.go.kr/hub/schoolInfo"  # 학교알리미 API 엔드포인트 (예시, 실제 엔드포인트 확인 필요)

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
                    -- 기본 키
                    school_code TEXT PRIMARY KEY,      -- 정보공시 학교코드 (SCHUL_CODE)
                    
                    -- 시도교육청 관련
                    atpt_ofcdc_org_nm TEXT,            -- 시도교육청명
                    atpt_ofcdc_org_code TEXT,          -- 시도교육청코드
                    ju_org_nm TEXT,                     -- 교육지원청명
                    ju_org_code TEXT,                    -- 교육지원청코드
                    
                    -- 지역 정보
                    adrcd_nm TEXT,                       -- 지역명
                    adrcd_cd TEXT,                       -- 지역코드
                    lctn_sc_code TEXT,                   -- 소재지구분코드
                    
                    -- 학교 기본 정보
                    schul_nm TEXT,                        -- 학교명
                    schul_knd_sc_code TEXT,               -- 학교급코드
                    fond_sc_code TEXT,                     -- 설립구분
                    hs_knd_sc_nm TEXT,                     -- 학교특성
                    bnhh_yn TEXT,                          -- 분교여부
                    schul_fond_typ_code TEXT,              -- 설립유형
                    dght_sc_code TEXT,                      -- 주야구분
                    
                    -- 설립/개교일
                    foas_memrd TEXT,                        -- 개교기념일
                    fond_ymd TEXT,                          -- 설립일
                    
                    -- 주소/위치
                    adres_brkdn TEXT,                       -- 주소내역
                    dtlad_brkdn TEXT,                       -- 상세주소내역
                    zip_code TEXT,                          -- 우편번호
                    schul_rdnzc TEXT,                       -- 학교도로명 우편번호
                    schul_rdnma TEXT,                       -- 학교도로명 주소
                    schul_rdnda TEXT,                       -- 학교도로명 상세주소
                    lttud REAL,                              -- 위도
                    lgtud REAL,                              -- 경도
                    
                    -- 연락처
                    user_telno TEXT,                         -- 전화번호
                    user_telno_sw TEXT,                      -- 전화번호(교무실)
                    user_telno_ga TEXT,                      -- 전화번호(행정실)
                    perc_faxno TEXT,                         -- 팩스번호
                    hmpg_adres TEXT,                         -- 홈페이지 주소
                    
                    -- 기타 구분
                    coedu_sc_code TEXT,                      -- 남녀공학 구분
                    absch_yn TEXT,                           -- 폐교여부
                    absch_ymd TEXT,                           -- 폐교일자
                    close_yn TEXT,                            -- 휴교여부
                    
                    -- 각종학교용
                    schul_crse_sc_value TEXT,                 -- 학교과정구분값(2-3-4)
                    schul_crse_sc_value_nm TEXT,              -- 학교과정구분명(초-중-고)
                    
                    -- 수집 메타
                    collected_at TEXT NOT NULL,
                    updated_at TEXT,
                    is_active INTEGER DEFAULT 1,
                    in_neis INTEGER DEFAULT 0
                )
            """)
            # 인덱스 생성
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(atpt_ofcdc_org_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(schul_knd_sc_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_in_neis ON schools(in_neis)")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return "school_code"

    def fetch_region(self, region_code: str, year: Optional[int] = None, date: Optional[str] = None, **kwargs) -> int:
        """지역별 학교 정보 수집"""
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)

        if not self.quiet_mode:
            self.print(f"📡 [{region_name}] 학년도 {year} 수집 시작", level="debug")

        # API 요청 파라미터 구성 (실제 학교알리미 API에 맞게 조정 필요)
        params = {
            "ATPT_OFCDC_SC_CODE": region_code,
            "pSize": 100,
            "pIndex": 1,
            "KEY": config.get_api_key('school_info'),  # API 키 설정 필요
            "Type": "json"
        }
        rows = self._fetch_paginated(API_URL, params, "schoolInfo", region=region_code, year=year)

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return 0

        school_count = 0
        for row in rows:
            school_code = row.get("SD_SCHUL_CODE")  # 실제 API 응답 키 확인 필요
            if not school_code:
                continue
            # 샤드 필터링 (선택)
            if self.shard != "none" and not should_include_school(self.shard, self.school_range, school_code):
                continue
            self.enqueue([self._transform_row(row, region_code)])
            school_count += 1

        return school_count

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        """API 응답 row를 DB 레코드 딕셔너리로 변환"""
        now = now_kst().isoformat()
        return {
            # 기본 키
            "school_code": row.get("SCHUL_CODE", ""),
            
            # 시도교육청
            "atpt_ofcdc_org_nm": row.get("ATPT_OFCDC_ORG_NM", ""),
            "atpt_ofcdc_org_code": row.get("ATPT_OFCDC_ORG_CODE", ""),
            "ju_org_nm": row.get("JU_ORG_NM", ""),
            "ju_org_code": row.get("JU_ORG_CODE", ""),
            
            # 지역
            "adrcd_nm": row.get("ADRCD_NM", ""),
            "adrcd_cd": row.get("ADRCD_CD", ""),
            "lctn_sc_code": row.get("LCTN_SC_CODE", ""),
            
            # 학교 기본
            "schul_nm": row.get("SCHUL_NM", ""),
            "schul_knd_sc_code": row.get("SCHUL_KND_SC_CODE", ""),
            "fond_sc_code": row.get("FOND_SC_CODE", ""),
            "hs_knd_sc_nm": row.get("HS_KND_SC_NM", ""),
            "bnhh_yn": row.get("BNHH_YN", ""),
            "schul_fond_typ_code": row.get("SCHUL_FOND_TYP_CODE", ""),
            "dght_sc_code": row.get("DGHT_SC_CODE", ""),
            
            # 날짜
            "foas_memrd": row.get("FOAS_MEMRD", ""),
            "fond_ymd": row.get("FOND_YMD", ""),
            
            # 주소
            "adres_brkdn": row.get("ADRES_BRKDN", ""),
            "dtlad_brkdn": row.get("DTLAD_BRKDN", ""),
            "zip_code": row.get("ZIP_CODE", ""),
            "schul_rdnzc": row.get("SCHUL_RDNZC", ""),
            "schul_rdnma": row.get("SCHUL_RDNMA", ""),
            "schul_rdnda": row.get("SCHUL_RDNDA", ""),
            "lttud": self._parse_float(row.get("LTTUD")),
            "lgtud": self._parse_float(row.get("LGTUD")),
            
            # 연락처
            "user_telno": row.get("USER_TELNO", ""),
            "user_telno_sw": row.get("USER_TELNO_SW", ""),
            "user_telno_ga": row.get("USER_TELNO_GA", ""),
            "perc_faxno": row.get("PERC_FAXNO", ""),
            "hmpg_adres": row.get("HMPG_ADRES", ""),
            
            # 기타
            "coedu_sc_code": row.get("COEDU_SC_CODE", ""),
            "absch_yn": row.get("ABSCH_YN", ""),
            "absch_ymd": row.get("ABSCH_YMD", ""),
            "close_yn": row.get("CLOSE_YN", ""),
            
            # 각종학교
            "schul_crse_sc_value": row.get("SCHUL_CRSE_SC_VALUE", ""),
            "schul_crse_sc_value_nm": row.get("SCHUL_CRSE_SC_VALUE_NM", ""),
            
            # 메타
            "collected_at": now,
            "updated_at": now,
            "is_active": 1,
            "in_neis": 0,  # NEIS 등록 여부는 별도로 채울 수 있음
        }

    def _parse_float(self, val: Any) -> Optional[float]:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[Dict[str, Any]]) -> None:
        sql = """
            INSERT OR REPLACE INTO schools (
                school_code,
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
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        rows = []
        for r in batch:
            rows.append((
                r["school_code"],
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
        conn.executemany(sql, rows)

    # BaseCollector의 추상 메서드 구현
    def _process_item(self, raw_item: dict) -> List[dict]:
        return [self._transform_row(raw_item, "")]
        