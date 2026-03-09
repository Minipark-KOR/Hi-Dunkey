#!/usr/bin/env python3
# collectors/school_info_collector.py
# 개발 가이드: docs/developer_guide.md 참조

import os
import sys
from pathlib import Path
import sqlite3
from typing import Union, Optional, List, Dict, Any

sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.network import safe_json_request, build_session
from core.config import config
from core.neis_validator import neis_validator
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR

API_URL = "https://open.neis.go.kr/hub/schoolInfo"


class SchoolInfoCollector(BaseCollector):
    # ----- 메타데이터 -----
    description = "학교 기본정보 (학교알리미)"
    table_name = "schools"
    merge_script = "scripts/merge_school_info_dbs.py"
    
    _cfg = config.get_collector_config("school_info")
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)
    metrics_config = _cfg.get("metrics_config", {"enabled": True, "collect_geo": True, "collect_global": True})
    parallel_config = {
        "max_workers": _cfg.get("max_workers", 4),
        "cpu_factor": _cfg.get("cpu_factor", 1.0),
        "max_by_api": _cfg.get("max_by_api", 10),
        "absolute_max": _cfg.get("absolute_max", 16),
    }
    # ---------------------

    def __init__(self, shard: str = "none", school_range: Optional[str] = None, debug_mode: bool = False):
        super().__init__("school_info", str(MASTER_DIR), shard, school_range)
        self.debug_mode = debug_mode
        
        # NEIS validator 상태 확인
        neis_count = len(neis_validator.get_all())
        self.logger.info(f"NEIS validator 로드 완료: {neis_count}개 학교 코드")
        
        self._init_db()

    def _init_db(self) -> None:
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    school_code TEXT PRIMARY KEY,
                    school_name TEXT NOT NULL,
                    region_code TEXT NOT NULL,
                    region_name TEXT,
                    school_type TEXT,
                    school_type_name TEXT,
                    address TEXT,
                    zip_code TEXT,
                    phone TEXT,
                    homepage TEXT,
                    establishment_date TEXT,
                    open_date TEXT,
                    close_date TEXT,
                    latitude REAL,
                    longitude REAL,
                    collected_at TEXT NOT NULL,
                    updated_at TEXT,
                    is_active INTEGER DEFAULT 1,
                    in_neis INTEGER DEFAULT 0   -- NEIS 등록 여부 (1: 등록됨, 0: 미등록)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(region_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(school_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_in_neis ON schools(in_neis)")

    def fetch_region(
        self, 
        region_code: str, 
        year: Optional[int] = None, 
        date: Optional[str] = None
    ) -> None:
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)

        if self.debug_mode:
            print(f"📡 [{region_name}] 학년도 {year} 수집 시작 (샤드: {self.shard})")

        params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(API_URL, params, "schoolInfo", region=region_code, year=year)

        if not rows:
            self.logger.warning(f"[{region_name}] 수집된 데이터 없음")
            return

        for row in rows:
            school_code = row.get("SD_SCHUL_CODE")
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue

            self.enqueue([self._transform_row(row, region_code)])

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        now = now_kst().isoformat()
        school_code = row.get("SD_SCHUL_CODE")
        return {
            "school_code": school_code,
            "school_name": row.get("SCHUL_NM", ""),
            "region_code": region_code,
            "region_name": REGION_NAMES.get(region_code, ""),
            "school_type": row.get("LCLAS_SC_CODE"),
            "school_type_name": row.get("LCLAS_SC_NM", ""),
            "address": row.get("ORG_RDNMA", ""),
            "zip_code": row.get("ORG_RDNZC", ""),
            "phone": row.get("ORG_TELNO", ""),
            "homepage": row.get("HMPG_ADRES", ""),
            "establishment_date": row.get("FOND_YMD", ""),
            "open_date": row.get("OPEN_YMD", ""),
            "close_date": row.get("CLOSE_YMD", ""),
            "latitude": self._parse_float(row.get("LTTUD")),
            "longitude": self._parse_float(row.get("LGTUD")),
            "collected_at": now,
            "updated_at": now,
            "is_active": 1,
            "in_neis": self._get_in_neis_status(school_code),
        }

    def _get_in_neis_status(self, school_code: str) -> int:
        """NEIS 등록 여부 조회 (예외 안전)"""
        try:
            return 1 if neis_validator.contains(school_code) else 0
        except Exception as e:
            self.logger.warning(f"NEIS 검증 실패 {school_code}: {e}")
            return 0

    def _parse_float(self, val: Union[str, float, None]) -> Optional[float]:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[Dict[str, Any]]) -> None:
        sql = """
            INSERT OR REPLACE INTO schools (
                school_code, school_name, region_code, region_name,
                school_type, school_type_name, address, zip_code,
                phone, homepage, establishment_date, open_date, close_date,
                latitude, longitude, collected_at, updated_at, is_active,
                in_neis
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [(
            r["school_code"], r["school_name"], r["region_code"], r["region_name"],
            r["school_type"], r["school_type_name"], r["address"], r["zip_code"],
            r["phone"], r["homepage"], r["establishment_date"], r["open_date"], r["close_date"],
            r["latitude"], r["longitude"], r["collected_at"], r["updated_at"], r["is_active"],
            r["in_neis"]
        ) for r in batch]
        conn.executemany(sql, rows)

    def _fetch_paginated(
        self,
        url: str,
        base_params: Dict[str, Any],
        root_key: str,
        region: Optional[str] = None,
        year: Optional[int] = None,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        session = build_session()
        all_rows: List[Dict[str, Any]] = []
        page = 1

        while True:
            params = base_params.copy()
            params.update({
                "pIndex": page,
                "pSize": page_size,
                "AY": str(year) if year else None,
                "Type": "json"
            })
            params = {k: v for k, v in params.items() if v is not None}

            data = safe_json_request(session, url, params, self.logger)
            if not data:
                break

            try:
                rows = data[root_key][1].get("row", []) if len(data[root_key]) > 1 else []
            except (KeyError, IndexError):
                rows = []

            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            page += 1

        return all_rows


if __name__ == "__main__":
    from core.collector_cli import run_collector

    def _fetch(collector: SchoolInfoCollector, region: str, **kwargs) -> None:
        collector.fetch_region(region, **kwargs)

    run_collector(
        SchoolInfoCollector,
        _fetch,
        "학교알리미 수집기",
    )
    