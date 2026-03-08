#!/usr/bin/env python3
# 개발 가이드: docs/developer_guide.md 참조
"""
학교알리미 오픈서비스 수집기 (BaseCollector 기반)
- NEIS 학교알리미 API (schoolInfo) 활용
- 학년도 필터링, 샤드 저장 지원
- collector_cli.py와 완전히 통합됨
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.network import safe_json_request, build_session
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR

API_URL = "https://open.neis.go.kr/hub/schoolInfo"


class SchoolInfoCollector(BaseCollector):
    # ----- 메타데이터 -----
    description = "학교 기본정보 (학교알리미)"
    table_name = "schools"
    merge_script = "scripts/merge_school_info_dbs.py"
    timeout_seconds = 3600
    parallel_timeout_seconds = 7200
    merge_timeout_seconds = 3600
    metrics_config = {
        "enabled": True,
        "collect_geo": True,
        "collect_global": True
    }
    parallel_config = {
        "max_workers": 4,
        "cpu_factor": 1.0,
        "max_by_api": 10,
        "absolute_max": 16
    }
    # ---------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False):
        super().__init__("school_info", str(MASTER_DIR), shard, school_range)
        self.debug_mode = debug_mode
        self._init_db()

    def _init_db(self):
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
                    is_active INTEGER DEFAULT 1
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(region_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(school_type)")

    def fetch_region(self, region_code: str, year: int = None, date: str = None):
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

    def _transform_row(self, row: dict, region_code: str) -> dict:
        now = now_kst().isoformat()
        return {
            "school_code": row.get("SD_SCHUL_CODE"),
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
        }

    def _parse_float(self, val):
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    def _do_save_batch(self, conn, batch):
        sql = """
            INSERT OR REPLACE INTO schools (
                school_code, school_name, region_code, region_name,
                school_type, school_type_name, address, zip_code,
                phone, homepage, establishment_date, open_date, close_date,
                latitude, longitude, collected_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [(
            r["school_code"], r["school_name"], r["region_code"], r["region_name"],
            r["school_type"], r["school_type_name"], r["address"], r["zip_code"],
            r["phone"], r["homepage"], r["establishment_date"], r["open_date"], r["close_date"],
            r["latitude"], r["longitude"], r["collected_at"], r["updated_at"], r["is_active"]
        ) for r in batch]
        conn.executemany(sql, rows)

    def _fetch_paginated(self, url, base_params, root_key, region=None, year=None, page_size=100):
        session = build_session()
        all_rows = []
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

    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)

    run_collector(
        SchoolInfoCollector,
        _fetch,
        "학교알리미 수집기",
    )
    