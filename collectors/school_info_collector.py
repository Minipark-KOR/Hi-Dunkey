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
from constants.codes import REGION_NAMES, ALL_REGIONS
from constants.paths import MASTER_DIR

# NEIS 학교 기본정보 API 엔드포인트 (학교알리미)
API_URL = "https://open.neis.go.kr/hub/schoolInfo"


class SchoolInfoCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, debug_mode=False):
        # BaseCollecter에 도메인명 "school_info" 전달 → DB 경로가 data/master/school_info[_shard].db 로 결정됨
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
        """
        지역별 학교 정보 수집
        - region_code: 교육청 코드 (B10, C10, ...)
        - year: 학년도 (기본값: 현재 날짜 기준 학년도)
        - date: 수집 기준일 (미사용, API 호환용)
        """
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

        # 샤드 필터링 + 배치 저장
        for row in rows:
            school_code = row.get("SD_SCHUL_CODE")
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue

            self.enqueue([self._transform_row(row, region_code)])

    def _transform_row(self, row: dict, region_code: str) -> dict:
        """API 응답 row를 DB 레코드 딕셔너리로 변환"""
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
        """배치 저장 (BaseCollector의 콜백)"""
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
        """
        페이지네이션 처리 (core.network의 safe_json_request 활용)
        """
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
            # None 값 제거
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
        