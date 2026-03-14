#!/usr/bin/env python3
"""Excel 형식으로 데이터 내보내기"""
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logging.warning("pandas 가 없습니다. Excel 내보내기 기능이 제한됩니다.")

from .base import BaseExporter
from constants.paths import EXCEL_DIR
from constants.codes import REGION_NAMES

logger = logging.getLogger(__name__)


class ExcelExporter(BaseExporter):
    """SQLite DB → Excel 내보내기"""

    def __init__(self, output_dir: str = str(EXCEL_DIR), filename_prefix: str = "school_export"):
        super().__init__(output_dir, filename_prefix)

    def export_from_db(self, db_path: str, table_name: str,
                      regions: Optional[List[str]] = None,
                      school_types: Optional[List[str]] = None,
                      columns: Optional[List[str]] = None) -> str:
        """DB 에서 직접 데이터를 읽어 Excel 로 내보내기

        Args:
            db_path: SQLite DB 파일 경로
            table_name: 테이블명
            regions: 지역 코드 리스트 (예: ["B10", "J10"]) - 필터
            school_types: 학교급 코드 리스트 (예: ["2", "3"]) - 필터
            columns: 내보낼 컬럼 리스트 (기본값: 주요 컬럼)

        Returns:
            생성된 Excel 파일 경로
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("Excel 내보내기를 위해 pandas 설치가 필요합니다: pip install pandas openpyxl")

        import sqlite3

        if columns is None:
            columns = [
                'school_code', 'school_name', 'region_code', 'region_name',
                'school_type', 'school_type_name', 'address', 'zip_code',
                'phone', 'homepage', 'latitude', 'longitude', 'collected_at'
            ]

        where_clauses = []
        params = []

        if regions:
            placeholders = ','.join('?' * len(regions))
            where_clauses.append(f"region_code IN ({placeholders})")
            params.extend(regions)

        if school_types:
            placeholders = ','.join('?' * len(school_types))
            where_clauses.append(f"school_type IN ({placeholders})")
            params.extend(school_types)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        columns_sql = ', '.join(f"`{c}`" for c in columns)

        query = f"SELECT {columns_sql} FROM `{table_name}` {where_sql} ORDER BY region_name, school_name"

        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(query, conn, params=params)
        finally:
            conn.close()

        if df.empty:
            logger.warning(f"조회된 데이터가 없습니다. 조건: regions={regions}, types={school_types}")
            return ""

        # 컬럼명 한글화 (가독성)
        column_names_kr = {
            'school_code': '학교코드', 'school_name': '학교명',
            'region_code': '지역코드', 'region_name': '지역명',
            'school_type': '학교급코드', 'school_type_name': '학교급',
            'address': '주소', 'zip_code': '우편번호',
            'phone': '전화번호', 'homepage': '홈페이지',
            'latitude': '위도', 'longitude': '경도',
            'collected_at': '수집일시'
        }
        df = df.rename(columns={k: v for k, v in column_names_kr.items() if k in df.columns})

        region_suffix = regions[0] if regions and len(regions) == 1 else None
        filename = self._generate_filename("xlsx", region_suffix)
        output_path = self.output_dir / filename

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='학교목록', index=False)
            # 지역별 요약 시트 추가 (지역별 학교 수)
            if '지역명' in df.columns:
                summary = df['지역명'].value_counts().reset_index()
                summary.columns = ['지역', '학교수']
                summary.to_excel(writer, sheet_name='지역별요약', index=False)

        logger.info(f"Excel 내보내기 완료: {output_path} ({len(df)} 건)")
        return str(output_path)

    def export(self, data: List[Dict[str, Any]], metadata: Optional[Dict] = None) -> str:
        """리스트 데이터를 Excel 로 내보내기 (BaseExporter 인터페이스 구현)"""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas 가 필요합니다")

        import pandas as pd
        df = pd.DataFrame(data)
        region_suffix = metadata.get('region') if metadata else None
        filename = self._generate_filename("xlsx", region_suffix)
        output_path = self.output_dir / filename
        df.to_excel(output_path, index=False)
        logger.info(f"Excel 내보내기 완료: {output_path} ({len(df)} 건)")
        return str(output_path)
        