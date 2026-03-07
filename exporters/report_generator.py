#!/usr/bin/env python3
"""통계 리포트 자동 생성 (JSON/CSV/Text)"""
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging
import sqlite3

from .base_exporter import BaseExporter

logger = logging.getLogger(__name__)

class ReportGenerator(BaseExporter):
    """통계 리포트 생성기"""

    def __init__(self, output_dir: str = "reports/stats", filename_prefix: str = "school_stats"):
        super().__init__(output_dir, filename_prefix)

    def generate_from_db(self, db_path: str, table_name: str,
                        regions: Optional[List[str]] = None,
                        report_format: str = 'json') -> str:
        """DB 에서 통계를 계산하여 리포트 생성

        생성되는 통계 항목 (메트릭):
        - total_count: 전체 학교 수 (데이터 규모)
        - region_distribution: 지역별 학교 수 분포 (지역별 데이터 현황)
        - school_type_distribution: 학교급별 수 (유형별 분포)
        - geo_coverage: 좌표 보유율 (데이터 품질 지표, 위도/경도 존재 여부)
        - latest_collection: 최근 수집 일시 (데이터 신선도)
        """
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            stats = {
                "generated_at": datetime.now().isoformat(),
                "db_path": db_path,
                "table_name": table_name,
                "filters": {"regions": regions},
                "summary": {}
            }

            where_sql = ""
            params = []
            if regions:
                placeholders = ','.join('?' * len(regions))
                where_sql = f"WHERE region_code IN ({placeholders})"
                params = regions

            # 1. 전체 건수 (total_count)
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}` {where_sql}", params)
            stats["summary"]["total_count"] = cursor.fetchone()["cnt"]

            # 2. 지역별 분포 (region_distribution)
            cursor.execute(f"""
                SELECT region_name, COUNT(*) as cnt 
                FROM `{table_name}` 
                {where_sql}
                GROUP BY region_code, region_name 
                ORDER BY cnt DESC
            """, params)
            stats["summary"]["by_region"] = [
                {"region": row["region_name"], "count": row["cnt"]}
                for row in cursor.fetchall()
            ]

            # 3. 학교급별 분포 (school_type_distribution)
            cursor.execute(f"""
                SELECT school_type_name, COUNT(*) as cnt 
                FROM `{table_name}` 
                {where_sql}
                GROUP BY school_type 
                ORDER BY cnt DESC
            """, params)
            stats["summary"]["by_type"] = [
                {"type": row["school_type_name"], "count": row["cnt"]}
                for row in cursor.fetchall()
            ]

            # 4. 좌표 보유율 (geo_coverage) - 데이터 품질 지표
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as with_geo
                FROM `{table_name}` {where_sql}
            """, params)
            row = cursor.fetchone()
            if row["total"] > 0:
                stats["summary"]["geo_coverage"] = {
                    "with_coordinates": row["with_geo"],  # 좌표가 있는 학교 수
                    "coverage_rate": round(row["with_geo"] / row["total"] * 100, 2)  # 좌표 보유율 (%)
                }

            # 5. 최근 수집 일시 (latest_collection)
            cursor.execute(f"""
                SELECT MAX(collected_at) as latest FROM `{table_name}` {where_sql}
            """, params)
            latest = cursor.fetchone()["latest"]
            stats["summary"]["latest_collection"] = latest

        finally:
            conn.close()

        # 파일 저장
        if report_format == 'json':
            filename = self._generate_filename("json", regions[0] if regions and len(regions)==1 else None)
            output_path = self.output_dir / filename
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)

        elif report_format == 'csv':
            filename = self._generate_filename("csv", regions[0] if regions and len(regions)==1 else None)
            output_path = self.output_dir / filename
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("지표,값,상세\n")
                f.write(f"총 학교수,{stats['summary']['total_count']},-\n")
                for item in stats['summary'].get('by_region', []):
                    f.write(f"지역별_{item['region']},{item['count']},지역분포\n")
                for item in stats['summary'].get('by_type', []):
                    f.write(f"학교급별_{item['type']},{item['count']},학교급분포\n")
                geo = stats['summary'].get('geo_coverage', {})
                if geo:
                    f.write(f"좌표보유율,{geo['coverage_rate']}%,{geo['with_coordinates']}건\n")

        else:  # text
            filename = self._generate_filename("txt", regions[0] if regions and len(regions)==1 else None)
            output_path = self.output_dir / filename
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"📊 학교 정보 통계 리포트\n")
                f.write(f"생성일시: {stats['generated_at']}\n")
                f.write(f"총 학교수: {stats['summary']['total_count']} 건\n\n")

                f.write("📍 지역별 분포:\n")
                for item in stats['summary'].get('by_region', [])[:10]:
                    f.write(f"  • {item['region']}: {item['count']} 건\n")

                f.write(f"\n🎓 학교급별 분포:\n")
                for item in stats['summary'].get('by_type', []):
                    f.write(f"  • {item['type']}: {item['count']} 건\n")

                geo = stats['summary'].get('geo_coverage', {})
                if geo:
                    f.write(f"\n🗺️ 지리정보 품질: {geo['coverage_rate']}% ({geo['with_coordinates']} 건)\n")

        logger.info(f"리포트 생성 완료: {output_path}")
        return str(output_path)

    def export(self, data: List[Dict[str, Any]], metadata: Optional[Dict] = None) -> str:
        """리스트 데이터로부터 간단한 리포트 생성 (인터페이스 구현)"""
        report = {
            "generated_at": datetime.now().isoformat(),
            "record_count": len(data),
            "metadata": metadata or {}
        }
        filename = self._generate_filename("json")
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        return str(output_path)
        