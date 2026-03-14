#!/usr/bin/env python3
"""
unknown 패턴 자동 분석 - GitHub Actions용
"""
import os
import sqlite3
from core.data.extractor_meal import UnknownPatternAnalyzer
from constants.paths import UNKNOWN_DB_PATH

REPORT_PATH = "unknown_patterns_report.md"


def generate_report():
    """Markdown 형식의 리포트 생성"""
    
    if not os.path.exists(UNKNOWN_DB_PATH):
        print("ℹ️ unknown_patterns.db 파일 없음")
        return
    
    analyzer = UnknownPatternAnalyzer(UNKNOWN_DB_PATH)
    stats = analyzer.get_statistics()
    patterns = analyzer.get_top_patterns(limit=30, min_frequency=5)
    
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("# 🔍 unknown 패턴 분석 리포트\n\n")
        f.write(f"분석 일시: {__import__('core.kst_time').kst_time.now_kst().isoformat()}\n\n")
        
        f.write("## 📊 통계\n\n")
        f.write(f"- 전체 패턴: {stats['total']}개\n")
        f.write(f"- 검토 완료: {stats['reviewed']}개\n")
        f.write(f"- 미검토: {stats['pending']}개\n\n")
        
        f.write("## 🔥 상위 패턴 (빈도수 5 이상)\n\n")
        f.write("| 값 | 빈도 | 타입 | 제안타입 |\n")
        f.write("|-----|------|------|----------|\n")
        
        for p in patterns:
            suggested = analyzer.suggest_meta_type(p)
            f.write(f"| {p['value']} | {p['frequency']} | {p['part_type']} | {suggested} |\n")
    
    print(f"✅ 리포트 생성: {REPORT_PATH}")


if __name__ == "__main__":
    generate_report()
    