#!/usr/bin/env python3
# scripts/seed_failures.py
"""
초기 누락 학교 failures 테이블 등록 스크립트
- schools 테이블에서 좌표가 없는 학교를 조회하여 failures에 등록
- 데드라인 없이 무제한 재시도 대상으로 등록 (deadline=None)
"""
import os
import sys
import sqlite3
from datetime import datetime, time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.kst_time import now_kst


def seed_missing_schools(
    school_db_path: str = "data/master/school_info.db",
    failures_db_path: str = "data/failures.db",
):
    """
    schools 테이블에서 좌표가 NULL인 학교를 failures 테이블에 등록합니다.
    """
    # RetryManager 인스턴스 생성 (max_retries=None으로 무제한 재시도)
    rm = RetryManager(db_path=failures_db_path, max_retries=None)

    # schools DB 연결
    if not os.path.exists(school_db_path):
        print(f"❌ 학교 DB 파일 없음: {school_db_path}")
        return

    with sqlite3.connect(school_db_path) as conn:
        cur = conn.execute("""
            SELECT sc_code, address FROM schools
            WHERE (latitude IS NULL OR longitude IS NULL)
              AND address IS NOT NULL AND address != ''
        """)
        rows = cur.fetchall()

    if not rows:
        print("✅ 좌표가 누락된 학교가 없습니다.")
        return

    print(f"🔍 발견된 누락 학교: {len(rows)}개")

    count = 0
    for sc_code, address in rows:
        # 데드라인 없이 등록 (deadline=None 명시적 전달)
        ok = rm.record_failure(
            domain="school",
            task_type="geocode",
            sc_code=sc_code,
            address=address,
            error="initial missing",
            deadline=None,  # ✅ 중요: 데드라인 제한 없음
        )
        if ok:
            count += 1
        else:
            print(f"⚠️ 등록 실패: {sc_code} - {address[:50]}...")
            # 실패 원인 파악을 위한 디버그 정보
            print(f"   현재 시간(KST): {now_kst()}")
            print(f"   failures.db 경로: {failures_db_path}")

    print(f"\n📌 등록 완료: {count}개 / 전체 {len(rows)}개")
    if count < len(rows):
        print("일부 등록 실패. 상세 로그를 확인하세요.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="누락 학교 failures 등록")
    parser.add_argument(
        "--school-db",
        default="data/master/school_info.db",
        help="schools DB 경로 (기본: data/master/school_info.db)",
    )
    parser.add_argument(
        "--failures-db",
        default="data/failures.db",
        help="failures DB 경로 (기본: data/failures.db)",
    )
    args = parser.parse_args()

    seed_missing_schools(args.school_db, args.failures_db)
    