#!/usr/bin/env python3
"""
초기 누락 학교를 실패 큐에 등록
- 지오코딩 좌표가 없는 학교를 failures.db 에 등록하여 retry_worker 가 처리하도록 함
"""
import os
import sys
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger

logger = build_logger("seed_failures", "logs/seed_failures.log")


def seed_missing_schools(
    school_db_path: str = "data/master/school_info.db",
    failures_db_path: str = "data/failures.db",
):
    """
    지오코딩 누락 학교를 failures 큐에 등록
    """
    rm = RetryManager(db_path=failures_db_path, max_retries=None)
    
    if not os.path.exists(school_db_path):
        logger.error(f"School DB not found: {school_db_path}")
        print(f"❌ School DB 없음: {school_db_path}")
        return
    
    with sqlite3.connect(school_db_path) as conn:
        cur = conn.execute("""
            SELECT sc_code, address FROM schools
            WHERE (latitude IS NULL OR longitude IS NULL)
            AND address IS NOT NULL AND address != ''
        """)
        rows = cur.fetchall()
    
    if not rows:
        print("✅ 지오코딩 누락 학교가 없습니다.")
        logger.info("No missing schools found.")
        return
    
    print(f"📋 지오코딩 누락 학교: {len(rows)}개 발견")
    logger.info(f"Found {len(rows)} missing schools.")
    
    count = 0
    for sc_code, address in rows:
        ok = rm.record_failure(
            domain="school",
            task_type="geocode",
            sc_code=sc_code,
            address=address,
            error="initial missing",
            deadline=None,
        )
        if ok:
            count += 1
            if count <= 10:  # 최대 10 개까지 상세 출력
                print(f"  ✅ [{count:3d}] {sc_code}: {address[:50]}...")
        else:
            logger.warning(f"Failed to register: {sc_code} - {address[:50]}...")
    
    if len(rows) > 10:
        print(f"  ... (생략: {len(rows) - 10}개)")
    
    print(f"📊 결과: {count}/{len(rows)} 개 등록 완료")
    logger.info(f"Registered {count} schools in failures table.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="누락 학교를 실패 큐에 등록")
    parser.add_argument("--school-db", default="data/master/school_info.db")
    parser.add_argument("--failures-db", default="data/failures.db")
    args = parser.parse_args()
    
    print("🚀 seed_failures 시작...")
    seed_missing_schools(args.school_db, args.failures_db)
    print("✨ seed_failures 완료!")
    