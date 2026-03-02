#!/usr/bin/env python3
"""
scripts/seed_failures.py

schools 테이블에서 좌표가 없는 모든 학교를 failures 테이블에 등록합니다.
(이미 등록된 중복은 RetryManager가 자동으로 무시/갱신)
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
    rm = RetryManager(db_path=failures_db_path)
    deadline = datetime.combine(now_kst().date(), time(15, 0))

    with sqlite3.connect(school_db_path) as conn:
        cur = conn.execute("""
            SELECT sc_code, address FROM schools
            WHERE (latitude IS NULL OR longitude IS NULL)
              AND address IS NOT NULL AND address != ''
        """)
        rows = cur.fetchall()

    count = 0
    for sc_code, address in rows:
        ok = rm.record_failure(
            domain="school",
            task_type="geocode",
            sc_code=sc_code,
            address=address,
            error="initial missing",
            deadline=deadline,
        )
        if ok:
            count += 1

    print(f"[seed_failures] {count}개 학교를 failures에 등록했습니다.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--school-db", default="data/master/school_info.db")
    parser.add_argument("--failures-db", default="data/failures.db")
    args = parser.parse_args()
    seed_missing_schools(args.school_db, args.failures_db)
    