#!/usr/bin/env python3
# scripts/vacuum.py
import sqlite3
import os
from datetime import timedelta
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.logger import build_logger
from core.kst_time import now_kst

logger = build_logger("vacuum", "logs/vacuum.log")

def vacuum_db(db_path: str, days: int = 30):
    if not os.path.exists(db_path):
        logger.error(f"DB 없음: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cutoff = (now_kst() - timedelta(days=days)).replace(tzinfo=None).isoformat()
    deleted = conn.execute(
        "DELETE FROM failures WHERE status IN ('SUCCESS', 'EXPIRED') AND resolved_at < ?",
        (cutoff,)
    ).rowcount
    conn.commit()
    conn.execute("VACUUM;")
    conn.close()
    logger.info(f"VACUUM 완료: {db_path} ({deleted}개 레코드 삭제)")

if __name__ == "__main__":
    vacuum_db("data/failures.db")
    # 필요시 학교 DB 정리: vacuum_db("data/master/school_info.db")
    