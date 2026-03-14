#!/usr/bin/env python3
# scripts/run/vacuum.py
import sqlite3
import os
import sys
from datetime import timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.util.manage_log import build_domain_logger
from core.kst_time import now_kst
from constants.paths import FAILURES_DB_PATH, LOG_DIR

logger = build_domain_logger("vacuum", "vacuum", __file__)


def vacuum_db(db_path: str, days: int = 30):
    if not os.path.exists(db_path):
        logger.error(f"DB 없음: {db_path}")
        return
    conn   = sqlite3.connect(db_path)
    cutoff = (now_kst() - timedelta(days=days)).replace(tzinfo=None).isoformat()
    deleted = conn.execute(
        "DELETE FROM failures WHERE status IN ('SUCCESS', 'EXPIRED') AND resolved_at < ?",
        (cutoff,)
    ).rowcount
    conn.commit()
    conn.execute("VACUUM;")
    conn.close()
    logger.info(f"VACUUM 완료: {db_path} ({deleted}개 삭제)")


if __name__ == "__main__":
    vacuum_db(str(FAILURES_DB_PATH))
