#!/usr/bin/env python3
import os
import shutil
import glob
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트를 path에 추가
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from core.util.manage_log import build_domain_logger
from constants.paths import LOG_DIR, ACTIVE_DIR, BACKUP_DIR, ARCHIVE_DIR
from core.kst_time import now_kst
from core.data.backup import vacuum_into, move_files_by_age, cleanup_files_older_than

logger = build_domain_logger("feb22", "feb22", __file__)

ACTIVE = str(ACTIVE_DIR)
BACKUP = str(BACKUP_DIR)
ARCHIVE = str(ARCHIVE_DIR)

DOMAINS = ['meal', 'schedule', 'timetable']
SHARDS = ['even', 'odd']

def get_block_range(year):
    start = (year // 10) * 10
    end = start + 9
    return start, end

def backup_active():
    today = now_kst().strftime("%Y%m%d")
    target_year = now_kst().year - 1
    for d in DOMAINS:
        for s in SHARDS:
            src = os.path.join(ACTIVE, f"{d}_{s}.db")
            if not os.path.exists(src):
                continue
            dst = os.path.join(BACKUP, f"{target_year}_{d}_{s}_{today}.db")
            vacuum_into(src, dst)
            logger.info(f"Backup: {dst}")

def move_to_archive():
    cutoff = now_kst().year - 3
    moved = move_files_by_age(BACKUP, ARCHIVE, cutoff)
    for f in moved:
        logger.info(f"Moved to archive: {f}")

def get_table_schema(conn, table_name):
    """테이블의 컬럼 정보를 가져옴"""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return columns

def update_archive_merged():
    for d in DOMAINS:
        domain_files = []
        for p in glob.glob(os.path.join(ARCHIVE, f"*_{d}_*.db")):
            if "_merged" in p:
                continue
            base = os.path.basename(p)
            try:
                y = int(base.split('_')[0])
            except Exception:
                continue
            domain_files.append((y, p))

        if not domain_files:
            continue

        blocks = {}
        for y, p in domain_files:
            start = (y // 10) * 10
            end = start + 9
            block = f"{start}-{end}"
            blocks.setdefault(block, []).append((y, p))

        for block_name, files in sorted(blocks.items()):
            merged_path = os.path.join(ARCHIVE, f"{block_name}_{d}_merged.db")
            sample_file = files[0][1]

            with sqlite3.connect(sample_file) as sample_conn:
                src_columns = get_table_schema(sample_conn, d)
                if not src_columns:
                    continue

            with sqlite3.connect(merged_path) as dest:
                dest.execute(f"DROP TABLE IF EXISTS {d}")
                dest.execute(f"""
                    CREATE TABLE {d} (
                        year INTEGER,
                        {', '.join([f'{col} TEXT' for col in src_columns])}
                    )
                """)

                for y, year_file in sorted(files):
                    dest.execute(f"ATTACH DATABASE '{year_file}' AS src")
                    col_list = ", ".join(src_columns)
                    dest.execute(f"""
                        INSERT INTO main.{d} (year, {col_list})
                        SELECT {y}, {col_list} FROM src.{d}
                    """)
                    dest.execute("DETACH DATABASE src")

                dest.execute("VACUUM")
            logger.info(f"Updated merged: {merged_path}")

def cleanup_archive():
    deleted = cleanup_files_older_than(ARCHIVE, days=365, exclude_pattern="_merged")
    for f in deleted:
        logger.info(f"Deleted old archive file: {f}")

def main():
    logger.info("="*60)
    logger.info("🏁 2월 22일 작업 시작")
    backup_active()
    move_to_archive()
    update_archive_merged()
    cleanup_archive()
    logger.info("✅ 2월 22일 완료")

if __name__ == "__main__":
    main()
    