#!/usr/bin/env python3
import os
import shutil
import glob
import sqlite3
from datetime import datetime, timedelta
from core.logger import build_logger
from core.kst_time import now_kst
from core.backup import vacuum_into, move_files_by_age, cleanup_files_older_than

logger = build_logger("feb22", "../logs/feb22.log")
BASE_DIR = "../data"
ACTIVE = os.path.join(BASE_DIR, "active")
BACKUP = os.path.join(BASE_DIR, "backup")
ARCHIVE = os.path.join(BASE_DIR, "archive")
DOMAINS = ['meal', 'schedule', 'timetable', 'school']
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

def update_archive_merged():
    for block_dir in glob.glob(os.path.join(ARCHIVE, "*-*")):
        if not os.path.isdir(block_dir):
            continue
        block_name = os.path.basename(block_dir)
        start, end = map(int, block_name.split('-'))
        for d in DOMAINS:
            merged_path = os.path.join(block_dir, f"{block_name}_{d}_merged.db")
            with sqlite3.connect(merged_path) as dest:
                # 간단히 year 컬럼 추가한 테이블 생성 (실제 컬럼은 각 도메인에 맞게)
                dest.execute(f"""
                    CREATE TABLE IF NOT EXISTS {d} (
                        year INTEGER,
                        sc_code TEXT,
                        meal_date INTEGER,
                        PRIMARY KEY (year, sc_code, meal_date)
                    )
                """)
                for year_file in glob.glob(os.path.join(block_dir, f"*_{d}_*.db")):
                    if "_merged" in year_file:
                        continue
                    base = os.path.basename(year_file)
                    try:
                        y = int(base.split('_')[0])
                    except:
                        continue
                    if y < start or y > end:
                        continue
                    dest.execute(f"ATTACH DATABASE '{year_file}' AS src")
                    # 가정: src의 테이블명도 d와 같음
                    dest.execute(f"INSERT OR REPLACE INTO main.{d} SELECT {y}, * FROM src.{d}")
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
    