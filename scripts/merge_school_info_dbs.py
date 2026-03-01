#!/usr/bin/env python3
"""
학교 기본정보 샤드 DB 병합 (school_info_collector용)
"""
import os
import sys
import sqlite3
import glob
import time
import shutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

def merge_databases():
    start_time = time.time()
    total_db_path = os.path.join(MASTER_DIR, "school_info.db")   # ✅ 변경

    shard_dbs = [
        db for db in glob.glob(os.path.join(MASTER_DIR, "school_info_*.db"))
        if "total" not in db
    ]
    print(f"🔍 발견된 샤드 DB: {len(shard_dbs)}개")
    if not shard_dbs:
        print("❌ 병합할 샤드 데이터 없음")
        return

    if os.path.exists(total_db_path):
        backup_path = total_db_path.replace(
            ".db", f"_backup_{time.strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(total_db_path, backup_path)
        print(f"💾 기존 DB 백업: {os.path.basename(backup_path)}")
        os.remove(total_db_path)

    conn = sqlite3.connect(total_db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # 스키마는 첫 번째 샤드에서 복사 (schools 테이블)
    with sqlite3.connect(shard_dbs[0]) as src:
        schema = src.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'").fetchone()[0]
        conn.execute(schema)

    total_rows = 0
    for shard_db in shard_dbs:
        print(f"📦 병합 중: {os.path.basename(shard_db)}")
        conn.execute(f"ATTACH DATABASE '{shard_db}' AS shard")
        c = conn.execute(
            "INSERT OR REPLACE INTO main.schools SELECT * FROM shard.schools"
        )
        total_rows += c.rowcount
        conn.execute("DETACH DATABASE shard")
        print(f"  ✅ {c.rowcount}건")

    print("🔨 인덱스 생성 중...")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_school_region ON schools(atpt_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_school_status ON schools(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_school_geo ON schools(latitude, longitude) "
        "WHERE latitude IS NOT NULL"
    )
    conn.commit()

    conn.execute("PRAGMA optimize")
    conn.close()

    elapsed = time.time() - start_time
    size = os.path.getsize(total_db_path) / 1024 / 1024
    print(f"\n✅ 병합 완료: {total_rows:,}건 | {size:.1f} MB | {elapsed:.1f}초")

if __name__ == "__main__":
    merge_databases()
    