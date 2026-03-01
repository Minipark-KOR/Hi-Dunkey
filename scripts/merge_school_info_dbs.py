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
    total_db_path = os.path.join(MASTER_DIR, "school_info.db")

    # school_ 로 시작하는 모든 .db 파일 (total 제외)
    all_shard_dbs = [
        db for db in glob.glob(os.path.join(MASTER_DIR, "school_*.db"))
        if "total" not in db
    ]

    # 그 중에서 schools 테이블이 있는 파일만 필터링
    shard_dbs = []
    for db in all_shard_dbs:
        try:
            with sqlite3.connect(db) as conn:
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schools'"
                )
                if cur.fetchone():
                    shard_dbs.append(db)
                else:
                    print(f"⚠️ {os.path.basename(db)}: schools 테이블 없음 → 제외")
        except Exception as e:
            print(f"⚠️ {os.path.basename(db)}: 검사 실패 ({e}) → 제외")

    print(f"🔍 유효한 샤드 DB: {len(shard_dbs)}개")
    if not shard_dbs:
        print("❌ 병합할 유효한 샤드 데이터 없음")
        return

    # 기존 통합 DB 백업
    if os.path.exists(total_db_path):
        backup_path = total_db_path.replace(
            ".db", f"_backup_{time.strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(total_db_path, backup_path)
        print(f"💾 기존 DB 백업: {os.path.basename(backup_path)}")
        os.remove(total_db_path)

    # schools 테이블이 있는 첫 번째 파일에서 스키마 가져오기
    schema = None
    for db in shard_dbs:
        with sqlite3.connect(db) as src:
            cur = src.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'"
            )
            row = cur.fetchone()
            if row:
                schema = row[0]
                print(f"📋 스키마 출처: {os.path.basename(db)}")
                break
    if schema is None:
        print("❌ schools 테이블 스키마를 찾을 수 없습니다.")
        return

    # 통합 DB 생성
    conn = sqlite3.connect(total_db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(schema)
    conn.commit()

    # 모든 유효한 샤드 병합
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

    # 인덱스 생성
    print("🔨 인덱스 생성 중...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_school_region ON schools(atpt_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_school_status ON schools(status)")
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
    