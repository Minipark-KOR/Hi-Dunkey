#!/usr/bin/env python3
"""
학교 기본정보 샤드 DB 병합 (ATTACH 미사용, 직접 INSERT)
"""
import os
import sys
import sqlite3
import time
import shutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

def merge_databases():
    start_time = time.time()
    total_db_path = os.path.join(MASTER_DIR, "Neis_info.db")

    # 샤드 파일 목록 (neis_info_odd.db, neis_info_even.db)
    shard_files = ["neis_info_odd.db", "neis_info_even.db"]
    shard_dbs = []
    for fname in shard_files:
        full_path = os.path.join(MASTER_DIR, fname)
        if os.path.exists(full_path):
            shard_dbs.append(full_path)
            print(f"✅ {fname} 발견")
        else:
            print(f"❌ {fname} 없음")
            return

    if len(shard_dbs) != 2:
        print("❌ 샤드 DB가 2개 필요합니다.")
        return

    # 기존 통합 DB 백업
    if os.path.exists(total_db_path):
        backup_path = total_db_path.replace(
            ".db", f"_backup_{time.strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(total_db_path, backup_path)
        print(f"💾 기존 DB 백업: {os.path.basename(backup_path)}")
        os.remove(total_db_path)

    # 첫 번째 샤드에서 스키마 가져오기
    with sqlite3.connect(shard_dbs[0]) as src:
        cur = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'"
        )
        schema = cur.fetchone()[0]

    # 통합 DB 생성
    conn = sqlite3.connect(total_db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(schema)
    conn.commit()

    total_rows = 0
    for shard_db in shard_dbs:
        print(f"📦 병합 중: {os.path.basename(shard_db)}")
        # 각 샤드 읽기 전용 연결
        shard_conn = sqlite3.connect(f"file:{shard_db}?mode=ro", uri=True)
        shard_conn.row_factory = sqlite3.Row
        # 데이터 읽기
        cur = shard_conn.execute("SELECT * FROM schools")
        rows = cur.fetchall()
        # INSERT 수행
        insert_data = []
        for row in rows:
            insert_data.append((
                row['sc_code'], row['school_id'], row['sc_name'], row['eng_name'],
                row['sc_kind'], row['atpt_code'], row['address'], row['address_hash'],
                row['tel'], row['homepage'], row['status'], row['last_seen'], row['load_dt'],
                row['latitude'], row['longitude'], row['city_id'], row['district_id'],
                row['street_id'], row['number_bit']
            ))
        conn.executemany("""
            INSERT OR REPLACE INTO schools VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, insert_data)
        conn.commit()
        total_rows += len(insert_data)
        shard_conn.close()
        print(f"  ✅ {len(insert_data)}건")

    # 인덱스 생성
    print("🔨 인덱스 생성 중...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_school_region ON schools(atpt_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_school_status ON schools(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_school_geo ON schools(latitude, longitude) WHERE latitude IS NOT NULL")
    conn.commit()
    conn.execute("PRAGMA optimize")
    conn.close()

    elapsed = time.time() - start_time
    size = os.path.getsize(total_db_path) / 1024 / 1024
    print(f"\n✅ 병합 완료: {total_rows:,}건 | {size:.1f} MB | {elapsed:.1f}초")

if __name__ == "__main__":
    merge_databases()
    