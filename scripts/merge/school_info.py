#!/usr/bin/env python3
# scripts/merge/school_info.py
"""
school_info 샤드 파일(odd/even)을 병합하여 통합 DB 생성
"""
import os
import sys
import sqlite3
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.data.database import get_db_connection
from constants.paths import SCHOOL_INFO_ODD_DB_PATH, SCHOOL_INFO_EVEN_DB_PATH, SCHOOL_INFO_DB_PATH


def merge_shards(shard_files, output_db):
    if not shard_files:
        print("❌ 샤드 파일이 없습니다.")
        return

    with sqlite3.connect(f"file:{shard_files[0]}?mode=ro", uri=True) as src:
        schema = src.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'"
        ).fetchone()
        if not schema:
            print(f"❌ {shard_files[0]}에 schools 테이블이 없습니다.")
            return
        create_table_sql = schema[0]

    with get_db_connection(output_db) as dest:
        dest.execute("DROP TABLE IF EXISTS schools")
        dest.execute(create_table_sql)
        print(f"✅ 대상 DB 초기화 완료: {output_db}")

        total_rows = 0
        for shard_db in shard_files:
            shard_name = os.path.basename(shard_db).replace(".db", "")
            print(f"\n📦 병합 중: {shard_name}")
            with sqlite3.connect(f"file:{shard_db}?mode=ro", uri=True) as src:
                total_in_shard = src.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
                cur = src.execute("SELECT * FROM schools")
                batch = []
                batch_size = 100
                for i, row in enumerate(cur, 1):
                    batch.append(row)
                    if len(batch) >= batch_size or i == total_in_shard:
                        placeholders = ','.join(['?'] * len(row))
                        dest.executemany(
                            f"INSERT OR REPLACE INTO schools VALUES ({placeholders})", batch
                        )
                        dest.commit()
                        batch = []
                    if i % 100 == 0:
                        print(f"   {i}/{total_in_shard} 처리...")
                total_rows += total_in_shard
    print(f"\n✅ 병합 완료! 총 레코드 수: {total_rows}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--odd", default=str(SCHOOL_INFO_ODD_DB_PATH))
    parser.add_argument("--even", default=str(SCHOOL_INFO_EVEN_DB_PATH))
    parser.add_argument("--output", default=str(SCHOOL_INFO_DB_PATH))
    args = parser.parse_args()

    shard_files = []
    for f in [args.odd, args.even]:
        if os.path.exists(f):
            shard_files.append(f)
        else:
            print(f"⚠️ 경고: {f} 파일이 없습니다. 건너뜁니다.")
    if not shard_files:
        sys.exit(1)
    merge_shards(shard_files, args.output)
