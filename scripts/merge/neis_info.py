#!/usr/bin/env python3
# scripts/merge/neis_info.py
"""
neis_info 샤드 파일(odd/even)을 병합하여 통합 DB 생성
"""
import os
import sys
import sqlite3
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.data.database import get_db_connection
from constants.paths import MASTER_DIR

ODD_DB_DEFAULT = str(MASTER_DIR / "neis_info_odd.db")
EVEN_DB_DEFAULT = str(MASTER_DIR / "neis_info_even.db")
OUTPUT_DB_DEFAULT = str(MASTER_DIR / "neis_info.db")

GREEN = "\033[92m"
RESET = "\033[0m"


def print_progress_bar(current, total, success, fail, skip, bar_length=40):
    percent = float(current) / total if total > 0 else 0
    filled = int(bar_length * percent)
    bar = "█" * filled + "░" * (bar_length - filled)
    sys.stdout.write(f"\r📊 [병합] 신규={success}, 실패={fail}, 스킵={skip}\n")
    sys.stdout.write(f"[{bar}] {current}/{total} ✅{success}")
    sys.stdout.flush()
    if current == total:
        print()


def merge_shards(shard_files, output_db, verbose=True):
    if not shard_files:
        print("❌ 샤드 파일이 없습니다.")
        return

    os.makedirs(os.path.dirname(output_db), exist_ok=True)

    with sqlite3.connect(f"file:{shard_files[0]}?mode=ro", uri=True) as src_conn:
        schema = src_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'"
        ).fetchone()
        if not schema:
            print(f"❌ {shard_files[0]}에 schools 테이블이 없습니다.")
            return
        create_table_sql = schema[0]

    with get_db_connection(output_db) as dest_conn:
        dest_conn.execute("DROP TABLE IF EXISTS schools")
        dest_conn.execute(create_table_sql)
        if verbose:
            print(f"✅ 대상 DB 초기화 완료: {output_db}")

        total_rows = 0
        for shard_db in shard_files:
            shard_name = os.path.basename(shard_db).replace(".db", "")
            if verbose:
                print(f"\n📦 병합 중: {shard_name}")

            with sqlite3.connect(f"file:{shard_db}?mode=ro", uri=True) as src_conn:
                total_in_shard = src_conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]

                cur = src_conn.execute("SELECT * FROM schools")
                batch_size = 100
                batch = []
                success = 0
                fail = 0
                skip = 0

                for i, row in enumerate(cur, 1):
                    batch.append(row)
                    if len(batch) >= batch_size or i == total_in_shard:
                        try:
                            placeholders = ','.join(['?'] * len(row))
                            dest_conn.executemany(
                                f"INSERT OR REPLACE INTO schools VALUES ({placeholders})",
                                batch
                            )
                            dest_conn.commit()
                            success += len(batch)
                        except Exception as e:
                            print(f"\n❌ 배치 오류: {e}")
                            fail += len(batch)
                        batch = []

                    if i % 100 == 0 or i == total_in_shard:
                        current_total = total_rows + success + fail
                        total_expected = total_rows + total_in_shard
                        print_progress_bar(
                            current=current_total,
                            total=total_expected,
                            success=total_rows + success,
                            fail=fail,
                            skip=skip
                        )

                total_rows += success + fail

            if verbose:
                print(f"\n✅ {shard_name} 완료 (성공: {success}, 실패: {fail})")

    print(f"\n{GREEN}✅ 병합 완료! 총 레코드 수: {total_rows}{RESET}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NEIS 정보 샤드 병합")
    parser.add_argument("--odd", default=ODD_DB_DEFAULT)
    parser.add_argument("--even", default=EVEN_DB_DEFAULT)
    parser.add_argument("--output", default=OUTPUT_DB_DEFAULT)
    parser.add_argument("--year", required=True, type=int, help="대상 학년도")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    shard_files = []
    for f in [args.odd, args.even]:
        if os.path.exists(f):
            shard_files.append(f)
        else:
            print(f"⚠️ 경고: {f} 파일이 없습니다. 건너뜁니다.")

    if not shard_files:
        print("❌ 병합할 샤드 파일이 없습니다.")
        sys.exit(1)

    merge_shards(shard_files, args.output, verbose=not args.quiet)
