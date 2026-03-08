#!/usr/bin/env python3
"""
neis_info 샤드 파일(odd/even)을 병합하여 통합 DB 생성
"""
import os
import sys
import sqlite3
import argparse
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

from core.database import get_db_connection

# 색상 코드 (선택)
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
    """
    shard_files: 병합할 샤드 DB 파일 리스트 (예: ['neis_info_odd.db', 'neis_info_even.db'])
    output_db: 결과 통합 DB 경로
    """
    if not shard_files:
        print("❌ 샤드 파일이 없습니다.")
        return

    # 출력 DB 디렉토리 생성
    os.makedirs(os.path.dirname(output_db), exist_ok=True)

    # 첫 번째 샤드 DB에서 schools 테이블 스키마 가져오기
    with sqlite3.connect(f"file:{shard_files[0]}?mode=ro", uri=True) as src_conn:
        schema = src_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='schools'"
        ).fetchone()
        if not schema:
            print(f"❌ {shard_files[0]}에 schools 테이블이 없습니다.")
            return
        create_table_sql = schema[0]

    # 대상 DB 연결 및 테이블 생성
    with get_db_connection(output_db) as dest_conn:
        dest_conn.execute("DROP TABLE IF EXISTS schools")  # 기존 테이블 제거 (선택)
        dest_conn.execute(create_table_sql)
        if verbose:
            print(f"✅ 대상 DB 초기화 완료: {output_db}")

        total_rows = 0
        for shard_db in shard_files:
            shard_name = os.path.basename(shard_db).replace(".db", "")
            if verbose:
                print(f"\n📦 병합 중: {shard_name}")

            with sqlite3.connect(f"file:{shard_db}?mode=ro", uri=True) as src_conn:
                # 전체 행 수 확인
                cur = src_conn.execute("SELECT COUNT(*) FROM schools")
                total_in_shard = cur.fetchone()[0]

                # 데이터 읽기 (컬럼 순서 유지)
                cur = src_conn.execute("SELECT * FROM schools")
                batch_size = 100
                batch = []
                success = 0
                fail = 0
                skip = 0  # skip은 없지만 인터페이스 유지

                for i, row in enumerate(cur, 1):
                    batch.append(row)
                    if len(batch) >= batch_size or i == total_in_shard:
                        try:
                            # INSERT OR REPLACE (컬럼 수에 맞게 placeholders 생성)
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

                    # 진행률 출력 (100개 단위 또는 마지막)
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
    parser = argparse.ArgumentParser(description="neis_info 샤드 DB 병합")
    parser.add_argument("--odd", default="data/master/neis_info_odd.db", help="odd 샤드 파일 경로")
    parser.add_argument("--even", default="data/master/neis_info_even.db", help="even 샤드 파일 경로")
    parser.add_argument("--output", default="data/master/neis_info_total.db", help="출력 통합 DB 경로")
    parser.add_argument("--quiet", action="store_true", help="출력 최소화")
    parser.add_argument("--year", type=int, help="학년도 (사용되지 않음)")  # 추가
    args = parser.parse_args()

    # 파일 존재 확인
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
    