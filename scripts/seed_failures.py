#!/usr/bin/env python3
"""
초기 누락 학교를 실패 큐에 등록
- 상세 출력으로 진행 상황 확인 가능
- 실행 후 메뉴에서 추가 작업 선택 가능
"""
import os
import sys
import sqlite3
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.kst_time import now_kst
from core.logger import build_logger

logger = build_logger("seed_failures", "logs/seed_failures.log")


def seed_missing_schools(
    school_db_path: str = "data/master/school_info.db",
    failures_db_path: str = "data/failures.db",
    verbose: bool = True
) -> dict:
    result = {
        'success': False,
        'found': 0,
        'registered': 0,
        'skipped': 0,
        'errors': []
    }

    if not os.path.exists(school_db_path):
        print(f"❌ School DB 없음: {school_db_path}")
        result['errors'].append("School DB 가 존재하지 않음")
        return result

    rm = RetryManager(db_path=failures_db_path, max_retries=None)

    # ✅ 수정: literal newline → \n
    print(f"\n🔍 누락 학교 검색 시작: {school_db_path}")
    print("=" * 70)

    try:
        with sqlite3.connect(school_db_path) as conn:
            cur = conn.execute("""
                SELECT sc_code, sc_name, address 
                FROM schools
                WHERE (latitude IS NULL OR longitude IS NULL)
                AND address IS NOT NULL AND address != ''
            """)
            rows = cur.fetchall()
            result['found'] = len(rows)

        if not rows:
            print("✅ 지오코딩 누락 학교가 없습니다.")
            result['success'] = True
            return result

        print(f"📋 지오코딩 누락 학교: {len(rows)}개 발견")
        print("-" * 70)

        count = 0
        skipped = 0

        for i, (sc_code, sc_name, address) in enumerate(rows, 1):
            ok = rm.record_failure(
                domain="school",
                task_type="geocode",
                sc_code=sc_code,
                address=address,
                error="initial missing",
                deadline=None,
            )
            if ok:
                count += 1
                if verbose and i <= 10:
                    print(f"  ✅ [{i:3d}] {sc_code}: {sc_name[:30]} ({address[:40]}...)")
            else:
                skipped += 1
                if verbose:
                    print(f"  ⚠️  [{i:3d}] {sc_code}: 등록 실패")

            if verbose and i > 10 and i % 100 == 0:
                print(f"  ... {i}개 처리 중")

        if len(rows) > 10:
            print(f"  ... (생략: {len(rows) - 10}개)")

        result['registered'] = count
        result['skipped'] = skipped
        result['success'] = True

        print("-" * 70)
        print(f"📊 결과: 등록 {count}개, 스킵 {skipped}개, 총 {len(rows)}개")

    except Exception as e:
        print(f"❌ 오류: {e}")
        result['errors'].append(str(e))
        logger.error(f"seed_failures 오류: {e}", exc_info=True)

    return result


def show_menu(school_db_path: str, failures_db_path: str):
    while True:
        # ✅ 수정: literal newline → \n
        print("\n" + "=" * 70)
        print("📋 추가 작업 메뉴")
        print("=" * 70)
        print("  1. 누락 학교 개수 확인")
        print("  2. failures 큐 상태 확인")
        print("  3. DB 파일 크기 확인")
        print("  4. retry_worker 즉시 실행")
        print("  5. seed_failures 재실행")
        print("  0. 종료")
        print("=" * 70)

        choice = input("번호를 선택하세요 (0-5): ").strip()

        if choice == '1':
            check_missing_count(school_db_path)
        elif choice == '2':
            check_failures_queue(failures_db_path)
        elif choice == '3':
            check_db_size(school_db_path, failures_db_path)
        elif choice == '4':
            run_retry_worker()
        elif choice == '5':
            seed_missing_schools(school_db_path, failures_db_path, verbose=True)
        elif choice == '0':
            print("👋 종료합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다.")


def check_missing_count(school_db_path: str):
    if not os.path.exists(school_db_path):
        print("❌ DB 파일 없음")
        return
    with sqlite3.connect(school_db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM schools WHERE latitude IS NULL OR longitude IS NULL")
        missing = cur.fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
        print(f"\n📍 지오코딩 상태: {missing}/{total} ({missing/total*100:.1f}% 누락)")


def check_failures_queue(failures_db_path: str):
    if not os.path.exists(failures_db_path):
        print("❌ DB 파일 없음")
        return
    with sqlite3.connect(failures_db_path) as conn:
        cur = conn.execute("SELECT status, COUNT(*) FROM failures GROUP BY status")
        rows = cur.fetchall()
        print("\n📊 failures 큐 상태")
        for s, c in rows:
            print(f"  - {s}: {c}개")


def run_retry_worker():
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    subprocess.run(
        [sys.executable, "scripts/retry_worker.py", "--limit", "100", "--force"],
        cwd=script_dir,
        env={**os.environ, "PYTHONPATH": script_dir}
    )


def check_db_size(school_db_path: str, failures_db_path: str):
    print("\n💾 DB 파일 크기:")
    for label, path in [("학교 DB", school_db_path), ("Failures DB", failures_db_path)]:
        if os.path.exists(path):
            size = os.path.getsize(path) / (1024*1024)
            print(f"  {label}: {size:.2f} MB")
        else:
            print(f"  {label}: 없음")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="누락 학교를 실패 큐에 등록")
    parser.add_argument("--school-db", default="data/master/school_info.db")
    parser.add_argument("--failures-db", default="data/failures.db")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-m", "--menu", action="store_true")
    args = parser.parse_args()

    result = seed_missing_schools(args.school_db, args.failures_db, verbose=not args.quiet)

    if args.menu or not result['success'] or result['found'] > 0:
        show_menu(args.school_db, args.failures_db)

    sys.exit(0 if result['success'] else 1)


if __name__ == "__main__":
    main()
    