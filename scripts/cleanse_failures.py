#!/usr/bin/env python3
# scripts/cleanse_failures.py
import sqlite3
import os
import sys
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.filters import AddressFilter
from core.logger import build_logger

logger = build_logger("cleanse_failures", "logs/cleanse_failures.log")


def _get_change_type(original: str, cleaned: str) -> str:
    """어떤 유형의 정제가 이루어졌는지 분류"""
    changes = []
    if len(original) > len(cleaned):
        changes.append("단축")
    if re.search(r'\([^)]*\)', original) and not re.search(r'\([^)]*\)', cleaned):
        changes.append("괄호제거")
    if '특별시' in original or '광역시' in original:
        if '서울 ' in cleaned or '부산 ' in cleaned:
            changes.append("지역명단축")
    if '번지' in original and '번지' not in cleaned:
        changes.append("번지제거")
    return ', '.join(changes) if changes else '일반정제'


def cleanse_and_requeue(
    failures_db: str = "data/failures.db",
    school_db: str = "data/master/school_info.db",
    show_changes: bool = True
):
    if not os.path.exists(failures_db):
        logger.error(f"DB 파일을 찾을 수 없습니다: {failures_db}")
        print(f"❌ DB 파일을 찾을 수 없습니다: {failures_db}")
        return

    rm = RetryManager(db_path=failures_db)

    rows = []
    with rm.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, address, sc_code FROM failures WHERE status='FAILED' AND resolved_at IS NULL"
        ).fetchall()

    logger.info(f"총 {len(rows)}개의 실패 데이터 검토 중...")
    print(f"📋 총 {len(rows)}개의 실패 데이터 검토 중...")
    print("=" * 80)

    updated = 0
    skipped_same = 0
    changes = []

    # 학교 DB 연결 (지번 저장용)
    with sqlite3.connect(school_db) as conn_s:
        conn_s.execute("PRAGMA journal_mode=WAL")  # 성능 최적화

        for row in rows:
            fid = row["id"]
            original = row["address"]
            sc_code = row["sc_code"]

            # 1. 지번 추출 및 schools 테이블 업데이트
            jibun = AddressFilter.extract_jibun(original)
            if jibun:
                conn_s.execute(
                    "UPDATE schools SET jibun_address = ? WHERE sc_code = ?",
                    (jibun, sc_code)
                )
                logger.info(f"📌 지번 업데이트: {sc_code} → {jibun}")

            # 2. 주소 정제 (level 4)
            cleaned = AddressFilter.clean(original, level=4)

            # 3. 원본과 다르면 재등록 (지번 정보 함께 저장)
            if cleaned != original:
                rm.mark_expired(fid, reason=f"주소 보정 후 재등록: {original} -> {cleaned}")

                success = rm.record_failure(
                    domain="school",
                    task_type="geocode",
                    sc_code=sc_code,
                    address=cleaned,
                    error="주소 보정 후 자동 재등록",
                    deadline=None,
                    jibun_address=jibun,  # ✅ 지번 함께 저장
                )

                if success:
                    updated += 1
                    changes.append({
                        'sc_code': sc_code,
                        'original': original,
                        'cleaned': cleaned,
                        'change_type': _get_change_type(original, cleaned)
                    })
                    logger.info(f"✅ [재등록 성공] {sc_code}: {cleaned[:60]}")
                    if show_changes:
                        print(f"✅ {sc_code}")
                else:
                    logger.error(f"❌ [재등록 실패] {sc_code}")
                    print(f"❌ {sc_code}")
            else:
                skipped_same += 1
                logger.debug(f"⏩ [변경 없음] {sc_code}")

        conn_s.commit()  # 학교 DB 변경사항 저장

    # 변경 사항 상세 출력
    if show_changes and changes:
        print("\n" + "=" * 80)
        print("📝 [주소 보정 상세 내역]")
        print("=" * 80)

        for i, ch in enumerate(changes, 1):
            print(f"\n[{i}/{len(changes)}] {ch['sc_code']} ({ch['change_type']})")
            print(f"   원본  : {ch['original']}")
            print(f"   → 보정 : {ch['cleaned']}")

        print("\n" + "=" * 80)

    # 최종 요약
    total = len(rows)
    summary = f"""
✨ 작업 완료
   - 총 검토       : {total}개
   - 보정 성공     : {updated}개
   - 변경 없음     : {skipped_same}개
   - 보정율        : {(updated/total*100) if total else 0:.1f}%
"""
    logger.info(summary)
    print(summary)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="실패 주소 정제 및 재등록")
    parser.add_argument("--failures-db", default="data/failures.db")
    parser.add_argument("--school-db", default="data/master/school_info.db")
    parser.add_argument("--quiet", action="store_true", help="상세 출력 없이 요약만 표시")
    args = parser.parse_args()

    print("🚀 cleanse_failures 시작...")
    cleanse_and_requeue(
        failures_db=args.failures_db,
        school_db=args.school_db,
        show_changes=not args.quiet
    )
    