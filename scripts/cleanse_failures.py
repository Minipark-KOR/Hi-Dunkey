#!/usr/bin/env python3
# scripts/cleanse_failures.py
"""
실패 큐의 도로명 주소에서 지번 주소를 추출하여 보정
"""
import sqlite3
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.filters import AddressFilter
from core.logger import build_logger

logger = build_logger("cleanse_failures", "logs/cleanse_failures.log")

DB_PATH = "data/failures.db"
SCHOOL_DB = "data/master/school_info.db"


def cleanse_failures():
    if not os.path.exists(DB_PATH):
        logger.error(f"DB 파일을 찾을 수 없습니다: {DB_PATH}")
        print(f"❌ DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return

    rm = RetryManager(db_path=DB_PATH)

    # failures 테이블에 jibun_address 컬럼이 없으면 추가
    with rm.get_connection() as conn:
        try:
            conn.execute("ALTER TABLE failures ADD COLUMN jibun_address TEXT")
            print("✅ failures.jibun_address 컬럼 추가됨")
        except sqlite3.OperationalError:
            print("⏭️  jibun_address 컬럼 이미 존재")

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

    # 학교 DB 연결 (지번 저장용)
    with sqlite3.connect(SCHOOL_DB) as conn_s:
        conn_s.execute("PRAGMA journal_mode=WAL")

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
                    jibun_address=jibun,
                )

                if success:
                    updated += 1
                    logger.info(f"✅ [재등록 성공] {sc_code}: {cleaned[:60]}")
                    print(f"✅ {sc_code}")
                else:
                    logger.error(f"❌ [재등록 실패] {sc_code}")
                    print(f"❌ {sc_code}")
            else:
                skipped_same += 1

        conn_s.commit()

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
    parser.add_argument("--failures-db", default=DB_PATH)
    parser.add_argument("--school-db", default=SCHOOL_DB)
    parser.add_argument("--quiet", action="store_true", help="상세 출력 없이 요약만 표시")
    args = parser.parse_args()

    print("🚀 cleanse_failures 시작...")
    cleanse_failures()
    