#!/usr/bin/env python3
# scripts/migrate_neis_info.py
# NEIS 정보 데이터베이스 마이그레이션 (지오코딩 심화용 + 전체 API 필드)

import sqlite3
import sys
import os
from pathlib import Path

# 🔑 프로젝트 루트 경로 계산 (scripts/migrate_neis_info.py 기준)
SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/
PROJECT_ROOT = SCRIPT_DIR.parent                       # 프로젝트 루트

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from constants.paths import NEIS_INFO_DB_PATH

def migrate(db_path: str) -> bool:
    if not os.path.exists(db_path):
        print(f"❌ DB 파일 없음: {db_path}")
        print(f"   → 먼저 neis_info_collector.py 를 실행하여 DB 를 생성하세요.")
        return False

    print(f"🔍 마이그레이션 시작: {db_path}")
    print("=" * 70)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")

        cur = conn.execute("PRAGMA table_info(schools)")
        existing = [row[1] for row in cur.fetchall()]
        print(f"📋 기존 컬럼: {len(existing)}개")

        # 추가할 컬럼 목록 (기존 지오코딩 컬럼 + NEIS API 전체 필드)
        new_columns = [
            # 기존 지오코딩 컬럼 (14개)
            ("cleaned_address", "TEXT"),
            ("geocode_attempts", "INTEGER DEFAULT 0"),
            ("last_error", "TEXT"),
            ("city_id", "INTEGER"),
            ("district_id", "INTEGER"),
            ("street_id", "INTEGER"),
            ("number_type", "TEXT"),
            ("number_value", "INTEGER"),
            ("number_start", "INTEGER"),
            ("number_end", "INTEGER"),
            ("number_bit", "INTEGER"),
            ("address_hash", "TEXT"),
            ("kakao_address", "TEXT"),
            ("jibun_address", "TEXT"),

            # NEIS API 전체 필드 (17개) - 순서는 API 문서 기준
            ("atpt_ofcdc_sc_nm", "TEXT"),           # 시도교육청명
            ("lctn_sc_nm", "TEXT"),                  # 시도명
            ("ju_org_nm", "TEXT"),                    # 관할조직명
            ("fond_sc_nm", "TEXT"),                   # 설립명
            ("org_rdnzc", "TEXT"),                    # 도로명우편번호 (앞자리 0 유지)
            ("org_rdnda", "TEXT"),                    # 도로명상세주소
            ("org_faxno", "TEXT"),                    # 팩스번호
            ("coedu_sc_nm", "TEXT"),                  # 남녀공학구분명
            ("hs_sc_nm", "TEXT"),                      # 고등학교구분명
            ("indst_specl_ccccl_exst_yn", "TEXT"),    # 산업체특별학급존재여부
            ("hs_gnrl_busns_sc_nm", "TEXT"),          # 고등학교일반전문구분명
            ("spcly_purps_hs_ord_nm", "TEXT"),        # 특수목적고등학교계열명
            ("ene_bfe_sehf_sc_nm", "TEXT"),           # 입시전후기구분명
            ("dght_sc_nm", "TEXT"),                    # 주야구분명
            ("fond_ymd", "TEXT"),                      # 설립일자
            ("foas_memrd", "TEXT"),                    # 개교기념일
            ("load_dtm", "TEXT"),                      # 수정일자
        ]

        added = 0
        for col, typ in new_columns:
            if col not in existing:
                try:
                    conn.execute(f"ALTER TABLE schools ADD COLUMN {col} {typ}")
                    print(f"  ✅ 추가: {col}")
                    added += 1
                except sqlite3.OperationalError as e:
                    print(f"  ⚠️  실패: {col} - {e}")
            else:
                print(f"  ⏭️  존재: {col}")

        # geocode_attempts NULL 처리
        conn.execute("UPDATE schools SET geocode_attempts = 0 WHERE geocode_attempts IS NULL")

        # 인덱스 생성 (기존과 동일)
        indexes = [
            ("idx_schools_missing", "ON schools(latitude) WHERE latitude IS NULL"),
            ("idx_schools_attempts", "ON schools(geocode_attempts) WHERE latitude IS NULL"),
            ("idx_address_hash", "ON schools(address_hash)"),
            ("idx_city", "ON schools(city_id)"),
            ("idx_district", "ON schools(district_id)"),
            ("idx_street", "ON schools(street_id)"),
        ]

        for idx_name, idx_def in indexes:
            try:
                conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} {idx_def}")
                print(f"  ✅ 인덱스: {idx_name}")
            except sqlite3.OperationalError as e:
                print(f"  ⚠️  인덱스 실패: {idx_name} - {e}")

        print("=" * 70)
        print(f"📊 결과: 추가 {added}개, 총 {len(new_columns)}개")
        return True

if __name__ == "__main__":
    print(f"📁 프로젝트 루트: {PROJECT_ROOT}")
    print()

    # 인자가 있으면 해당 경로 사용, 없으면 기본 경로 사용
    if len(sys.argv) >= 2:
        db_path = sys.argv[1]
        print(f"📍 지정 경로 사용: {db_path}")
    else:
        db_path = str(NEIS_INFO_DB_PATH)
        print(f"ℹ️  기본 경로 사용: {db_path}")

    print()

    if migrate(db_path):
        print("🎉 NEIS 정보 마이그레이션 완료")
    else:
        print("❌ 마이그레이션 실패")
        sys.exit(1)
        