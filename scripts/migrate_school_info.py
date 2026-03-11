#!/usr/bin/env python3
# scripts/migrate_school_info.py
import sqlite3
import sys
import os
from pathlib import Path

# 🔑 경로 계산 (scripts/migrate_school_info.py 기준)
SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/
PROJECT_ROOT = SCRIPT_DIR.parent                       # 프로젝트 루트

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from constants.paths import SCHOOL_INFO_DB_PATH

def migrate(db_path: str) -> bool:
    if not os.path.exists(db_path):
        print(f"❌ DB 파일 없음: {db_path}")
        return False

    print(f"🔍 마이그레이션 시작: {db_path}")
    print("=" * 70)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")

        cur = conn.execute("PRAGMA table_info(schools)")
        existing = [row[1] for row in cur.fetchall()]
        print(f"📋 기존 컬럼: {len(existing)}개")

        # 학교알리미 API 전체 필드 (총 37개)
        new_columns = [
            # 기본 키
            ("school_code", "TEXT"),
            
            # 시도교육청 관련
            ("atpt_ofcdc_org_nm", "TEXT"),
            ("atpt_ofcdc_org_code", "TEXT"),
            ("ju_org_nm", "TEXT"),
            ("ju_org_code", "TEXT"),
            
            # 지역 정보
            ("adrcd_nm", "TEXT"),
            ("adrcd_cd", "TEXT"),
            ("lctn_sc_code", "TEXT"),
            
            # 학교 기본 정보
            ("schul_nm", "TEXT"),
            ("schul_knd_sc_code", "TEXT"),
            ("fond_sc_code", "TEXT"),
            ("hs_knd_sc_nm", "TEXT"),
            ("bnhh_yn", "TEXT"),
            ("schul_fond_typ_code", "TEXT"),
            ("dght_sc_code", "TEXT"),
            
            # 설립/개교일
            ("foas_memrd", "TEXT"),
            ("fond_ymd", "TEXT"),
            
            # 주소/위치
            ("adres_brkdn", "TEXT"),
            ("dtlad_brkdn", "TEXT"),
            ("zip_code", "TEXT"),
            ("schul_rdnzc", "TEXT"),
            ("schul_rdnma", "TEXT"),
            ("schul_rdnda", "TEXT"),
            ("lttud", "REAL"),
            ("lgtud", "REAL"),
            
            # 연락처
            ("user_telno", "TEXT"),
            ("user_telno_sw", "TEXT"),
            ("user_telno_ga", "TEXT"),
            ("perc_faxno", "TEXT"),
            ("hmpg_adres", "TEXT"),
            
            # 기타 구분
            ("coedu_sc_code", "TEXT"),
            ("absch_yn", "TEXT"),
            ("absch_ymd", "TEXT"),
            ("close_yn", "TEXT"),
            
            # 각종학교용
            ("schul_crse_sc_value", "TEXT"),
            ("schul_crse_sc_value_nm", "TEXT"),
            
            # 수집 메타
            ("collected_at", "TEXT"),
            ("updated_at", "TEXT"),
            ("is_active", "INTEGER DEFAULT 1"),
            ("in_neis", "INTEGER DEFAULT 0"),
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

        # 인덱스 생성 (필요시)
        indexes = [
            ("idx_schools_region", "ON schools(atpt_ofcdc_org_code)"),
            ("idx_schools_type", "ON schools(schul_knd_sc_code)"),
            ("idx_schools_in_neis", "ON schools(in_neis)"),
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

    if len(sys.argv) >= 2:
        db_path = sys.argv[1]
        print(f"📍 지정 경로 사용: {db_path}")
    else:
        db_path = str(SCHOOL_INFO_DB_PATH)
        print(f"ℹ️  기본 경로 사용: {db_path}")

    print()

    if migrate(db_path):
        print("🎉 학교알리미 정보 마이그레이션 완료")
    else:
        print("❌ 마이그레이션 실패")
        sys.exit(1)
        