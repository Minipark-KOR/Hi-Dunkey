#!/usr/bin/env python3
# scripts/migrate/migrate_neis_info.py
# NEIS 정보 데이터베이스 마이그레이션 (지오코딩 심화용)

import sqlite3
import sys
import os
from pathlib import Path

# 🔑 1. 이 스크립트 파일의 절대 경로를 기준으로 프로젝트 루트 찾기
SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/migrate/
PROJECT_ROOT = SCRIPT_DIR.parent.parent               # Hi-Dunkey/ (scripts 의 상위)

# 🔑 2. 프로젝트 루트를 sys.path 에 추가 (어디서 실행해도 임포트 가능)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 🔑 3. 이제 constants.paths 임포트 가능
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
        
        # NEIS 정보 전용 컬럼 (지오코딩, 주소 분석 등)
        new_columns = [
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
        
        conn.execute("UPDATE schools SET geocode_attempts = 0 WHERE geocode_attempts IS NULL")
        
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
        