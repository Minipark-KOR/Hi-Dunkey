#!/usr/bin/env python3
# scripts/migrate/migrate_school_info.py
import sqlite3, sys, os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/migrate/
PROJECT_ROOT = SCRIPT_DIR.parent.parent               # Hi-Dunkey/
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
        
        # 학교알리미 정보 전용 컬럼
        new_columns = [
            ("school_code", "TEXT"),
            ("school_name", "TEXT"),
            ("region_code", "TEXT"),
            ("region_name", "TEXT"),
            ("school_type", "TEXT"),
            ("school_type_name", "TEXT"),
            ("address", "TEXT"),
            ("zip_code", "TEXT"),
            ("phone", "TEXT"),
            ("homepage", "TEXT"),
            ("establishment_date", "TEXT"),
            ("open_date", "TEXT"),
            ("close_date", "TEXT"),
            ("latitude", "REAL"),
            ("longitude", "REAL"),
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
        
        indexes = [
            ("idx_schools_region", "ON schools(region_code)"),
            ("idx_schools_type", "ON schools(school_type)"),
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
        