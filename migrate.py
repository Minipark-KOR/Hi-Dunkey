#!/usr/bin/env python3
# migrate.py
import sqlite3
import sys
import os

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
            ("kakao_address", "TEXT"),   # ✅ Kakao 공식 주소 저장용
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
    if len(sys.argv) < 2:
        print("사용법: python migrate.py <school_info.db 경로>")
        sys.exit(1)
    db_path = sys.argv[1]
    if migrate(db_path):
        print("🎉 마이그레이션 완료")
    else:
        sys.exit(1)
        