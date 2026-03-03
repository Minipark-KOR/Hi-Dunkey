#!/usr/bin/env python3
# migrate.py
import sqlite3
import sys
import os

def migrate(db_path: str) -> bool:
    if not os.path.exists(db_path):
        print(f"❌ DB 파일 없음: {db_path}")
        return False

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")

        cur = conn.execute("PRAGMA table_info(schools)")
        existing = [row[1] for row in cur.fetchall()]

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
        ]

        for col, typ in new_columns:
            if col not in existing:
                conn.execute(f"ALTER TABLE schools ADD COLUMN {col} {typ}")
                print(f"✅ 컬럼 추가됨: {col}")

        conn.execute("UPDATE schools SET geocode_attempts = 0 WHERE geocode_attempts IS NULL")

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_schools_missing "
            "ON schools(latitude) WHERE latitude IS NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_schools_attempts "
            "ON schools(geocode_attempts) WHERE latitude IS NULL"
        )
        print("✅ 인덱스 생성 완료")

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
        