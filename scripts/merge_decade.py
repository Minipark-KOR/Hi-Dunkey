#!/usr/bin/env python3
"""
10년 단위 통합본 생성 스크립트
- 도메인별: decade_{start}_{end}_{domain}.db
- global_vocab: decade_{start}_{end}_global.db
- unknown_patterns: decade_{start}_{end}_unknown.db
- school은 연도별 스냅샷 (year 컬럼, PRIMARY KEY (sc_code, year))
"""
import os
import sys
import sqlite3
import glob
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.logger import build_logger

logger  = build_logger("merge_decade", "../logs/merge_decade.log")
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archive")

DOMAIN_CONFIG = {
    "meal": {
        "table": "meal",
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_meal_date   ON meal(meal_date)",
            "CREATE INDEX IF NOT EXISTS idx_meal_school ON meal(school_id)",
        ],
    },
    "timetable": {
        "table": "timetable",
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_tt_school ON timetable(school_id, ay)",
            "CREATE INDEX IF NOT EXISTS idx_tt_day    ON timetable(day_of_week)",
        ],
    },
    "schedule": {
        "table": "schedule",
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_sch_date   ON schedule(ev_date)",
            "CREATE INDEX IF NOT EXISTS idx_sch_school ON schedule(school_id)",
        ],
    },
    "school": {
        "table": "schools",
        "has_year": True,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_school_sc_year ON schools(sc_code, year)",
            "CREATE INDEX IF NOT EXISTS idx_school_id      ON schools(school_id)",
        ],
    },
}

GLOBAL_TABLES = ["vocab_meal", "vocab_timetable", "vocab_schedule", "meta_vocab"]
UNKNOWN_TABLE = "unknown_patterns"


def get_files(pattern: str, start: int, end: int) -> list:
    result = []
    for f in glob.glob(os.path.join(ARCHIVE_DIR, pattern)):
        base = os.path.basename(f)
        try:
            year = int(base[:4])
        except ValueError:
            continue
        if start <= year <= end:
            result.append(f)
    return sorted(result)


def _common_pragmas(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA page_size=8192")
    conn.execute("PRAGMA auto_vacuum=FULL")


def _finalize(conn: sqlite3.Connection, domain: str, dest: str) -> bool:
    for idx in DOMAIN_CONFIG.get(domain, {}).get("indexes", []):
        conn.execute(idx)
    conn.execute("VACUUM")
    conn.execute("ANALYZE")
    if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        logger.error(f"{domain} 무결성 검사 실패")
        return False
    sz = os.path.getsize(dest) / 1024 / 1024
    logger.info(f"✅ {domain} 완료 ({sz:.1f} MB)")
    return True


def create_domain_db(domain: str, start: int, end: int) -> str:
    files = get_files(f"*_{domain}.db", start, end)
    if not files:
        logger.warning(f"{domain}: 파일 없음 ({start}~{end})")
        return ""

    dest = os.path.join(ARCHIVE_DIR, f"decade_{start}_{end}_{domain}.db")
    logger.info(f"📦 {domain} 통합본 생성: {dest} ({len(files)}개)")

    if domain == "school":
        return _create_school_db(files, dest, start, end)
    return _create_generic_db(domain, files, dest)


def _create_generic_db(domain: str, files: list, dest: str) -> str:
    table  = DOMAIN_CONFIG[domain]["table"]
    schema = ""
    with sqlite3.connect(files[0]) as src:
        for line in src.iterdump():
            if f"CREATE TABLE {table}" in line or f'CREATE TABLE "{table}"' in line:
                schema = line
                break
    if not schema:
        logger.error(f"{domain}: 스키마 추출 실패")
        return ""

    with sqlite3.connect(dest) as conn:
        _common_pragmas(conn)
        conn.execute(schema)
        for f in files:
            conn.execute(f"ATTACH DATABASE '{f}' AS src")
            conn.execute(f"INSERT OR IGNORE INTO main.{table} SELECT * FROM src.{table}")
            conn.execute("DETACH DATABASE src")
        if not _finalize(conn, domain, dest):
            return ""
    return dest


def _create_school_db(files: list, dest: str, start: int, end: int) -> str:
    """school은 연도별 스냅샷 — year 컬럼 추가, PRIMARY KEY (sc_code, year)"""
    with sqlite3.connect(files[0]) as src:
        columns = [row[1] for row in src.execute("PRAGMA table_info(schools)")]

    col_defs = [f"{col} TEXT" for col in columns] + ["year INTEGER"]
    schema   = (
        f"CREATE TABLE schools ({', '.join(col_defs)}, "
        f"PRIMARY KEY (sc_code, year))"
    )

    with sqlite3.connect(dest) as conn:
        _common_pragmas(conn)
        conn.execute(schema)
        for f in files:
            base = os.path.basename(f)
            try:
                file_year = int(base[:4])
            except ValueError:
                continue
            if not (start <= file_year <= end):
                continue
            conn.execute(f"ATTACH DATABASE '{f}' AS src")
            col_names   = ", ".join(columns + ["year"])
            select_cols = ", ".join([f"src.{c}" for c in columns] + [str(file_year)])
            conn.execute(f"""
                INSERT OR REPLACE INTO main.schools ({col_names})
                SELECT {select_cols} FROM src.schools
            """)
            conn.execute("DETACH DATABASE src")
        if not _finalize(conn, "school", dest):
            return ""
    return dest


def create_global_db(start: int, end: int) -> str:
    files = get_files("*_global_vocab.db", start, end)
    if not files:
        logger.warning("global_vocab: 파일 없음")
        return ""

    dest = os.path.join(ARCHIVE_DIR, f"decade_{start}_{end}_global.db")
    logger.info(f"📦 global_vocab 통합본 생성: {dest}")

    with sqlite3.connect(files[0]) as src:
        with sqlite3.connect(dest) as conn:
            _common_pragmas(conn)
            src.backup(conn)

    with sqlite3.connect(dest) as conn:
        for f in files[1:]:
            conn.execute(f"ATTACH DATABASE '{f}' AS src")
            for tbl in GLOBAL_TABLES:
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ).fetchone()
                if exists:
                    conn.execute(f"INSERT OR IGNORE INTO main.{tbl} SELECT * FROM src.{tbl}")
            conn.execute("DETACH DATABASE src")
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            logger.error("global_vocab 무결성 실패")
            return ""

    sz = os.path.getsize(dest) / 1024 / 1024
    logger.info(f"✅ global_vocab 완료 ({sz:.1f} MB)")
    return dest


def create_unknown_db(start: int, end: int) -> str:
    files = get_files("*_unknown_patterns.db", start, end)
    if not files:
        logger.warning("unknown_patterns: 파일 없음")
        return ""

    dest = os.path.join(ARCHIVE_DIR, f"decade_{start}_{end}_unknown.db")
    logger.info(f"📦 unknown_patterns 통합본 생성: {dest}")

    with sqlite3.connect(files[0]) as src:
        with sqlite3.connect(dest) as conn:
            _common_pragmas(conn)
            src.backup(conn)

    with sqlite3.connect(dest) as conn:
        for f in files[1:]:
            conn.execute(f"ATTACH DATABASE '{f}' AS src")
            conn.execute(
                f"INSERT OR IGNORE INTO main.{UNKNOWN_TABLE} SELECT * FROM src.{UNKNOWN_TABLE}"
            )
            conn.execute("DETACH DATABASE src")
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            logger.error("unknown_patterns 무결성 실패")
            return ""

    sz = os.path.getsize(dest) / 1024 / 1024
    logger.info(f"✅ unknown_patterns 완료 ({sz:.1f} MB)")
    return dest


def main():
    parser = argparse.ArgumentParser(description="10년 단위 통합본 생성")
    parser.add_argument("--start",  type=int, required=True)
    parser.add_argument("--end",    type=int, required=True)
    parser.add_argument("--domain", choices=list(DOMAIN_CONFIG.keys()) + ["global", "unknown"])
    args = parser.parse_args()

    if args.start > args.end:
        print("❌ 시작 연도가 종료 연도보다 큼")
        return

    logger.info(f"🚀 {args.start}~{args.end} 통합본 생성 시작")
    if args.domain:
        if args.domain == "global":
            create_global_db(args.start, args.end)
        elif args.domain == "unknown":
            create_unknown_db(args.start, args.end)
        else:
            create_domain_db(args.domain, args.start, args.end)
    else:
        for d in DOMAIN_CONFIG:
            create_domain_db(d, args.start, args.end)
        create_global_db(args.start, args.end)
        create_unknown_db(args.start, args.end)

    logger.info("✅ 모든 통합본 생성 완료")


if __name__ == "__main__":
    main()
