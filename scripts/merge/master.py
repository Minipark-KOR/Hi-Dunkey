#!/usr/bin/env python3
# scripts/merge/master.py
"""
school_master.db 생성 스크립트
- school_info.db의 모든 데이터를 school_master.db로 복사
- additional_school_info.db의 데이터를 추가 (중복 시 무시)
"""
import sqlite3
import sys
from pathlib import Path
from typing import List

sys.path.append(str(Path(__file__).parent.parent.parent))

from constants.paths import (
    SCHOOL_INFO_DB_PATH,
    ADDITIONAL_SCHOOL_INFO_DB_PATH,
    SCHOOL_MASTER_DB_PATH,
)
from constants.schema import SCHEMAS
from core.util.manage_log import build_domain_logger

logger = build_domain_logger("master", "merge", __file__)

TABLE_NAME = SCHEMAS["school_info"]["table_name"]
COLUMNS = [col[0] for col in SCHEMAS["school_info"]["columns"]]
PK_COLUMNS = SCHEMAS["school_info"]["primary_key"]


def init_master_db(conn: sqlite3.Connection):
    schema = SCHEMAS["school_info"]
    col_defs = []
    for col, typ, constraint in schema["columns"]:
        col = col.strip()
        typ = typ.strip()
        constraint = constraint.strip() if constraint else ""
        col_def = f"{col} {typ}"
        if constraint:
            col_def += f" {constraint}"
        col_defs.append(col_def)

    pk = schema["primary_key"]
    create_sql = f"CREATE TABLE IF NOT EXISTS {schema['table_name']} ({', '.join(col_defs)}, PRIMARY KEY ({', '.join(pk)}))"
    conn.execute(create_sql)

    for idx in schema.get("indexes", []):
        if len(idx) == 2:
            idx_name, col = idx
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema['table_name']}({col})"
        else:
            idx_name, col, cond = idx
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema['table_name']}({col}) {cond}"
        conn.execute(sql)

    conn.commit()
    logger.info(f"✅ 테이블 및 인덱스 준비 완료: {SCHOOL_MASTER_DB_PATH}")


def copy_table_data(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection,
                    src_table: str, dst_table: str, columns: List[str]):
    cur_src = src_conn.cursor()
    cur_src.execute(f"SELECT {','.join(columns)} FROM {src_table}")
    rows = cur_src.fetchall()

    if not rows:
        logger.info(f"   소스 테이블 {src_table}에 데이터가 없습니다.")
        return 0

    placeholders = ','.join(['?' for _ in columns])
    insert_sql = f"INSERT OR IGNORE INTO {dst_table} ({','.join(columns)}) VALUES ({placeholders})"

    cur_dst = dst_conn.cursor()
    cur_dst.executemany(insert_sql, rows)
    inserted = cur_dst.rowcount
    logger.info(f"   {inserted}개 레코드 삽입됨 (총 {len(rows)}개 중)")
    return inserted


def main():
    logger.info("🚀 school_master.db 생성 시작")

    if not SCHOOL_INFO_DB_PATH.exists():
        logger.error(f"school_info.db 없음: {SCHOOL_INFO_DB_PATH}")
        sys.exit(1)
    if not ADDITIONAL_SCHOOL_INFO_DB_PATH.exists():
        logger.warning(f"additional_school_info.db 없음: {ADDITIONAL_SCHOOL_INFO_DB_PATH} (계속 진행)")

    master_conn = sqlite3.connect(str(SCHOOL_MASTER_DB_PATH))
    try:
        init_master_db(master_conn)

        logger.info("📄 school_info.db 처리 중...")
        src_conn = sqlite3.connect(str(SCHOOL_INFO_DB_PATH))
        try:
            cnt = copy_table_data(src_conn, master_conn, TABLE_NAME, TABLE_NAME, COLUMNS)
            logger.info(f"   school_info.db에서 {cnt}개 복사됨")
        finally:
            src_conn.close()

        if ADDITIONAL_SCHOOL_INFO_DB_PATH.exists():
            logger.info("📄 additional_school_info.db 처리 중...")
            add_conn = sqlite3.connect(str(ADDITIONAL_SCHOOL_INFO_DB_PATH))
            try:
                cnt = copy_table_data(add_conn, master_conn, TABLE_NAME, TABLE_NAME, COLUMNS)
                logger.info(f"   additional_school_info.db에서 {cnt}개 복사됨")
            finally:
                add_conn.close()
        else:
            logger.info("⏭️ additional_school_info.db 없음, 건너뜀")

        master_conn.commit()

        cur = master_conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total = cur.fetchone()[0]
        logger.info(f"🎉 완료! school_master.db에 총 {total}개 학교 정보 저장됨.")

    except Exception as e:
        logger.error(f"치명적 오류: {e}")
        master_conn.rollback()
        sys.exit(1)
    finally:
        master_conn.close()


if __name__ == "__main__":
    main()
