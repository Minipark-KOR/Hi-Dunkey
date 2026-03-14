#!/usr/bin/env python3
"""
neis_raw.db의 원시 데이터를 읽어 정제/보강하여 neis_enriched.db에 저장합니다.
"""
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Any

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.kst_time import now_kst
from core.school.id import create_school_id
from core.data.meta_vocab import MetaVocabManager
from core.school.address.address_filter import AddressFilter
from constants.paths import (
    NEIS_RAW_DB_PATH,
    NEIS_ENRICHED_DB_PATH,
    GLOBAL_VOCAB_DB_PATH,
)
from constants.schema import SCHEMAS
from core.util.manage_log import build_domain_logger

logger = build_domain_logger("enrich_neis_info", "misc", __file__)

LEVEL_GEOCODING = 3

def safe_str(value):
    return value.strip() if value else ""

def parse_float(val):
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None

def init_db():
    schema = SCHEMAS["neis_info"]
    conn = sqlite3.connect(str(NEIS_ENRICHED_DB_PATH))
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
    conn.close()
    logger.info(f"✅ 테이블 준비 완료: {NEIS_ENRICHED_DB_PATH}")

def process_row(row: Dict[str, Any], vocab_mgr: MetaVocabManager) -> Dict[str, Any]:
    now = now_kst().isoformat()
    run_date = now[:10].replace("-", "")

    sc_code = row["sc_code"]
    atpt_code = row["atpt_code"]
    full_address = row.get("address") or ""

    cleaned = None
    jibun = None
    if full_address:
        cleaned = AddressFilter.clean(full_address, level=LEVEL_GEOCODING)
        jibun = AddressFilter.extract_jibun(full_address)

    address_hash = AddressFilter.hash(full_address) if full_address else ""

    lat = parse_float(row.get("latitude"))
    lon = parse_float(row.get("longitude"))

    addr_ids = {}
    if cleaned:
        try:
            addr_ids = vocab_mgr.save_address(cleaned)
        except Exception as e:
            logger.error(f"주소 변환 실패 {sc_code}: {e}")

    try:
        school_id = create_school_id(atpt_code, sc_code)
    except Exception as e:
        logger.error(f"school_id 생성 실패 {sc_code}: {e}")
        school_id = None

    record = dict(row)
    record.update({
        "school_id": school_id,
        "cleaned_address": cleaned or "",
        "address_hash": address_hash,
        "latitude": lat,
        "longitude": lon,
        "geocode_attempts": 0,
        "last_error": None,
        "city_id": addr_ids.get("city_id", 0),
        "district_id": addr_ids.get("district_id", 0),
        "street_id": addr_ids.get("street_id", 0),
        "number_type": addr_ids.get("number_type"),
        "number_value": addr_ids.get("number"),
        "number_start": addr_ids.get("number_start"),
        "number_end": addr_ids.get("number_end"),
        "number_bit": addr_ids.get("number_bit", 0),
        "jibun_address": jibun,
        "kakao_address": None,
        "load_dt": now,
    })
    return record

def main():
    logger.info("🚀 NEIS 데이터 보강(Enrich) 시작")

    if not NEIS_RAW_DB_PATH.exists():
        logger.error(f"neis_raw.db 없음: {NEIS_RAW_DB_PATH}")
        sys.exit(1)

    vocab_mgr = MetaVocabManager(str(GLOBAL_VOCAB_DB_PATH), debug=False)
    init_db()

    # 원시 DB 연결
    src_conn = sqlite3.connect(str(NEIS_RAW_DB_PATH))
    src_conn.row_factory = sqlite3.Row
    src_cur = src_conn.cursor()
    src_cur.execute("SELECT * FROM schools_neis")
    rows = src_cur.fetchall()
    total = len(rows)
    logger.info(f"📄 읽은 원시 데이터: {total}건")

    # 대상 DB 연결
    dst_conn = sqlite3.connect(str(NEIS_ENRICHED_DB_PATH))
    dst_cur = dst_conn.cursor()

    schema = SCHEMAS["neis_info"]
    columns = [col[0] for col in schema["columns"]]
    placeholders = ','.join(['?' for _ in columns])
    insert_sql = f"INSERT OR IGNORE INTO {schema['table_name']} ({','.join(columns)}) VALUES ({placeholders})"

    processed = 0
    inserted = 0
    batch = []
    batch_size = 500

    for i, row in enumerate(rows, 1):
        row_dict = dict(row)
        try:
            record = process_row(row_dict, vocab_mgr)
            batch.append(tuple(record.get(col) for col in columns))
            processed += 1
        except Exception as e:
            logger.error(f"행 처리 오류 (sc_code={row_dict.get('sc_code')}): {e}")
            continue

        if len(batch) >= batch_size or i == total:
            try:
                dst_cur.executemany(insert_sql, batch)
                inserted += dst_cur.rowcount
                dst_conn.commit()
            except Exception as e:
                logger.error(f"배치 삽입 오류: {e}")
                dst_conn.rollback()
            batch.clear()

        if i % 1000 == 0 or i == total:
            logger.info(f"진행률: {i}/{total} (처리됨: {processed}, 삽입됨: {inserted})")

    src_conn.close()
    dst_conn.close()
    vocab_mgr.close()

    logger.info(f"🎉 완료! 총 {inserted}개 학교 정보가 neis_enriched.db에 저장됨.")

if __name__ == "__main__":
    main()
    