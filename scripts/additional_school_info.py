#!/usr/bin/env python3
# scripts/additional_school_info.py
"""
additional_school_info.db 생성 스크립트 (JSON 파일 처리)
여러 학교 JSON 파일을 읽어 데이터베이스에 저장합니다.
JSON 형식: {"header": [...], "body": [[...]]}
"""
import json
import sqlite3
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent))

from core.kst_time import now_kst
from constants.paths import ADDITIONAL_SCHOOL_INFO_DB_PATH
from constants.schema import SCHEMAS

# ===== 로깅 설정 =====
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG 레벨로 변경하여 상세 로그 확인
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ===== 사용자 설정 =====
JSON_DIR = Path(__file__).parent.parent / "raw_data"
JSON_FILES = [
    "학교기본정보(초)_전체.json",
    "학교기본정보(중)_전체.json",
    "학교기본정보(고)_전체.json",
    "학교기본정보(특)_전체.json",
    "학교기본정보(그)_전체.json",
    "학교기본정보(각)_전체.json",
]
DB_PATH = ADDITIONAL_SCHOOL_INFO_DB_PATH

HEADER_TO_COLUMN = {
    "정보공시 학교코드": "school_code",
    "학교명": "school_name",
    "시도교육청": "atpt_ofcdc_org_nm",
    "교육지원청": "ju_org_nm",
    "지역": "adrcd_nm",
    "학교급코드": "schul_knd_sc_code",
    "설립구분": "fond_sc_code",
    "학교특성": "hs_knd_sc_nm",
    "분교여부": "bnhh_yn",
    "설립유형": "schul_fond_typ_code",
    "주야구분": "dght_sc_code",
    "개교기념일": "foas_memrd",
    "설립일": "fond_ymd",
    "주소내역": "adres_brkdn",
    "상세주소내역": "dtlad_brkdn",
    "우편번호": "zip_code",
    "학교도로명 우편번호": "schul_rdnzc",
    "학교도로명 주소": "schul_rdnma",
    "학교도로명 상세주소": "schul_rdnda",
    "위도": "lttud",
    "경도": "lgtud",
    "전화번호": "user_telno",
    "팩스번호": "perc_faxno",
    "홈페이지 주소": "hmpg_adres",
    "남녀공학 구분": "coedu_sc_code",
    "폐교여부": "absch_yn",
    "폐교일자": "absch_ymd",
    "휴교여부": "close_yn",
}

def parse_float(val):
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None

def load_json(file_path: Path) -> List[Dict[str, Any]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("JSON 파일이 객체(dict) 형식이 아닙니다.")
    header = data.get("header")
    body = data.get("body")
    if not header or not isinstance(header, list):
        raise ValueError("header가 없거나 리스트가 아닙니다.")
    if not body or not isinstance(body, list):
        raise ValueError("body가 없거나 리스트가 아닙니다.")

    # 🔍 헤더 출력 (디버깅)
    logger.debug(f"헤더: {header}")

    rows = []
    for raw_row in body:
        if len(raw_row) != len(header):
            logger.warning(f"헤더 길이({len(header)})와 행 길이({len(raw_row)})가 다릅니다. 해당 행 무시.")
            continue
        row_dict = {
            h: (v.strip() if isinstance(v, str) else v)
            for h, v in zip(header, raw_row)
        }
        rows.append(row_dict)
    return rows

def transform_row(row_dict: Dict[str, Any], schema_columns: List[str]) -> Dict[str, Any]:
    now = now_kst().isoformat()
    record = {}

    # 1. 매핑된 필드 처리
    for header, column in HEADER_TO_COLUMN.items():
        value = row_dict.get(header)
        if isinstance(value, str):
            value = value.strip()
        if value == "":
            value = None
        if column in ("lttud", "lgtud"):
            record[column] = parse_float(value)
        else:
            record[column] = value

        # 🔍 school_code 값 로그 (첫 5개만)
        if column == "school_code" and record["school_code"] is not None:
            logger.debug(f"school_code 샘플: {record['school_code']}")

    # 2. 나머지 필드 기본값
    default_values = {
        "region_code": "",
        "atpt_ofcdc_org_code": None,
        "ju_org_code": None,
        "adrcd_cd": None,
        "lctn_sc_code": None,
        "schul_nm": record.get("school_name"),
        "schul_crse_sc_value": None,
        "schul_crse_sc_value_nm": None,
        "collected_at": now,
        "updated_at": now,
        "is_active": 1,
        "in_neis": 0,
    }
    for col in schema_columns:
        if col not in record:
            record[col] = default_values.get(col, None)

    return record

def init_db(conn: sqlite3.Connection):
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
    logger.info(f"✅ 테이블 준비 완료: {DB_PATH}")

def insert_data(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    schema = SCHEMAS["school_info"]
    columns = [col[0] for col in schema["columns"]]
    placeholders = ','.join(['?' for _ in columns])
    insert_sql = f"INSERT OR IGNORE INTO {schema['table_name']} ({','.join(columns)}) VALUES ({placeholders})"

    # school_code 있는 행만 필터링
    valid_rows = [row for row in rows if row.get("school_code")]
    logger.debug(f"유효한 school_code를 가진 행 수: {len(valid_rows)} / 전체 {len(rows)}")

    if not valid_rows:
        return 0

    data_to_insert = [tuple(row.get(col) for col in columns) for row in valid_rows]
    cur = conn.cursor()
    try:
        cur.executemany(insert_sql, data_to_insert)
        inserted = cur.rowcount
    except Exception as e:
        logger.error(f"배치 삽입 오류: {e}")
        inserted = 0
    return inserted

def main():
    logger.info("🚀 additional_school_info.db 생성 시작")
    if not JSON_DIR.exists():
        logger.error(f"raw_data 디렉토리가 존재하지 않습니다: {JSON_DIR}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    try:
        init_db(conn)
        total_inserted = 0
        for json_file in JSON_FILES:
            file_path = JSON_DIR / json_file
            if not file_path.exists():
                logger.warning(f"파일 없음: {file_path}")
                continue
            logger.info(f"📄 처리 중: {json_file}")
            try:
                data = load_json(file_path)
            except Exception as e:
                logger.error(f"JSON 로드 실패: {e}")
                continue

            logger.info(f"   로드된 행 수: {len(data)}")
            schema_columns = [col[0] for col in SCHEMAS["school_info"]["columns"]]
            transformed = [transform_row(item, schema_columns) for item in data]

            cnt = insert_data(conn, transformed)
            total_inserted += cnt
            logger.info(f"   ✅ {cnt}개 삽입됨 (중복 제외)")

        conn.commit()
        logger.info(f"\n🎉 완료! 총 {total_inserted}개 학교 정보가 additional_school_info.db에 저장됨.")
    except Exception as e:
        logger.error(f"치명적 오류 발생: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()