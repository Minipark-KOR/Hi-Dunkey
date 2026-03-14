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
from pathlib import Path
from typing import List, Dict, Any

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent))

from core.kst_time import now_kst
from constants.paths import ADDITIONAL_SCHOOL_INFO_DB_PATH
from constants.schema import SCHEMAS

# ===== 사용자 설정 =====
# JSON 파일이 위치한 디렉토리
JSON_DIR = Path(__file__).parent.parent / "raw_data"
# 처리할 JSON 파일 목록 (실제 파일명 그대로)
JSON_FILES = [
    "학교기본정보(초)_전체.json",
    "학교기본정보(중)_전체.json",
    "학교기본정보(고)_전체.json",
    "학교기본정보(특)_전체.json",
    "학교기본정보(그)_전체.json",
    "학교기본정보(각)_전체.json",
]
# ======================

DB_PATH = ADDITIONAL_SCHOOL_INFO_DB_PATH

# JSON 헤더(키)와 school_info 테이블 컬럼 간 매핑
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
    """문자열을 실수로 변환 (None 또는 빈 값이면 None 반환)"""
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None

def load_json(file_path: Path) -> List[Dict[str, Any]]:
    """
    JSON 파일을 읽어 리스트(딕셔너리)로 반환합니다.
    예상 구조: {"header": [...], "body": [[...]]}
    """
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
    
    rows = []
    for raw_row in body:   # 피드백 반영: 변수명 raw_row로 변경
        if len(raw_row) != len(header):
            print(f"⚠️ 헤더 길이({len(header)})와 행 길이({len(raw_row)})가 다릅니다. 해당 행 무시.")
            continue
        # zip을 사용하여 헤더와 값을 쌍으로 묶고, 문자열이면 strip() 처리
        row_dict = {
            h: (v.strip() if isinstance(v, str) else v)
            for h, v in zip(header, raw_row)
        }
        rows.append(row_dict)
    return rows

def transform_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSON 객체 하나를 schools_info 테이블 레코드로 변환합니다.
    (load_json에서 이미 strip 처리했으므로 여기서는 추가 strip 없음)
    """
    now = now_kst().isoformat()
    record = {}

    # 1. HEADER_TO_COLUMN에 매핑된 필드 처리
    for header, column in HEADER_TO_COLUMN.items():
        value = row_dict.get(header)
        # load_json에서 이미 문자열은 strip했으므로 추가 strip 불필요
        if value == "":
            value = None
        if column in ("lttud", "lgtud"):
            record[column] = parse_float(value)
        else:
            record[column] = value

    # 2. JSON에 존재하지 않는 필드 (school_info 스키마의 나머지 컬럼) 기본값 할당
    record["region_code"] = None
    record["atpt_ofcdc_org_code"] = None
    record["ju_org_code"] = None
    record["adrcd_cd"] = None
    record["lctn_sc_code"] = None
    record["schul_nm"] = record.get("school_name")  # 학교명 중복 저장
    record["schul_crse_sc_value"] = None
    record["schul_crse_sc_value_nm"] = None
    record["collected_at"] = now
    record["updated_at"] = now
    record["is_active"] = 1
    record["in_neis"] = 0

    return record

def init_db():
    """데이터베이스와 테이블을 생성합니다."""
    schema = SCHEMAS["school_info"]
    conn = sqlite3.connect(str(DB_PATH))

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
    print(f"✅ 테이블 준비 완료: {DB_PATH}")

def insert_data(rows: List[Dict[str, Any]]) -> int:
    """
    변환된 레코드들을 DB에 배치 삽입합니다. (executemany 사용)
    school_code 중복 시 무시(INSERT OR IGNORE).
    """
    schema = SCHEMAS["school_info"]
    columns = [col[0] for col in schema["columns"]]
    placeholders = ','.join(['?' for _ in columns])
    insert_sql = f"INSERT OR IGNORE INTO {schema['table_name']} ({','.join(columns)}) VALUES ({placeholders})"

    # 유효한 레코드만 필터링 (school_code 존재)
    valid_rows = [row for row in rows if row.get("school_code")]
    if not valid_rows:
        return 0

    # 모든 데이터를 튜플 리스트로 준비
    data_to_insert = [tuple(row.get(col) for col in columns) for row in valid_rows]

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    try:
        cur.executemany(insert_sql, data_to_insert)
        conn.commit()
        # executemany 후 rowcount는 전체 영향을 받은 행 수 (OR IGNORE 포함)
        inserted = cur.rowcount
    except Exception as e:
        print(f"⚠️ 배치 삽입 오류: {e}")
        inserted = 0
    finally:
        conn.close()
    return inserted

def main():
    print("🚀 additional_school_info.db 생성 시작")
    init_db()
    total_inserted = 0
    for json_file in JSON_FILES:
        file_path = JSON_DIR / json_file
        if not file_path.exists():
            print(f"⚠️ 파일 없음: {file_path}")
            continue
        print(f"\n📄 처리 중: {json_file}")
        try:
            data = load_json(file_path)
        except Exception as e:
            print(f"   ❌ JSON 로드 실패: {e}")
            continue
        print(f"   로드된 행 수: {len(data)}")
        transformed = [transform_row(item) for item in data]
        # school_code 없는 행은 insert_data 내부에서 걸러짐
        cnt = insert_data(transformed)
        total_inserted += cnt
        print(f"   ✅ {cnt}개 삽입됨 (중복 제외)")
    print(f"\n🎉 완료! 총 {total_inserted}개 학교 정보가 additional_school_info.db에 저장됨.")
    print("📌 다음 단계: 이 DB와 기존 school_info.db를 병합하여 school_master.db를 생성할 수 있습니다.")

if __name__ == "__main__":
    main()
    