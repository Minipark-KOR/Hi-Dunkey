#!/usr/bin/env python3
"""
학사일정 파서 (이벤트명, 학년 정보)
"""
import re
from typing import Dict, Any, Optional

from core.filters import TextFilter
from core.id_generator import IDGenerator
from .grade_parser import analyze_grade_display, get_grade_codes

def strip_html(s: Optional[str]) -> str:
    """HTML 태그 제거 및 기본 정규화"""
    if not s:
        return ""
    return TextFilter.clean_html(s)


def is_special_school(row: dict) -> bool:
    """특수학교 여부 판별 (API 응답 기준)"""
    return (
        row.get("SCHUL_KND_NM") in ("특수학교", "특수학급") or
        row.get("SP_EDU_GRADE_EVENT_YN") == "Y"
    )


def parse_schedule_row(row: dict, school_info: Optional[dict] = None) -> Dict[str, Any]:
    """
    학사일정 한 줄 파싱
    school_info: 학교정보 캐시에서 가져온 dict (is_special 포함 가능)
    """
    sc_code = row.get("SD_SCHUL_CODE")
    aa_ymd = row.get("AA_YMD")
    ev_nm_raw = row.get("EVENT_NM", "")
    
    if not sc_code or not aa_ymd or not ev_nm_raw:
        return {}
    
    # 학교정보에서 특수학교 여부 우선 사용
    if school_info:
        is_sp = school_info.get('is_special', False)
    else:
        is_sp = is_special_school(row)
    
    ev_nm = strip_html(ev_nm_raw)
    grade_disp, grade_raw = analyze_grade_display(row, is_sp)
    grade_codes = get_grade_codes(row)
    
    # 이벤트 ID 생성 (학년 코드가 있으면 학년 포함)
    if grade_codes and grade_codes != [0]:
        # 학년별로 다른 ID 부여 (첫 번째 학년 코드 사용)
        ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}|{grade_codes[0]}", namespace="schedule")
    else:
        ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}", namespace="schedule")
    
    sub_nm = (row.get("SBTR_DD_SC_NM") or "").strip()
    
    return {
        "sc_code": sc_code,
        "ev_date": int(aa_ymd),
        "ev_id": ev_id,
        "ev_nm": ev_nm,
        "ay": int(row.get("AY") or aa_ymd[:4]),
        "is_sp": 1 if is_sp else 0,
        "grade_disp": grade_disp,
        "grade_raw": grade_raw,
        "grade_codes": grade_codes,
        "sub_yn": 1 if (sub_nm and sub_nm != "해당없음") else 0,
        "sub_code": sub_nm,
        "dn_yn": 1 if "야간" in (row.get("DGHT_CRSE_SC_NM") or "") else 0,
        "content": strip_html(row.get("EVENT_CNTNT", "")),
        "load_dt": row.get("LOAD_DTM") or row.get("LOAD_DT") or ""
    }
    