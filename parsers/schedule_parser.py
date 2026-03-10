#!/usr/bin/env python3
"""
학사일정 파서
"""
from typing import Dict, Any, Optional
from core.filters import TextFilter
from core.id_generator import IDGenerator
from .grade_parser import analyze_grade_display, get_grade_codes


def strip_html(s: Optional[str]) -> str:
    if not s:
        return ""
    return TextFilter.clean_html(s)


def is_special_school(row: dict) -> bool:
    return (
        row.get("SCHUL_KND_NM") in ("특수학교", "특수학급") or
        row.get("SP_EDU_GRADE_EVENT_YN") == "Y"
    )  # ✅ 닫는 괄호 추가


def parse_schedule_row(row: dict, school_info: Optional[dict] = None) -> Dict[str, Any]:
    sc_code = row.get("SD_SCHUL_CODE")
    aa_ymd = row.get("AA_YMD")
    ev_nm_raw = row.get("EVENT_NM", "")
    if not sc_code or not aa_ymd or not ev_nm_raw:
        return {}

    is_sp = school_info.get('is_special', False) if school_info else is_special_school(row)

    ev_nm = strip_html(ev_nm_raw)
    grade_disp, grade_raw = analyze_grade_display(row, is_sp)
    grade_codes = get_grade_codes(row)

    if grade_codes and grade_codes != [0]:
        ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}|{grade_codes[0]}", namespace="schedule")
    else:
        ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}", namespace="schedule")

    sub_nm = (row.get("SBTR_DD_SC_NM") or "").strip()
    return {
        "sc_code":     sc_code,
        "ev_date":     int(aa_ymd),
        "ev_id":       ev_id,
        "ev_nm":       ev_nm,
        "ay":          int(row.get("AY") or aa_ymd[:4]),
        "is_sp":       1 if is_sp else 0,
        "grade_disp":  grade_disp,
        "grade_raw":   grade_raw,
        "grade_codes": grade_codes,
        "sub_yn":      1 if (sub_nm and sub_nm != "해당없음") else 0,
        "sub_code":    sub_nm,
        "dn_yn":       1 if "야간" in (row.get("DGHT_CRSE_SC_NM") or "") else 0,
        "content":     strip_html(row.get("EVENT_CNTNT", "")),
        "load_dt":     row.get("LOAD_DTM") or row.get("LOAD_DT") or "",
    }
#!/usr/bin/env python3
"""
학사일정 파서
"""
import logging
from typing import Dict, Any, Optional

from core.filters import TextFilter
from core.id_generator import IDGenerator
from .grade_parser import analyze_grade_display, get_grade_codes

logger = logging.getLogger(__name__)


def strip_html(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        return TextFilter.clean_html(s)
    except Exception as e:
        logger.error(f"strip_html 오류: {e}")
        return ""


def is_special_school(row: dict) -> bool:
    """
    학교 종류 또는 특수학급 이벤트 여부로 특수학교 여부를 판단합니다.
    """
    return (
        row.get("SCHUL_KND_NM") in ("특수학교", "특수학급") or
        row.get("SP_EDU_GRADE_EVENT_YN") == "Y"
    )


def parse_schedule_row(row: dict, school_info: Optional[dict] = None) -> Dict[str, Any]:
    """
    NEIS 학사일정 API 응답 row를 파싱하여 내부 딕셔너리로 변환합니다.
    """
    if not isinstance(row, dict):
        logger.warning(f"parse_schedule_row: 입력이 dict 아님: {type(row)}")
        return {}

    sc_code = row.get("SD_SCHUL_CODE")
    aa_ymd = row.get("AA_YMD")
    ev_nm_raw = row.get("EVENT_NM", "")
    if not sc_code or not aa_ymd or not ev_nm_raw:
        logger.debug(f"필수 필드 누락: sc_code={sc_code}, aa_ymd={aa_ymd}, ev_nm={ev_nm_raw}")
        return {}

    try:
        is_sp = school_info.get('is_special', False) if school_info else is_special_school(row)
        ev_nm = strip_html(ev_nm_raw)
        grade_disp, grade_raw = analyze_grade_display(row, is_sp)
        grade_codes = get_grade_codes(row)

        if grade_codes and grade_codes != [0]:
            ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}|{grade_codes[0]}", namespace="schedule")
        else:
            ev_id = IDGenerator.text_to_int(f"{ev_nm}|{aa_ymd}", namespace="schedule")

        sub_nm = (row.get("SBTR_DD_SC_NM") or "").strip()

        # 학년도 추출 (AY 필드가 없으면 날짜에서 추출)
        ay_str = row.get("AY") or aa_ymd[:4]
        try:
            ay = int(ay_str)
        except (ValueError, TypeError):
            ay = 0

        load_dt = row.get("LOAD_DTM") or row.get("LOAD_DT") or ""

        return {
            "sc_code": sc_code,
            "ev_date": int(aa_ymd),
            "ev_id": ev_id,
            "ev_nm": ev_nm,
            "ay": ay,
            "is_sp": 1 if is_sp else 0,
            "grade_disp": grade_disp,
            "grade_raw": grade_raw,
            "grade_codes": grade_codes,
            "sub_yn": 1 if (sub_nm and sub_nm != "해당없음") else 0,
            "sub_code": sub_nm,
            "dn_yn": 1 if "야간" in (row.get("DGHT_CRSE_SC_NM") or "") else 0,
            "content": strip_html(row.get("EVENT_CNTNT", "")),
            "load_dt": load_dt,
        }
    except Exception as e:
        logger.error(f"parse_schedule_row 예외: {e}", exc_info=True)
        return {}
        