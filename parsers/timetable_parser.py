#!/usr/bin/env python3
"""
시간표 파서
"""
import logging
from typing import Dict, Any

from core.filters import TextFilter
from core.id_generator import IDGenerator
from constants.codes import DAY_OF_WEEK

# SubjectNameFilter 임포트 시도 (없으면 TextFilter 사용)
try:
    from core.text_filter import SubjectNameFilter
except ImportError:
    from core.filters import TextFilter as SubjectNameFilter

logger = logging.getLogger(__name__)


def parse_timetable_row(row: dict) -> Dict[str, Any]:
    """
    NEIS 시간표 API 응답 row를 파싱하여 내부 딕셔너리로 변환합니다.
    """
    if not isinstance(row, dict):
        logger.warning(f"parse_timetable_row: 입력이 dict 아님: {type(row)}")
        return {}

    subject_raw = row.get("ITRT_CNTNT", "") or row.get("ALL_TI_YMD_NM", "")
    display_name = TextFilter.clean_html(subject_raw)
    
    try:
        normalized_key = SubjectNameFilter.normalize_for_id(display_name)
        level = SubjectNameFilter.extract_level(display_name)
    except Exception as e:
        logger.error(f"과목명 정규화 오류: {e} (display_name={display_name})")
        normalized_key = ""
        level = ""

    subject_id = None
    if normalized_key:
        subject_id = IDGenerator.text_to_int(normalized_key, namespace="subject")

    teacher_raw = row.get("TEACHER_CLASS_NM", "") or row.get("TEACHER_NAME", "")
    teacher_display = TextFilter.clean_html(teacher_raw)
    teacher_id = IDGenerator.text_to_int(teacher_display, namespace="teacher") if teacher_display else None

    day_kor = row.get("DAY_OF_WEEK", "")
    day_of_week = DAY_OF_WEEK.get(day_kor, 0)

    try:
        period = int(row.get("PERIO", "0"))
    except (ValueError, TypeError) as e:
        logger.debug(f"period 변환 오류: {e}, 기본값 0 사용")
        period = 0

    try:
        grade = int(row.get("GRADE", "0"))
    except (ValueError, TypeError):
        grade = 0

    try:
        ay = int(row.get("AY", "0"))
    except (ValueError, TypeError):
        ay = 0

    try:
        semester = int(row.get("SEM", "1"))
    except (ValueError, TypeError):
        semester = 1

    return {
        "subject_id": subject_id,
        "subject_name": display_name,
        "normalized_key": normalized_key,
        "level": level,
        "teacher_id": teacher_id,
        "teacher_name": teacher_display,
        "day_of_week": day_of_week,
        "period": period,
        "grade": grade,
        "class_nm": row.get("CLASS_NM", "").strip(),
        "ay": ay,
        "semester": semester
    }
    