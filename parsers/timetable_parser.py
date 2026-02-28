#!/usr/bin/env python3
"""
시간표 파서 (과목명, 레벨, 교사)
"""
import re
from typing import Dict, Any

from core.filters import TextFilter, SubjectNameFilter
from core.id_generator import IDGenerator
from constants.codes import DAY_OF_WEEK

def parse_timetable_row(row: dict) -> Dict[str, Any]:
    """
    시간표 한 줄 파싱
    반환:
        {
            "subject_id": int,
            "subject_name": str,      # 원본 과목명 (표시용)
            "normalized_key": str,    # 과목명 정규화 키 (ID 생성용)
            "level": str,             # 레벨 정보 (I, II, 1, 2 등)
            "teacher_id": Optional[int],
            "teacher_name": str,
            "day_of_week": int,
            "period": int,
            "grade": int,
            "class_nm": str,
            "ay": int,
            "semester": int
        }
    """
    # 과목명 (원본)
    subject_raw = row.get("ITRT_CNTNT", "") or row.get("ALL_TI_YMD_NM", "")
    display_name = TextFilter.clean_html(subject_raw)
    
    # 과목명 정규화 및 레벨 추출
    normalized_key = SubjectNameFilter.normalize_for_id(display_name)
    level = SubjectNameFilter.extract_level(display_name)
    
    subject_id = IDGenerator.text_to_int(normalized_key, namespace="subject") if normalized_key else None
    
    # 교사명
    teacher_raw = row.get("TEACHER_CLASS_NM", "") or row.get("TEACHER_NAME", "")
    teacher_display = TextFilter.clean_html(teacher_raw)
    teacher_id = IDGenerator.text_to_int(teacher_display, namespace="teacher") if teacher_display else None
    
    # 요일
    day_kor = row.get("DAY_OF_WEEK", "")
    day_of_week = DAY_OF_WEEK.get(day_kor, 0)
    
    # 교시
    try:
        period = int(row.get("PERIO", "0"))
    except (ValueError, TypeError):
        period = 0
    
    return {
        "subject_id": subject_id,
        "subject_name": display_name,
        "normalized_key": normalized_key,
        "level": level,
        "teacher_id": teacher_id,
        "teacher_name": teacher_display,
        "day_of_week": day_of_week,
        "period": period,
        "grade": int(row.get("GRADE", "0")),
        "class_nm": row.get("CLASS_NM", "").strip(),
        "ay": int(row.get("AY", "0")),
        "semester": int(row.get("SEM", "1"))
    }
    