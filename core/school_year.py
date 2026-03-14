#!/usr/bin/env python3
"""
학년도 계산 유틸리티 (3월~2월 기준)
"""
from datetime import datetime
from .kst_time import now_kst

def get_current_school_year(dt: datetime = None) -> int:
    """
    현재 학년도 반환
    - 2026년 3월~12월: 2026학년도
    - 2027년 1월~2월: 2026학년도
    """
    if dt is None:
        dt = now_kst()
    if dt.month >= 3:
        return dt.year
    else:
        return dt.year - 1

def get_previous_school_year() -> int:
    """작년 학년도 반환"""
    return get_current_school_year() - 1

def get_school_year_range(year: int) -> tuple:
    """
    특정 학년도의 시작일/종료일 반환 (YYYYMMDD 문자열)
    예: 2026 → ('20260301', '20270228')
    """
    start = f"{year}0301"
    end = f"{year+1}0228"
    # 간단히 2월 28일로 고정 (윤년 무시)
    return start, end
    