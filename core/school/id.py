#!/usr/bin/env python3
"""
학교 ID 생성/관리 (교육청 코드 + 학교 코드 → 32비트 정수)
"""
from typing import Optional

# 교육청 코드 → 1바이트 매핑
REGION_CODE = {
    'B10': 0x01, 'C10': 0x02, 'D10': 0x03, 'E10': 0x04,
    'F10': 0x05, 'G10': 0x06, 'H10': 0x07, 'I10': 0x08,
    'J10': 0x09, 'K10': 0x0A, 'L10': 0x0B, 'M10': 0x0C,
    'N10': 0x0D, 'P10': 0x0E, 'Q10': 0x0F, 'R10': 0x10,
    'S10': 0x11, 'T10': 0x12,
}

REGION_REVERSE = {v: k for k, v in REGION_CODE.items()}

def create_school_id(region_code: str, school_code: str) -> int:
    """
    교육청 코드 + 학교 코드 → 32비트 정수 ID
    - 상위 8비트: 교육청 코드 (1바이트)
    - 하위 24비트: 학교 코드 (최대 16,777,215)
    """
    region_byte = REGION_CODE.get(region_code, 0)
    try:
        school_num = int(school_code) & 0xFFFFFF  # 24비트 제한
    except (ValueError, TypeError):
        school_num = 0
    return (region_byte << 24) | school_num

def extract_region_code(school_id: int) -> str:
    """school_id에서 교육청 코드 추출"""
    region_byte = (school_id >> 24) & 0xFF
    return REGION_REVERSE.get(region_byte, '')

def extract_school_code(school_id: int) -> str:
    """school_id에서 학교 코드 추출 (7자리 문자열)"""
    school_num = school_id & 0xFFFFFF
    return str(school_num).zfill(7)

def get_region_range(region_code: str) -> tuple:
    """
    특정 교육청의 school_id 범위 반환 (검색 최적화)
    (start_id, end_id)
    """
    region_byte = REGION_CODE.get(region_code, 0)
    start = region_byte << 24
    end = ((region_byte + 1) << 24) - 1
    return start, end
    