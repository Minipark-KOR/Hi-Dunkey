#!/usr/bin/env python3
"""지역 코드 필터링 유틸리티"""
from typing import List

from constants.codes import REGION_NAMES

REGION_CODES = REGION_NAMES.copy()
REGION_NAMES_TO_CODE = {name: code for code, name in REGION_NAMES.items()}

def parse_region_input(region_input: str) -> List[str]:
    """
    사용자 입력을 지역 코드 리스트로 파싱
    예: "서울,경기" → ["B10", "J10"]
        "B10,C10" → ["B10", "C10"]
    """
    if not region_input:
        return []
    result = []
    for item in region_input.split(','):
        item = item.strip()
        if item in REGION_NAMES_TO_CODE:
            result.append(REGION_NAMES_TO_CODE[item])
        elif item in REGION_CODES:
            result.append(item)
        else:
            print(f"⚠️ 알 수 없는 지역: {item}")
    return result

def get_region_name(code: str) -> str:
    """지역 코드 → 이름"""
    return REGION_NAMES.get(code, code)

def get_all_regions() -> List[tuple]:
    """모든 지역 (코드, 이름) 리스트"""
    return list(REGION_NAMES.items())
    