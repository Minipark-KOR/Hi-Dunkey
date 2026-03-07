#!/usr/bin/env python3
"""지역 코드 필터링 유틸리티"""
from typing import List, Optional

# 지역 코드 ↔ 이름 매핑 (수집기와 공유)
REGION_CODES = {
    "B10": "서울", "C10": "부산", "D10": "대구", "E10": "인천",
    "F10": "광주", "G10": "대전", "H10": "울산", "I10": "세종",
    "J10": "경기", "K10": "강원", "L10": "충북", "M10": "충남",
    "N10": "전북", "O10": "전남", "P10": "경북", "Q10": "경남",
    "R10": "제주", "S10": "외국"
}

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
        if item in REGION_CODES.values():
            code = [k for k, v in REGION_CODES.items() if v == item][0]
            result.append(code)
        elif item in REGION_CODES:
            result.append(item)
        else:
            print(f"⚠️ 알 수 없는 지역: {item}")
    return result

def get_region_name(code: str) -> str:
    """지역 코드 → 이름"""
    return REGION_CODES.get(code, code)

def get_all_regions() -> List[tuple]:
    """모든 지역 (코드, 이름) 리스트"""
    return list(REGION_CODES.items())
    