#!/usr/bin/env python3
"""
샤딩 + 범위 필터 공통 로직 (확장 버전)
- BaseCollector에서 사용하는 모든 필터링 함수 포함
"""

def get_shard(school_code: str) -> str:
    """학교 코드로 샤드 결정 (odd/even)"""
    if not school_code:
        return "odd"
    try:
        last_digit = int(school_code[-1])
        return "even" if last_digit % 2 == 0 else "odd"
    except (ValueError, TypeError, IndexError):
        return "odd"


def get_shard_group(school_code: str) -> str:
    """학교 코드로 상세 샤드 그룹 결정 (0-3, 4-7, 8-9)"""
    if not school_code:
        return "group1"
    try:
        last_digit = int(school_code[-1])
        if 0 <= last_digit <= 3:
            return "group1"
        elif 4 <= last_digit <= 7:
            return "group2"
        else:  # 8-9
            return "group3"
    except (ValueError, TypeError, IndexError):
        return "group1"


def get_range_group(school_code: str) -> str:
    """학교 코드 첫 자리로 범위 그룹 결정 (A:1-3, B:4-6, C:7-9, 0)"""
    if not school_code:
        return "A"
    try:
        first_digit = int(school_code[0])
        if first_digit == 0:
            return "Z"  # 0은 특별 케이스
        elif 1 <= first_digit <= 3:
            return "A"
        elif 4 <= first_digit <= 6:
            return "B"
        else:  # 7-9
            return "C"
    except (ValueError, TypeError, IndexError):
        return "A"


def should_include_shard(shard: str, school_code: str) -> bool:
    """
    샤드 필터
    - none: 항상 True
    - odd/even: 홀수/짝수
    - group1/group2/group3: 마지막 자리 그룹 (0-3, 4-7, 8-9)
    """
    if shard == "none":
        return True
    elif shard in ["odd", "even"]:
        return get_shard(school_code) == shard
    elif shard in ["group1", "group2", "group3"]:
        return get_shard_group(school_code) == shard
    else:
        return True  # 알 수 없는 shard 값


def should_include_range(range_code: str | None, school_code: str) -> bool:
    """
    범위 필터
    - None 또는 "none": 항상 True
    - "A": 첫 자리 1-4 (기존 호환성)
    - "B": 첫 자리 5-9 (기존 호환성)
    - "A1": 첫 자리 1-3
    - "B1": 첫 자리 4-6
    - "C1": 첫 자리 7-9
    - "Z": 첫 자리 0
    - "low": 첫 자리 1-3
    - "mid": 첫 자리 4-6
    - "high": 첫 자리 7-9
    - "zero": 첫 자리 0
    """
    if range_code is None or range_code == "none":
        return True

    try:
        first_digit = int(school_code[0])

        # 기존 호환성 (A/B)
        if range_code == "A":
            return first_digit <= 4
        elif range_code == "B":
            return first_digit >= 5

        # 세분화된 범위
        elif range_code in ["A1", "low"]:
            return 1 <= first_digit <= 3
        elif range_code in ["B1", "mid"]:
            return 4 <= first_digit <= 6
        elif range_code in ["C1", "high"]:
            return 7 <= first_digit <= 9
        elif range_code in ["Z", "zero"]:
            return first_digit == 0

        # 숫자 범위 직접 지정 (예: "1-3", "4-6", "7-9")
        elif "-" in range_code:
            start, end = map(int, range_code.split("-"))
            return start <= first_digit <= end

        return True

    except (ValueError, TypeError, IndexError):
        return True


def should_include_school(shard: str, range_code: str | None, school_code: str) -> bool:
    """
    샤드 + 범위 조합 필터 (BaseCollector에서 호출)
    - range_code가 None이면 샤드만 검사
    - range_code가 있으면 샤드와 범위 모두 검사
    """
    # 샤드 검사
    if not should_include_shard(shard, school_code):
        return False

    # 범위 검사 (range_code가 None이면 항상 True)
    return should_include_range(range_code, school_code)


# --------------------------------------------------------
# 유틸리티 함수
# --------------------------------------------------------
def get_school_partition_info(school_code: str) -> dict:
    """학교 코드의 파티션 정보를 dict로 반환"""
    return {
        "school_code": school_code,
        "shard": get_shard(school_code),
        "shard_group": get_shard_group(school_code),
        "range_group": get_range_group(school_code),
        "first_digit": school_code[0] if school_code else None,
        "last_digit": school_code[-1] if school_code else None
    }


def filter_schools_by_shard_and_range(school_codes: list, shard: str, range_code: str | None) -> list:
    """학교 코드 리스트를 샤드와 범위로 필터링"""
    return [
        code for code in school_codes
        if should_include_school(shard, range_code, code)
    ]


def get_shard_distribution(school_codes: list) -> dict:
    """학교 코드 리스트의 샤드 분포 통계"""
    distribution = {
        "odd": 0,
        "even": 0,
        "group1": 0,
        "group2": 0,
        "group3": 0,
        "total": len(school_codes)
    }

    for code in school_codes:
        distribution[get_shard(code)] += 1
        distribution[get_shard_group(code)] += 1

    return distribution