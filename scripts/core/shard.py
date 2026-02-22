#!/usr/bin/env python3
"""
샤딩 유틸리티 (홀수/짝수 학교 코드 기준)
"""
def get_shard_from_school_code(school_code: str) -> str:
    """
    학교 코드의 마지막 자리로 샤드 결정
    - 짝수: even, 홀수: odd
    """
    try:
        last_digit = int(str(school_code)[-1])
        return "even" if last_digit % 2 == 0 else "odd"
    except (ValueError, TypeError, IndexError):
        return "odd"   # 기본값

def should_include(shard: str, school_code: str) -> bool:
    """
    해당 샤드에 포함되어야 하는지 확인
    - shard == "none": 항상 True
    - 그 외: get_shard_from_school_code 결과와 일치해야 True
    """
    if shard == "none":
        return True
    return get_shard_from_school_code(school_code) == shard
    