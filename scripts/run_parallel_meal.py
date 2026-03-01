#!/usr/bin/env python3
"""
급식 병렬 수집 실행 스크립트 (공통 병렬 유틸리티 사용)
- --year 인자로 학년도 지정 가능 (기본: 현재 학년도)
"""
import sys
import os
import argparse
from multiprocessing import Pool

# 프로젝트 루트를 path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.parallel import setup_worker_pool
from core.school_year import get_current_school_year
from core.kst_time import now_kst
from collectors.annual_full_meal_collector import AnnualFullMealCollector

# 실행할 조합 정의: (region, shard, school_range)
COMBINATIONS = [
    # 경기 (J10) - 4개 조합
    ("J10", "odd", "A"),
    ("J10", "odd", "B"),
    ("J10", "even", "A"),
    ("J10", "even", "B"),
    # 서울 (B10) - 2개 조합
    ("B10", "odd", None),
    ("B10", "even", None),
    # 부산 (C10) - 2개 조합
    ("C10", "odd", None),
    ("C10", "even", None),
    # 인천 (E10) - 2개 조합
    ("E10", "odd", None),
    ("E10", "even", None),
    # 대구 (D10) - 2개 조합
    ("D10", "odd", None),
    ("D10", "even", None),
    # 광주 (F10) - 1개
    ("F10", "none", None),
    # 대전 (G10) - 1개
    ("G10", "none", None),
    # 울산 (H10) - 1개
    ("H10", "none", None),
    # 세종 (I10) - 1개
    ("I10", "none", None),
    # 강원 (K10) - 1개
    ("K10", "none", None),
    # 충북 (M10) - 1개
    ("M10", "none", None),
    # 충남 (N10) - 1개
    ("N10", "none", None),
    # 전북 (P10) - 1개
    ("P10", "none", None),
    # 전남 (Q10) - 1개
    ("Q10", "none", None),
    # 경북 (R10) - 1개
    ("R10", "none", None),
    # 경남 (S10) - 1개
    ("S10", "none", None),
    # 제주 (T10) - 1개
    ("T10", "none", None),
]

DEBUG = True  # 디버그 모드

def run_collector(args):
    """단일 collector 프로세스 실행"""
    region, shard, school_range, year = args
    collector = AnnualFullMealCollector(
        shard=shard,
        school_range=school_range,
        debug_mode=DEBUG
    )
    try:
        collector.fetch_year(region, year)
    finally:
        collector.close()
    return 0

def main():
    parser = argparse.ArgumentParser(description="급식 병렬 수집")
    parser.add_argument("--year", type=int,
                        default=get_current_school_year(now_kst()),
                        help="수집할 학년도 (기본: 현재 학년도)")
    args = parser.parse_args()
    year = args.year

    total_jobs = len(COMBINATIONS)
    # 각 조합에 year를 추가하여 전달
    tasks = [(r, s, rg, year) for (r, s, rg) in COMBINATIONS]

    with setup_worker_pool(total_jobs) as pool:
        pool.map(run_collector, tasks)

    # 병합 실행
    print("🔄 샤드 병합 시작...")
    from scripts.merge_meal_dbs import merge_databases
    merge_databases(do_consolidate_vocab=True)
    print("✅ 병합 완료")

if __name__ == "__main__":
    main()
    