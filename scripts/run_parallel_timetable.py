#!/usr/bin/env python3
"""
시간표 병렬 수집 실행 스크립트 (공통 병렬 유틸리티 사용)
- --year, --semester 인자 지정 가능
"""
import sys
import os
import argparse
from multiprocessing import Pool

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.parallel import setup_worker_pool
from core.school_year import get_current_school_year
from core.kst_time import now_kst
from collectors.timetable_collector import AnnualFullTimetableCollector

COMBINATIONS = [
    ("J10", "odd", "A"),
    ("J10", "odd", "B"),
    ("J10", "even", "A"),
    ("J10", "even", "B"),
    ("B10", "odd", None),
    ("B10", "even", None),
    ("C10", "odd", None),
    ("C10", "even", None),
    ("E10", "odd", None),
    ("E10", "even", None),
    ("D10", "odd", None),
    ("D10", "even", None),
    ("F10", "none", None),
    ("G10", "none", None),
    ("H10", "none", None),
    ("I10", "none", None),
    ("K10", "none", None),
    ("M10", "none", None),
    ("N10", "none", None),
    ("P10", "none", None),
    ("Q10", "none", None),
    ("R10", "none", None),
    ("S10", "none", None),
    ("T10", "none", None),
]

DEBUG = True

def run_collector(args):
    region, shard, school_range, year, semester = args
    collector = AnnualFullTimetableCollector(
        shard=shard,
        school_range=school_range,
        debug_mode=DEBUG
    )
    try:
        collector.fetch_year(region, year, semester)
    finally:
        collector.close()
    return 0

def main():
    parser = argparse.ArgumentParser(description="시간표 병렬 수집")
    parser.add_argument("--year", type=int,
                        default=get_current_school_year(now_kst()),
                        help="수집할 학년도 (기본: 현재 학년도)")
    parser.add_argument("--semester", type=int, default=1, choices=[1,2],
                        help="학기 (1 또는 2, 기본: 1)")
    args = parser.parse_args()
    year = args.year
    semester = args.semester

    total_jobs = len(COMBINATIONS)
    tasks = [(r, s, rg, year, semester) for (r, s, rg) in COMBINATIONS]

    with setup_worker_pool(total_jobs) as pool:
        pool.map(run_collector, tasks)

    print("🔄 샤드 병합 시작...")
    from scripts.merge_timetable_dbs import merge_databases
    merge_databases(do_consolidate_vocab=True)
    print("✅ 병합 완료")

if __name__ == "__main__":
    main()
    