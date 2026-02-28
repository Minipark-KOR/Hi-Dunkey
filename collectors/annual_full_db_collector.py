#!/usr/bin/env python3
"""
전체 데이터 수집기 (기존 데이터를 모두 새로 수집)
- 사용법: python collectors/full_collector.py --year 2026
"""
import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kst_time import now_kst
from core.school_year import get_current_school_year
from collectors.school_master_collector import SchoolMasterCollector
from collectors.meal_collector import MealCollector
from collectors.timetable_collector import TimetableCollector
from collectors.schedule_collector import ScheduleCollector
from constants.codes import ALL_REGIONS


def full_collect(year: int):
    """전체 수집 실행 (각 Collector에 full=True 전달)"""
    print(f"🚀 전체 데이터 수집 시작 (학년도: {year})")

    # school master (항상 전체 수집)
    print("🏫 학교 기본정보 수집 중...")
    school = SchoolMasterCollector(shard="none", full=True)
    for region in ALL_REGIONS:
        school.fetch_region(region)
    school.close()

    # meal
    print("🍽️ 급식 정보 수집 중...")
    meal = MealCollector(shard="none", full=True)
    for region in ALL_REGIONS:
        meal.fetch_region(region)  # fetch_region이 일일 단위? 실제로는 fetch_daily 등을 사용해야 함
    meal.close()

    # timetable (1,2학기)
    print("📚 시간표 수집 중...")
    tt = TimetableCollector(shard="none", full=True)
    for region in ALL_REGIONS:
        tt.fetch_region(region, year, 1)
        tt.fetch_region(region, year, 2)
    tt.close()

    # schedule
    print("📅 학사일정 수집 중...")
    schedule = ScheduleCollector(shard="none", full=True)
    for region in ALL_REGIONS:
        schedule.fetch_region(region, year)
    schedule.close()

    print("✅ 전체 수집 완료")


def main():
    parser = argparse.ArgumentParser(description="전체 데이터 수집 (기존 데이터 초기화)")
    parser.add_argument("--year", type=int, default=get_current_school_year(),
                        help="수집할 학년도 (기본: 현재 학년도)")
    args = parser.parse_args()
    full_collect(args.year)


if __name__ == "__main__":
    main()
    