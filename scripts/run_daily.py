#!/usr/bin/env python3
"""
매일 실행되는 수집 스크립트 (Hot50 위주, 스케줄 기반)
"""
import os
import sys
import subprocess
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.logger import build_logger
from core.kst_time import now_kst, get_kst_time
from core.school_year import get_current_school_year
from baskets.update_hot import get_hot_schools

logger = build_logger("run_daily", "../logs/run_daily.log")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_collector(script, args, desc):
    logger.info(f"🚀 {desc}")
    cmd = [sys.executable, script] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"❌ {desc} 실패: {result.stderr}")
    else:
        logger.info(f"✅ {desc} 완료")
    time.sleep(5)

def run_meal_daily():
    """매일 06:10 KST: Hot50 학교 급식 수집"""
    hot_schools = get_hot_schools(limit=50)
    if hot_schools:
        regions = ",".join(hot_schools)
        logger.info(f"🍽️ 급식 일일 수집 (Hot50: {len(hot_schools)}개)")
    else:
        regions = "ALL"
        logger.warning("Hot50 목록이 비어 있어 전체 지역 수집으로 fallback")
    
    run_collector(
        "collectors/meal_collector.py",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        f"급식 odd (incremental) - regions: {regions}"
    )
    run_collector(
        "collectors/meal_collector.py",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        f"급식 even (incremental) - regions: {regions}"
    )

def run_meal_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"🍽️ 급식 월간 전체 수집 (day={day})")
    run_collector(
        "collectors/meal_collector.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "급식 odd (full)"
    )
    run_collector(
        "collectors/meal_collector.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--full"],
        "급식 even (full)"
    )

def run_schedule_daily():
    """매일 06:30 KST: Hot50 학교 학사일정 수집"""
    hot_schools = get_hot_schools(limit=50)
    if hot_schools:
        regions = ",".join(hot_schools)
        logger.info(f"📅 학사일정 일일 수집 (Hot50: {len(hot_schools)}개)")
    else:
        regions = "ALL"
        logger.warning("Hot50 목록이 비어 있어 전체 지역 수집으로 fallback")
    
    run_collector(
        "collectors/schedule_collector.py",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        f"학사일정 odd (incremental) - regions: {regions}"
    )
    run_collector(
        "collectors/schedule_collector.py",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        f"학사일정 even (incremental) - regions: {regions}"
    )

def run_schedule_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"📅 학사일정 월간 전체 수집 (day={day})")
    run_collector(
        "collectors/schedule_collector.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "학사일정 odd (full)"
    )
    run_collector(
        "collectors/schedule_collector.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--full"],
        "학사일정 even (full)"
    )

def run_timetable():
    """3,4,9,10월 월/수 06:50 KST: 전체 학교 시간표 수집"""
    now = now_kst()
    if now.month in [3,4,9,10] and now.weekday() in [0,2]:
        logger.info("📚 시간표 수집")
        ay = get_current_school_year()
        run_collector(
            "collectors/timetable_collector.py",
            ["--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "odd", "--full"],
            "시간표 odd (full)"
        )
        run_collector(
            "collectors/timetable_collector.py",
            ["--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "even", "--full"],
            "시간표 even (full)"
        )

def run_master():
    """매월 1일 07:10 KST: 학교정보 전체 수집 (변경분만)"""
    if now_kst().day == 1:
        logger.info("🏫 학교정보 월간 수집")
        run_collector(
            "collectors/school_info_collector.py",
            ["--regions", "ALL", "--shard", "odd", "--incremental", "--compare"],
            "학교정보 odd (incremental+compare)"
        )
        run_collector(
            "collectors/school_info_collector.py",
            ["--regions", "ALL", "--shard", "even", "--incremental", "--compare"],
            "학교정보 even (incremental+compare)"
        )
        run_collector(
            "scripts/merge_school_info_dbs.py",
            [],
            "학교정보 병합"
        )

def main():
    logger.info("="*60)
    logger.info(f"🏁 일일 수집 시작 ({get_kst_time()})")
    run_meal_daily()
    run_schedule_daily()
    run_timetable()
    run_master()
    logger.info(f"✅ 일일 수집 완료 ({get_kst_time()})")

if __name__ == "__main__":
    main()
    