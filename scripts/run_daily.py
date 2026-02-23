#!/usr/bin/env python3
"""
매일 실행되는 수집 스크립트 (Hot50 위주, 스케줄 기반)
실제로는 crontab 등에서 시간대별로 호출될 것을 가정
"""
import os
import sys
import subprocess
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.logger import build_logger
from core.kst_time import now_kst, get_kst_time
from core.school_year import get_current_school_year
from baskets.update_hot import get_hot_schools  # 가상의 함수

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
    logger.info("🍽️ 급식 일일 수집 (Hot50)")
    hot = get_hot_schools(period='daily')  # daily Hot50
    if not hot:
        logger.warning("Hot50 없음")
        return
    # 학교 코드를 지역별로 묶어서 호출 (간단히 regions=ALL 로 하고, incremental 모드로)
    # 실제로는 학교 목록을 전달할 수 없으므로 --schools 옵션을 추가하거나,
    # collectors 내에서 Hot50만 필터링하도록 수정해야 함. 여기서는 간단히 regions=ALL 로 하고,
    # incremental 모드에서 이미 있는 데이터는 skip 되도록 함.
    run_collector(
        "collectors/meal.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        "급식 odd (incremental)"
    )
    run_collector(
        "collectors/meal.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        "급식 even (incremental)"
    )

def run_meal_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"🍽️ 급식 월간 전체 수집 (day={day})")
    run_collector(
        "collectors/meal.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "급식 odd (full)"
    )
    run_collector(
        "collectors/meal.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--full"],
        "급식 even (full)"
    )

def run_schedule_daily():
    """매일 06:30 KST: Hot50 학교 학사일정 수집"""
    logger.info("📅 학사일정 일일 수집 (Hot50)")
    run_collector(
        "collectors/schedule.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        "학사일정 odd (incremental)"
    )
    run_collector(
        "collectors/schedule.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        "학사일정 even (incremental)"
    )

def run_schedule_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"📅 학사일정 월간 전체 수집 (day={day})")
    run_collector(
        "collectors/schedule.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "학사일정 odd (full)"
    )
    run_collector(
        "collectors/schedule.py",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--full"],
        "학사일정 even (full)"
    )

def run_timetable():
    """3,4,9,10월 월/수 06:50 KST: 전체 학교 시간표 수집"""
    now = now_kst()
    if now.month in [3,4,9,10] and now.weekday() in [0,2]:  # 월(0), 수(2)
        logger.info("📚 시간표 수집")
        ay = get_current_school_year()
        run_collector(
            "collectors/timetable.py",
            ["--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "odd", "--full"],
            "시간표 odd (full)"
        )
        run_collector(
            "collectors/timetable.py",
            ["--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "even", "--full"],
            "시간표 even (full)"
        )

def run_master():
    """매월 1일 07:10 KST: 학교정보 전체 수집 (변경분만)"""
    if now_kst().day == 1:
        logger.info("🏫 학교정보 월간 수집")
        run_collector(
            "collectors/school_master.py",
            ["--regions", "ALL", "--shard", "odd", "--incremental", "--compare"],
            "학교정보 odd (incremental+compare)"
        )
        run_collector(
            "collectors/school_master.py",
            ["--regions", "ALL", "--shard", "even", "--incremental", "--compare"],
            "학교정보 even (incremental+compare)"
        )
        run_collector(
            "collectors/school_master.py",
            ["--merge"],
            "학교정보 병합"
        )

def main():
    logger.info("="*60)
    logger.info(f"🏁 일일 수집 시작 ({get_kst_time()})")
    # 실제로는 이 스크립트가 crontab에서 원하는 시간에 실행되도록 함
    # 여기서는 단순히 함수들을 순차 실행하는 예시
    run_meal_daily()
    run_schedule_daily()
    run_timetable()
    run_master()
    logger.info(f"✅ 일일 수집 완료 ({get_kst_time()})")

if __name__ == "__main__":
    main()
    