#!/usr/bin/env python3
# 개발 가이드: docs/developer_guide.md 참조
"""
통합 수집 실행기 (run_collector.py)
- 매일/매월/특정 요일 등 다양한 주기의 수집 작업을 관리
- collector_cli.py를 통해 각 수집기 호출
- NEIS 학교정보 → retry_worker → 학교알리미 정보 → 급식/학사일정/시간표 순서로 실행
"""
import os
import sys
import subprocess
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.logger import build_logger
from constants.paths import LOG_DIR   # 추가
from core.kst_time import now_kst, get_kst_time
from core.school_year import get_current_school_year
from baskets.update_hot import get_hot_schools

logger = build_logger("run_collector", str(LOG_DIR / "run_collector.log"))   # 수정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_collector(collector_name, args_list, desc):
    """collector_cli.py를 통해 수집기 실행"""
    logger.info(f"🚀 {desc}")
    cmd = [sys.executable, "collector_cli.py", collector_name] + args_list
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"❌ {desc} 실패: {result.stderr}")
    else:
        logger.info(f"✅ {desc} 완료")
    time.sleep(5)


def run_retry_worker():
    """지오코딩 실패 재시도 워커 실행 (배치 모드)"""
    logger.info("🔄 지오코딩 재시도 워커 실행")
    cmd = [sys.executable, "scripts/retry_worker.py", "--limit", "100", "--force", "--no-menu"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"❌ retry_worker 실패: {result.stderr}")
    else:
        logger.info(f"✅ retry_worker 완료")
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
        "meal_collector",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        f"급식 odd (incremental) - regions: {regions}"
    )
    run_collector(
        "meal_collector",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        f"급식 even (incremental) - regions: {regions}"
    )


def run_meal_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"🍽️ 급식 월간 전체 수집 (day={day})")
    run_collector(
        "meal_collector",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "급식 odd (full)"
    )
    run_collector(
        "meal_collector",
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
        "schedule_collector",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--incremental"],
        f"학사일정 odd (incremental) - regions: {regions}"
    )
    run_collector(
        "schedule_collector",
        ["--regions", regions, "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--incremental"],
        f"학사일정 even (incremental) - regions: {regions}"
    )


def run_schedule_monthly(day):
    """매월 1,10,20일 전체 수집"""
    logger.info(f"📅 학사일정 월간 전체 수집 (day={day})")
    run_collector(
        "schedule_collector",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "odd", "--full"],
        "학사일정 odd (full)"
    )
    run_collector(
        "schedule_collector",
        ["--regions", "ALL", "--date", now_kst().strftime("%Y%m%d"),
         "--shard", "even", "--full"],
        "학사일정 even (full)"
    )


def run_timetable():
    """3,4,9,10월 월/수 06:50 KST: 전체 학교 시간표 수집"""
    now = now_kst()
    if now.month in [3, 4, 9, 10] and now.weekday() in [0, 2]:  # 월요일(0), 수요일(2)
        logger.info("📚 시간표 수집")
        ay = get_current_school_year()
        run_collector(
            "timetable_collector",
            ["--regions", "ALL", "--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "odd", "--full"],
            "시간표 odd (full)"
        )
        run_collector(
            "timetable_collector",
            ["--regions", "ALL", "--ay", str(ay), "--semester", "1" if now.month <= 7 else "2",
             "--shard", "even", "--full"],
            "시간표 even (full)"
        )


def run_master():
    """매월 1일 07:10 KST: NEIS 학교정보 수집 (변경분만)"""
    if now_kst().day == 1:
        logger.info("🏫 NEIS 학교정보 월간 수집")
        run_collector(
            "neis_info",
            ["--regions", "ALL", "--shard", "odd", "--incremental", "--compare"],
            "NEIS 학교정보 odd (incremental+compare)"
        )
        run_collector(
            "neis_info",
            ["--regions", "ALL", "--shard", "even", "--incremental", "--compare"],
            "NEIS 학교정보 even (incremental+compare)"
        )

        # 병합 스크립트 (별도 실행)
        logger.info("🔗 NEIS 학교정보 DB 병합 시작")
        merge_cmd = [sys.executable, "scripts/merge_neis_info_dbs.py"]
        merge_result = subprocess.run(merge_cmd, capture_output=True, text=True)
        if merge_result.returncode != 0:
            logger.error(f"❌ 병합 실패: {merge_result.stderr}")
        else:
            logger.info(f"✅ 병합 완료")

        # 지오코딩 재시도
        run_retry_worker()
    else:
        logger.info("🏫 오늘은 NEIS 학교정보 수집일이 아닙니다 (매월 1일만 실행).")


def run_school_info():
    """학교알리미 기본정보 수집 (매월 1일 전체 수집)"""
    now = now_kst()
    if now.day == 1:
        logger.info("🏫 학교알리미 정보 월간 수집")
        run_collector(
            "school_info",
            ["--regions", "ALL", "--shard", "odd", "--year", str(now.year)],
            "학교알리미 odd"
        )
        run_collector(
            "school_info",
            ["--regions", "ALL", "--shard", "even", "--year", str(now.year)],
            "학교알리미 even"
        )
        # 필요시 통합본
        # run_collector("school_info", ["--regions", "ALL", "--shard", "none", "--year", str(now.year)], "학교알리미 통합")
    else:
        logger.info("🏫 오늘은 학교알리미 수집일이 아닙니다 (매월 1일만 실행).")


def main():
    logger.info("=" * 60)
    logger.info(f"🏁 통합 수집 시작 ({get_kst_time()})")

    run_master()          # NEIS 학교정보 (매월 1일)
    run_school_info()     # 학교알리미 정보 (매월 1일)
    run_meal_daily()      # 급식 (매일 Hot50)
    run_schedule_daily()  # 학사일정 (매일 Hot50)
    run_timetable()       # 시간표 (특정 월/요일)

    logger.info(f"✅ 통합 수집 완료 ({get_kst_time()})")


if __name__ == "__main__":
    main()
    