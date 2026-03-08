네, 이해했습니다. 사용자는 매일 실행되는 스크립트(`run_daily.py`)에서 학교 기본정보(나이스)를 가장 먼저 수집하고, 수집 후 변경 사항이 있으면 재시도 로직(`retry_worker.py`)을 실행하길 원합니다. 또한 `run_daily.py`에 이미 다른 수집기들이 포함되어 있으므로 순서를 조정하면 됩니다.

아래는 수정된 `run_daily.py`입니다. 주요 변경점:
- `run_master()`를 가장 먼저 호출 (학교정보 우선 수집)
- `run_master()` 실행 후, `retry_worker.py`를 실행하여 지오코딩 실패를 재시도
- `retry_worker` 실행 시 `--force`와 `--no-menu` 옵션을 사용하여 배치 모드로 실행

```python
#!/usr/bin/env python3
"""
매일 실행되는 수집 스크립트 (Hot50 위주, 스케줄 기반)
- 학교정보(나이스)를 가장 먼저 수집하고, 변경사항이 있으면 재시도 로직 실행
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
            "collectors/neis_info_collector.py",
            ["--regions", "ALL", "--shard", "odd", "--incremental", "--compare"],
            "학교정보 odd (incremental+compare)"
        )
        run_collector(
            "collectors/neis_info_collector.py",
            ["--regions", "ALL", "--shard", "even", "--incremental", "--compare"],
            "학교정보 even (incremental+compare)"
        )
        run_collector(
            "scripts/merge_neis_info_dbs.py",
            [],
            "학교정보 병합"
        )
        # 학교정보 수집 후 지오코딩 재시도 실행
        run_retry_worker()
    else:
        logger.info("🏫 오늘은 학교정보 수집일이 아닙니다 (매월 1일만 실행).")

def main():
    logger.info("="*60)
    logger.info(f"🏁 일일 수집 시작 ({get_kst_time()})")
    
    # 학교정보를 가장 먼저 수집 (매월 1일)
    run_master()
    
    # 그 다음 다른 수집기들 실행
    run_meal_daily()
    run_schedule_daily()
    run_timetable()
    
    logger.info(f"✅ 일일 수집 완료 ({get_kst_time()})")

if __name__ == "__main__":
    main()
```

### 주요 변경점 설명
1. **`run_master()`를 `main()`의 첫 번째로 호출**하여 학교정보가 다른 수집보다 먼저 실행되도록 함.
2. **`run_master()` 내부에 `run_retry_worker()` 추가** – 학교정보 수집 후 변경된 데이터에 대한 지오코딩 재시도를 바로 실행.
3. **`run_retry_worker()` 함수 정의** – `retry_worker.py`를 배치 모드(`--no-menu`)와 강제 실행(`--force`)으로 호출하며, 한 번에 100개씩 처리.
4. **기존 일일 수집기(급식, 학사일정, 시간표)는 그대로 유지**되며, 학교정보 다음에 실행.

이제 매일 아침 실행될 때 학교정보가 가장 먼저 수집되고, 이후 지오코딩 재시도가 자동으로 수행됩니다. 다른 수집기들은 순차적으로 실행됩니다.

만약 매월 1일이 아닌 날에는 `run_master()`가 아무것도 하지 않으므로, 다른 수집기만 실행됩니다. 이는 기존 로직과 동일합니다.
