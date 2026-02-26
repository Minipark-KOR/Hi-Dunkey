#!/usr/bin/env python3
import subprocess
import sys
import time
from datetime import datetime
from core.kst_time import now_kst
from core.logger import build_logger

logger = build_logger("feb20", "../logs/feb20.log")
BASE_DIR = "."

def run_collector(script, args, desc):
    logger.info(f"🚀 {desc}")
    cmd = [sys.executable, script] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"❌ {desc} 실패: {result.stderr}")
    else:
        logger.info(f"✅ {desc} 완료")
    time.sleep(5)

def main():
    logger.info("="*60)
    logger.info("🏁 2월 20일 전체 수집 시작")
    today = now_kst().strftime("%Y%m%d")
    year = now_kst().year - 1  # 작년 학년도

    domains = [
        ("collectors/meal.py", ["--regions", "ALL", "--date", today]),
        ("collectors/schedule.py", ["--regions", "ALL", "--year", str(now_kst().year)]),
        ("collectors/timetable.py", ["--ay", str(year), "--semester", "2"]),
        ("collectors/school_master.py", ["--regions", "ALL"]),
    ]
    for script, base_args in domains:
        for shard in ['odd', 'even']:
            args = base_args + ["--shard", shard, "--full"]
            run_collector(script, args, f"{script} {shard}")
        if script == "collectors/school_master.py":
            run_collector(script, ["--merge"], "school_master 병합")
    logger.info("✅ 2월 20일 완료")

if __name__ == "__main__":
    main()
    