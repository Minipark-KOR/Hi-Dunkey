#!/usr/bin/env python3
"""
시간표 병렬 수집 실행 스크립트
"""
import sys
import os
import subprocess
from multiprocessing import Pool

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.parallel import setup_worker_pool

COMBINATIONS = [
    ("J10", "odd", "A"),
    ("J10", "odd", "B"),
    ("J10", "even", "A"),
    ("J10", "even", "B"),
    ("B10", "odd", "none"),
    ("B10", "even", "none"),
    ("C10", "odd", "none"),
    ("C10", "even", "none"),
    ("E10", "odd", "none"),
    ("E10", "even", "none"),
    ("D10", "odd", "none"),
    ("D10", "even", "none"),
    ("F10", "none", "none"),
    ("G10", "none", "none"),
    ("H10", "none", "none"),
    ("I10", "none", "none"),
    ("K10", "none", "none"),
    ("M10", "none", "none"),
    ("N10", "none", "none"),
    ("P10", "none", "none"),
    ("Q10", "none", "none"),
    ("R10", "none", "none"),
    ("S10", "none", "none"),
    ("T10", "none", "none"),
]

YEAR = "2025"
SEMESTER = 1
DEBUG = True

def run_collector(args):
    region, shard, school_range = args
    cmd = [
        sys.executable,
        "collectors/timetable_collector.py",   # 변경 없음
        "--regions", region,
        "--year", YEAR,
        "--semester", str(SEMESTER),
        "--shard", shard,
    ]
    if school_range != "none":
        cmd.extend(["--school_range", school_range])
    if DEBUG:
        cmd.append("--debug")
    subprocess.run(cmd, check=False)

def main():
    total_jobs = len(COMBINATIONS)
    with setup_worker_pool(total_jobs) as pool:
        pool.map(run_collector, COMBINATIONS)
    subprocess.run([sys.executable, "scripts/merge_timetable_dbs.py", "--consolidate-vocab"])

if __name__ == "__main__":
    main()
    