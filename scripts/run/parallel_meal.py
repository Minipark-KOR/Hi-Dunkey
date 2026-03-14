#!/usr/bin/env python3
# scripts/run/parallel_meal.py
"""
급식 병렬 수집 실행 스크립트 (로컬 멀티프로세싱)
- 여러 프로세스를 띄워 동시에 수집
- 모든 작업 완료 후 merge 실행
"""
import subprocess
import sys
import os
from multiprocessing import Pool
from typing import List, Tuple, Optional
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from constants.paths import LOG_DIR
from core.config import config

# 실행할 조합 정의: (region, shard, school_range)
COMBINATIONS: List[Tuple[str, str, Optional[str]]] = [
    # 경기 (J10) - 4개 조합
    ("J10", "odd",  "A"),
    ("J10", "odd",  "B"),
    ("J10", "even", "A"),
    ("J10", "even", "B"),
    # 서울 (B10)
    ("B10", "odd",  None),
    ("B10", "even", None),
    # 부산 (C10)
    ("C10", "odd",  None),
    ("C10", "even", None),
    # 인천 (E10)
    ("E10", "odd",  None),
    ("E10", "even", None),
    # 대구 (D10)
    ("D10", "odd",  None),
    ("D10", "even", None),
    # 필요에 따라 추가
]

DEFAULT_YEAR = str(config.get('pipeline', 'default_year', default='2025'))
DEBUG = True


def run_collector(args: Tuple[str, str, Optional[str], str, bool]) -> int:
    region, shard, school_range, year, debug = args
    cmd = [
        sys.executable,
        "scripts/collector/meal.py",
        "--regions", region,
        "--year", year,
        "--shard", shard,
    ]
    if school_range:
        cmd.extend(["--school_range", school_range])
    if debug:
        cmd.append("--debug")

    os.makedirs(LOG_DIR, exist_ok=True)
    range_suffix = f"_{school_range}" if school_range else ""
    log_file = LOG_DIR / f"run.parallel_meal.meal_{region}_{shard}{range_suffix}.log"

    with open(log_file, "w", encoding="utf-8") as f:
        proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    return proc.returncode


def main():
    import argparse
    parser = argparse.ArgumentParser(description="급식 병렬 수집")
    parser.add_argument("--year", default=DEFAULT_YEAR, help="수집할 학년도")
    args = parser.parse_args()

    print(f"🚀 급식 병렬 수집 시작: {len(COMBINATIONS)}개 작업 (학년도: {args.year})")
    tasks = [(r, s, rg, args.year, DEBUG) for r, s, rg in COMBINATIONS]

    with Pool(processes=len(tasks)) as pool:
        results = pool.map(run_collector, tasks)

    failed = [i for i, code in enumerate(results) if code != 0]
    if failed:
        print(f"⚠️ {len(failed)}개 작업 실패: {failed}")
    else:
        print("✅ 모든 수집 작업 완료")

    print("🔄 샤드 병합 시작...")
    merge_cmd = [sys.executable, "scripts/merge/meal.py", "--consolidate-vocab"]
    subprocess.run(merge_cmd)
    print("✅ 병합 완료")


if __name__ == "__main__":
    main()
