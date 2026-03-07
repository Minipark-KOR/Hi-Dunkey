#!/usr/bin/env python3
"""
학교알리미 수집기 - 샤드 병렬 실행용 래퍼
- odd/even 샤드를 동시에 실행하여 수집 시간 단축
- master_collectors.py 와 연동 가능
"""
import sys
import subprocess
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

BASE_DIR = Path(__file__).parent
COLLECTOR_SCRIPT = BASE_DIR / "school_info_collector.py"
CONFIG_FILE = BASE_DIR.parent / "collectors.json"

def get_collector_config(name: str) -> dict:
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            collectors = json.load(f)
        for col in collectors:
            if col.get('name') == name:
                return col
    except Exception:
        pass
    return {}

def run_shard(shard_type: str, base_args: list) -> int:
    cmd = [sys.executable, str(COLLECTOR_SCRIPT)] + base_args + ['--shard', shard_type]
    print(f"▶ {shard_type} 샤드 실행: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode

def main():
    config = get_collector_config("school_info")
    parallel_config = config.get('parallel_config', {})
    timeout = parallel_config.get('timeout_seconds', 7200)

    base_args = []
    skip_next = False
    for i, arg in enumerate(sys.argv[1:]):
        if skip_next:
            skip_next = False
            continue
        if arg in ['--shard', '-s']:
            skip_next = True
            continue
        base_args.append(arg)

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(run_shard, 'odd', base_args): 'odd',
            executor.submit(run_shard, 'even', base_args): 'even'
        }
        results = {}
        for future in as_completed(futures, timeout=timeout):
            shard = futures[future]
            try:
                results[shard] = future.result(timeout=5)
            except Exception as e:
                print(f"⚠️ {shard} 샤드 실행 중 예외: {e}")
                results[shard] = 1

    if all(r == 0 for r in results.values()):
        print("✅ 모든 샤드 수집 완료")
        sys.exit(0)
    else:
        print("⚠️ 일부 샤드 실패")
        sys.exit(1)

if __name__ == "__main__":
    main()
    