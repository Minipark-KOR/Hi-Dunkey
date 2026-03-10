#!/usr/bin/env python3
# collectors/school_info_shard_collector.py
# 개발 가이드: docs/developer_guide.md 참조
"""
학교알리미 수집기 - 샤드 병렬 실행용 래퍼
- odd/even 샤드를 동시에 실행하여 수집 시간 단축
- 설정 파일에서 타임아웃 등을 가져옴
"""
import sys
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent))

from core.config import config

BASE_DIR = Path(__file__).parent
COLLECTOR_SCRIPT = BASE_DIR / "school_info_collector.py"

def run_shard(shard_type: str, base_args: list, debug: bool, quiet: bool) -> int:
    cmd = [sys.executable, str(COLLECTOR_SCRIPT)] + base_args + ['--shard', shard_type]
    if debug:
        cmd.append('--debug')
    if quiet:
        cmd.append('--quiet')
    if not quiet:
        print(f"▶ {shard_type} 샤드 실행: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode

def main():
    # 설정 로드
    collector_cfg = config.get_collector_config("school_info")
    timeout = collector_cfg.get("parallel_timeout_seconds", 7200)
    
    # 명령행 인자에서 --debug, --quiet 추출하고 나머지는 base_args로 전달
    base_args = []
    debug = False
    quiet = False
    skip_next = False
    for i, arg in enumerate(sys.argv[1:]):
        if skip_next:
            skip_next = False
            continue
        if arg in ['--shard', '-s']:
            skip_next = True
            continue
        if arg == '--debug':
            debug = True
            continue
        if arg == '--quiet':
            quiet = True
            continue
        base_args.append(arg)

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(run_shard, 'odd', base_args, debug, quiet): 'odd',
            executor.submit(run_shard, 'even', base_args, debug, quiet): 'even'
        }
        results = {}
        for future in as_completed(futures, timeout=timeout):
            shard = futures[future]
            try:
                results[shard] = future.result(timeout=5)
            except Exception as e:
                if not quiet:
                    print(f"⚠️ {shard} 샤드 실행 중 예외: {e}")
                results[shard] = 1

    if all(r == 0 for r in results.values()):
        if not quiet:
            print("✅ 모든 샤드 수집 완료")
        sys.exit(0)
    else:
        if not quiet:
            print("⚠️ 일부 샤드 실패")
        sys.exit(1)

if __name__ == "__main__":
    main()
    