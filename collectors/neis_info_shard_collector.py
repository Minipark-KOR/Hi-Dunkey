#!/usr/bin/env python3
# collectors/neis_info_shard_collector.py
# 개발 가이드: docs/developer_guide.md 참조
"""
neis_info odd/even 샤드 병렬 수집기
- 두 개의 하위 프로세스로 odd, even 샤드를 동시에 실행
- 각 출력에 [ODD] / [EVEN] 접두어 추가
- 완료 후 결과 요약 및 병합 옵션 제공
"""
import os
import sys
import subprocess
import threading
import time
import sqlite3
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (config import를 위해)
sys.path.append(str(Path(__file__).parent.parent))

from core.config import config
from constants.paths import NEIS_INFO_ODD_DB_PATH, NEIS_INFO_EVEN_DB_PATH

# ANSI 색상
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

def print_colored(prefix, color, line, quiet=False):
    if not quiet:
        sys.stdout.write(f"{color}[{prefix}]{RESET} {line}")
        sys.stdout.flush()

def stream_reader(pipe, prefix, color, quiet):
    with pipe:
        for line in iter(pipe.readline, ''):
            if line:
                print_colored(prefix, color, line, quiet)

def run_shard(shard, debug, quiet):
    cmd = [sys.executable, "collectors/neis_info_collector.py", "--shard", shard]
    if debug:
        cmd.append("--debug")
    if quiet:
        cmd.append("--quiet")
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )

def main():
    # 설정 로드
    collector_cfg = config.get_collector_config("neis_info")
    timeout = collector_cfg.get("parallel_timeout_seconds", 7200)
    
    # 명령행 인자 파싱 (간단히)
    debug = False
    quiet = False
    args = sys.argv[1:]
    for arg in args:
        if arg == "--debug":
            debug = True
        if arg == "--quiet":
            quiet = True
    
    if not quiet:
        print(f"{BLUE}🚀 두 개의 샤드 수집을 동시에 시작합니다.{RESET}")
        print(f"{YELLOW}각 프로세스의 출력 앞에 [ODD] / [EVEN] 접두어가 붙습니다.{RESET}")
        print("-" * 60)

    proc_odd = run_shard("odd", debug, quiet)
    proc_even = run_shard("even", debug, quiet)

    thread_odd = threading.Thread(
        target=stream_reader,
        args=(proc_odd.stdout, "ODD", GREEN, quiet)
    )
    thread_even = threading.Thread(
        target=stream_reader,
        args=(proc_even.stdout, "EVEN", YELLOW, quiet)
    )
    thread_odd.start()
    thread_even.start()

    # 타임아웃 처리
    try:
        proc_odd.wait(timeout=timeout)
        proc_even.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc_odd.kill()
        proc_even.kill()
        print(f"{RED}❌ 타임아웃({timeout}초) 초과로 프로세스를 종료했습니다.{RESET}")
        sys.exit(1)

    thread_odd.join()
    thread_even.join()

    if not quiet:
        print("-" * 60)
        print(f"{GREEN}✅ 두 샤드 수집이 모두 완료되었습니다.{RESET}")

    odd_count = 0
    even_count = 0
    odd_db = NEIS_INFO_ODD_DB_PATH
    even_db = NEIS_INFO_EVEN_DB_PATH
    if odd_db.exists():
        try:
            conn = sqlite3.connect(str(odd_db))
            odd_count = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
            conn.close()
        except:
            pass
    if even_db.exists():
        try:
            conn = sqlite3.connect(str(even_db))
            even_count = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
            conn.close()
        except:
            pass
    total = odd_count + even_count

    if not quiet:
        print(f"{BLUE}📊 최종 결과:{RESET}")
        print(f"   odd 샤드: {odd_count}건")
        print(f"   even 샤드: {even_count}건")
        print(f"{GREEN}   총합: {total}건{RESET}")

    # 종료 코드 확인
    exit_code_odd = proc_odd.returncode
    exit_code_even = proc_even.returncode
    if exit_code_odd != 0 or exit_code_even != 0:
        print(f"{RED}⚠️ 일부 프로세스 실패: odd={exit_code_odd}, even={exit_code_even}{RESET}")
        sys.exit(1)

    # 병합 스크립트 실행 여부 (--merge 옵션을 추가할 수도 있으나, 여기서는 기존처럼 프롬프트)
    if not quiet:
        print()
        answer = input("병합 스크립트를 실행하시겠습니까? (y/n): ").strip().lower()
        if answer == 'y':
            merge_script = Path("scripts/merge_neis_info_dbs.py")
            if merge_script.exists():
                subprocess.run([sys.executable, str(merge_script)])
            else:
                print(f"{RED}❌ scripts/merge_neis_info_dbs.py 파일이 없습니다.{RESET}")

    sys.exit(0)

if __name__ == "__main__":
    main()
    