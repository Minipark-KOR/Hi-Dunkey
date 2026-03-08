#!/usr/bin/env python3
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

# ANSI 색상
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

def print_colored(prefix, color, line):
    """색상과 접두어를 붙여 출력"""
    sys.stdout.write(f"{color}[{prefix}]{RESET} {line}")
    sys.stdout.flush()

def stream_reader(pipe, prefix, color):
    """프로세스의 출력 스트림을 읽어 실시간 출력"""
    with pipe:
        for line in iter(pipe.readline, ''):
            if line:
                print_colored(prefix, color, line)

def run_shard(shard):
    """단일 샤드 실행 (하위 프로세스)"""
    cmd = [sys.executable, "collectors/neis_info_collector.py", "--shard", shard]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )

def main():
    print(f"{BLUE}🚀 두 개의 샤드 수집을 동시에 시작합니다.{RESET}")
    print(f"{YELLOW}각 프로세스의 출력 앞에 [ODD] / [EVEN] 접두어가 붙습니다.{RESET}")
    print("-" * 60)

    # 프로세스 시작
    proc_odd = run_shard("odd")
    proc_even = run_shard("even")

    # 출력 스트림을 읽는 스레드 시작
    thread_odd = threading.Thread(
        target=stream_reader,
        args=(proc_odd.stdout, "ODD", GREEN)
    )
    thread_even = threading.Thread(
        target=stream_reader,
        args=(proc_even.stdout, "EVEN", YELLOW)
    )
    thread_odd.start()
    thread_even.start()

    # 프로세스 종료 대기
    proc_odd.wait()
    proc_even.wait()
    thread_odd.join()
    thread_even.join()

    print("-" * 60)
    print(f"{GREEN}✅ 두 샤드 수집이 모두 완료되었습니다.{RESET}")

    # 결과 요약 (레코드 수)
    odd_count = 0
    even_count = 0
    odd_db = Path("data/master/neis_info_odd.db")
    even_db = Path("data/master/neis_info_even.db")
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

    print(f"{BLUE}📊 최종 결과:{RESET}")
    print(f"   odd 샤드: {odd_count}건")
    print(f"   even 샤드: {even_count}건")
    print(f"{GREEN}   총합: {total}건{RESET}")

    # 병합 여부 묻기
    print()
    answer = input("병합 스크립트를 실행하시겠습니까? (y/n): ").strip().lower()
    if answer == 'y':
        merge_script = Path("scripts/merge_neis_info_dbs.py")
        if merge_script.exists():
            subprocess.run([sys.executable, str(merge_script)])
        else:
            print(f"{RED}❌ scripts/merge_neis_info_dbs.py 파일이 없습니다.{RESET}")

if __name__ == "__main__":
    main()
    