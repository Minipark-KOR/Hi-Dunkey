#!/usr/bin/env python3
"""
마스터 수집기 - 모든 수집기 통합 관리 (초기 버전)
- collectors.json 에 정의된 수집기 목록을 읽어 메뉴 제공
- 각 수집기의 모드(통합/샤드/병렬) 선택 실행
- 실행 후 결과 확인(레코드 수) 기능
"""
import os
import sys
import json
import subprocess
import time
from pathlib import Path

# ANSI 색상 (선택)
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"

CONFIG_FILE = Path(__file__).parent / "collectors.json"

def load_collectors():
    """설정 파일 로드"""
    if not CONFIG_FILE.exists():
        print(f"{RED}❌ 설정 파일이 없습니다: {CONFIG_FILE}{RESET}")
        sys.exit(1)
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def print_header():
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📊 마스터 수집기{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

def select_collector(collectors):
    """수집기 선택 메뉴"""
    print(f"\n{YELLOW}실행할 수집기를 선택하세요:{RESET}")
    for i, col in enumerate(collectors, 1):
        print(f"  {i}) {col['description']}")
    print(f"  {len(collectors)+1}) 종료")
    choice = input("선택: ").strip()
    return choice

def select_mode(collector):
    """실행 모드 선택 메뉴"""
    print(f"\n{YELLOW}실행 모드를 선택하세요 ({collector['description']}):{RESET}")
    modes = collector.get('modes', ['통합'])
    for i, mode in enumerate(modes, 1):
        print(f"  {i}) {mode}")
    print(f"  {len(modes)+1}) 뒤로 가기")
    choice = input("선택: ").strip()
    return choice

def run_collector(script, args):
    """수집기 실행 (단일 프로세스)"""
    cmd = [sys.executable, script] + args
    print(f"\n{GREEN}▶ 실행: {' '.join(cmd)}{RESET}\n")
    subprocess.run(cmd)

def run_parallel(parallel_script):
    """병렬 실행 스크립트 호출"""
    if not parallel_script or not os.path.exists(parallel_script):
        print(f"{RED}❌ 병렬 실행 스크립트가 없습니다: {parallel_script}{RESET}")
        return False
    cmd = [sys.executable, parallel_script]
    print(f"\n{GREEN}▶ 병렬 실행: {' '.join(cmd)}{RESET}\n")
    subprocess.run(cmd)
    return True

def show_results(collector):
    """결과 확인 메뉴"""
    while True:
        print(f"\n{YELLOW}결과 확인 옵션 ({collector['description']}):{RESET}")
        print("  1) 통합 DB 레코드 수 확인")
        if 'shard_odd' in collector and 'shard_even' in collector:
            print("  2) 샤드 DB 레코드 수 확인 (odd/even)")
        if 'merge_script' in collector:
            print("  3) 병합 실행")
        print("  4) 뒤로 가기")
        choice = input("선택: ").strip()

        if choice == '1':
            db_path = collector.get('db_path')
            if db_path and os.path.exists(db_path):
                table = collector.get('table_name', 'schools')
                try:
                    count = subprocess.getoutput(
                        f"sqlite3 {db_path} \"SELECT COUNT(*) FROM {table};\""
                    )
                    print(f"{GREEN}📊 통합 DB 레코드 수: {count}{RESET}")
                except Exception as e:
                    print(f"{RED}❌ 조회 실패: {e}{RESET}")
            else:
                print(f"{RED}❌ DB 파일이 없습니다: {db_path}{RESET}")
        elif choice == '2' and 'shard_odd' in collector and 'shard_even' in collector:
            for key in ['shard_odd', 'shard_even']:
                db_path = collector.get(key)
                if db_path and os.path.exists(db_path):
                    table = collector.get('table_name', 'schools')
                    try:
                        count = subprocess.getoutput(
                            f"sqlite3 {db_path} \"SELECT COUNT(*) FROM {table};\""
                        )
                        print(f"{GREEN}📊 {key}: {count}{RESET}")
                    except Exception as e:
                        print(f"{RED}❌ {key} 조회 실패: {e}{RESET}")
                else:
                    print(f"{YELLOW}⚠️ {key} 파일 없음{RESET}")
        elif choice == '3' and 'merge_script' in collector:
            merge_script = collector['merge_script']
            if os.path.exists(merge_script):
                subprocess.run([sys.executable, merge_script])
            else:
                print(f"{RED}❌ 병합 스크립트 없음: {merge_script}{RESET}")
        elif choice == '4':
            break
        else:
            print(f"{RED}잘못된 선택입니다.{RESET}")

def main():
    collectors = load_collectors()
    while True:
        print_header()
        choice = select_collector(collectors)
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(collectors):
                collector = collectors[idx]
                while True:
                    mode_choice = select_mode(collector)
                    if mode_choice.isdigit():
                        m = int(mode_choice) - 1
                        modes = collector.get('modes', ['통합'])
                        if 0 <= m < len(modes):
                            selected_mode = modes[m]
                            # 실행 인자 구성
                            if selected_mode == "통합":
                                args = ['--shard', 'none']
                                run_collector(collector['script'], args)
                            elif "odd" in selected_mode:
                                args = ['--shard', 'odd']
                                run_collector(collector['script'], args)
                            elif "even" in selected_mode:
                                args = ['--shard', 'even']
                                run_collector(collector['script'], args)
                            elif "병렬" in selected_mode:
                                # 병렬 실행
                                parallel_script = collector.get('parallel_script')
                                if parallel_script and os.path.exists(parallel_script):
                                    run_parallel(parallel_script)
                                else:
                                    print(f"{YELLOW}⚠️ 병렬 실행 스크립트가 없어 순차적으로 실행합니다.{RESET}")
                                    # fallback: odd -> even 순차 실행
                                    run_collector(collector['script'], ['--shard', 'odd'])
                                    run_collector(collector['script'], ['--shard', 'even'])
                            else:
                                print(f"{RED}❌ 알 수 없는 모드: {selected_mode}{RESET}")
                                continue

                            # 실행 후 결과 확인 메뉴로 이동
                            show_results(collector)
                        elif m == len(modes):
                            break  # 뒤로 가기
                        else:
                            print(f"{RED}잘못된 선택입니다.{RESET}")
                    else:
                        print(f"{RED}숫자를 입력하세요.{RESET}")
            elif idx == len(collectors):
                print(f"{GREEN}👋 종료합니다.{RESET}")
                break
            else:
                print(f"{RED}잘못된 선택입니다.{RESET}")
        else:
            print(f"{RED}숫자를 입력하세요.{RESET}")

if __name__ == "__main__":
    main()
    