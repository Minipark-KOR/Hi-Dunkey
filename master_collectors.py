#!/usr/bin/env python3
"""
마스터 수집기 - 실행 유형과 수집 방식을 계층적으로 선택
- 실행 후 결과 확인 메뉴 제공 (데이터 무결성, 병합, 다른 수집기, 종료)
"""
import os
import sys
import json
import subprocess
from pathlib import Path

# ANSI 색상
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"

CONFIG_FILE = Path(__file__).parent / "collectors.json"

def load_collectors():
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
    print(f"\n{YELLOW}실행할 수집기를 선택하세요:{RESET}")
    for i, col in enumerate(collectors, 1):
        print(f"  {i}) {col['description']}")
    print(f"  {len(collectors)+1}) 종료")
    return input("선택: ").strip()

def select_run_type():
    """실행 유형 선택"""
    print(f"\n{YELLOW}실행 유형을 선택하세요:{RESET}")
    print("  1) 학교 기본정보 수집 (실제 수집, 전체)")
    print("  2) 테스트 모드 (간단 로그, 제한 수집)")
    print("  3) 디버그 모드 (상세 로그)")
    print("  4) 고급 모드(메뉴형) – 단계별 옵션 선택")
    print("  5) 고급 모드(직접 입력) – 옵션 직접 입력")
    print("  6) 뒤로 가기")
    return input("선택: ").strip()

def select_mode(collector):
    """수집 방식 선택"""
    print(f"\n{YELLOW}수집 방식을 선택하세요 ({collector['description']}):{RESET}")
    modes = collector.get('modes', ['통합', 'odd 샤드', 'even 샤드', '병렬 실행'])
    for i, mode in enumerate(modes, 1):
        print(f"  {i}) {mode}")
    print(f"  {len(modes)+1}) 뒤로 가기")
    return input("선택: ").strip()

def get_basic_options(run_type):
    """기본 실행 유형(1,2,3)에 대한 옵션 수집"""
    base_args = []
    if run_type == '1':  # 학교 기본정보 수집
        print(f"{YELLOW}실제 수집 모드: 전체 데이터 수집{RESET}")
    elif run_type == '2':  # 테스트 모드
        base_args.extend(['--limit', '50'])
        print(f"{YELLOW}테스트 모드: --limit 50 적용{RESET}")
    elif run_type == '3':  # 디버그 모드
        base_args.append('--debug')
        print(f"{YELLOW}디버그 모드: --debug 적용{RESET}")

    # 지역 입력
    regions = input("지역 코드 (기본 전체, 여러 개는 쉼표, 예: B10,C10): ").strip()
    if regions:
        base_args.extend(['--regions', regions])

    # 제한 개수 입력
    limit = input("수집 제한 개수 (기본: 전체): ").strip()
    if limit.isdigit():
        if '--limit' in base_args:
            idx = base_args.index('--limit')
            base_args.pop(idx)
            base_args.pop(idx)
        base_args.extend(['--limit', limit])

    return base_args

def menu_advanced_mode():
    """고급 모드(메뉴형) – 단계별 옵션 선택"""
    args = []
    print(f"\n{YELLOW}[고급 모드 메뉴형] 옵션을 선택하세요.{RESET}")

    # 1. 샤드 모드 선택
    print("\n샤드 모드를 선택하세요:")
    print("  1) 통합 (none)")
    print("  2) odd 샤드")
    print("  3) even 샤드")
    print("  4) 병렬 실행 (odd+even 동시)")
    shard_choice = input("선택 (1-4, 기본 통합): ").strip()
    if shard_choice == '2':
        args.extend(['--shard', 'odd'])
        is_parallel = False
    elif shard_choice == '3':
        args.extend(['--shard', 'even'])
        is_parallel = False
    elif shard_choice == '4':
        is_parallel = True
    else:
        args.extend(['--shard', 'none'])
        is_parallel = False

    # 2. 지역 선택
    regions = input("\n지역 코드 (기본 전체, 여러 개는 쉼표, 예: B10,C10): ").strip()
    if regions:
        args.extend(['--regions', regions])

    # 3. 제한 개수
    limit = input("수집 제한 개수 (기본 전체): ").strip()
    if limit.isdigit():
        args.extend(['--limit', limit])

    # 4. 디버그 여부
    debug = input("디버그 모드? (y/n) [n]: ").strip().lower()
    if debug == 'y':
        args.append('--debug')

    return args, is_parallel

def direct_advanced_mode():
    """고급 모드(직접 입력) – 옵션 문자열 직접 입력"""
    print(f"\n{YELLOW}[고급 모드 직접 입력] 원하는 옵션을 한 줄로 입력하세요.{RESET}")
    print("예시: --shard odd --regions B10 --limit 50 --debug")
    custom = input("옵션: ").strip()
    if custom:
        return custom.split()
    else:
        print(f"{YELLOW}옵션이 없습니다. 기본 실행됩니다.{RESET}")
        return []

def run_collector(script, args):
    cmd = [sys.executable, script] + args
    print(f"\n{GREEN}▶ 실행: {' '.join(cmd)}{RESET}\n")
    subprocess.run(cmd)

def run_parallel(parallel_script, base_args):
    """병렬 실행 (각각 odd/even 에 base_args 적용)"""
    if not parallel_script or not os.path.exists(parallel_script):
        print(f"{RED}❌ 병렬 실행 스크립트가 없습니다: {parallel_script}{RESET}")
        return False
    cmd = [sys.executable, parallel_script] + base_args
    print(f"\n{GREEN}▶ 병렬 실행: {' '.join(cmd)}{RESET}\n")
    subprocess.run(cmd)
    return True

def post_run_menu(collector):
    """실행 후 결과 확인 및 후속 작업 메뉴"""
    while True:
        print(f"\n{YELLOW}후속 작업을 선택하세요.{RESET}")
        print("  1) 데이터 무결성 확인 (레코드 수, 샤드 합계 등)")
        print("  2) 병합 실행 (샤드 파일이 있을 경우)")
        print("  3) 다른 수집기 실행 (메인 메뉴로)")
        print("  4) 종료")
        choice = input("선택: ").strip()

        if choice == '1':
            # 데이터 무결성 확인
            print(f"\n{BLUE}📊 데이터 무결성 확인{RESET}")
            # 통합 DB
            db_path = collector.get('db_path')
            if db_path and os.path.exists(db_path):
                table = collector.get('table_name', 'schools')
                try:
                    count = subprocess.getoutput(f"sqlite3 {db_path} \"SELECT COUNT(*) FROM {table};\"")
                    print(f"   통합 DB ({db_path}): {count}건")
                except Exception as e:
                    print(f"   {RED}통합 DB 조회 실패: {e}{RESET}")
            else:
                print(f"   통합 DB 없음")

            # 샤드 DB
            odd_path = collector.get('shard_odd')
            even_path = collector.get('shard_even')
            odd_count = even_count = None
            if odd_path and os.path.exists(odd_path):
                try:
                    odd_count = subprocess.getoutput(f"sqlite3 {odd_path} \"SELECT COUNT(*) FROM {table};\"")
                    print(f"   odd 샤드 ({odd_path}): {odd_count}건")
                except Exception as e:
                    print(f"   {RED}odd 샤드 조회 실패: {e}{RESET}")
            if even_path and os.path.exists(even_path):
                try:
                    even_count = subprocess.getoutput(f"sqlite3 {even_path} \"SELECT COUNT(*) FROM {table};\"")
                    print(f"   even 샤드 ({even_path}): {even_count}건")
                except Exception as e:
                    print(f"   {RED}even 샤드 조회 실패: {e}{RESET}")

            # 합계 계산
            if odd_count is not None and even_count is not None:
                try:
                    total_shard = int(odd_count) + int(even_count)
                    print(f"   샤드 합계: {total_shard}건")
                    if db_path and os.path.exists(db_path):
                        total_db = int(subprocess.getoutput(f"sqlite3 {db_path} \"SELECT COUNT(*) FROM {table};\""))
                        if total_shard == total_db:
                            print(f"{GREEN}   ✅ 통합 DB와 샤드 합계가 일치합니다.{RESET}")
                        else:
                            print(f"{RED}   ❌ 불일치! 통합 DB: {total_db}, 샤드 합계: {total_shard}{RESET}")
                except:
                    pass

        elif choice == '2':
            # 병합 실행
            merge_script = collector.get('merge_script')
            if merge_script and os.path.exists(merge_script):
                print(f"\n{GREEN}🔗 병합 스크립트 실행{RESET}")
                subprocess.run([sys.executable, merge_script])
            else:
                print(f"{RED}❌ 병합 스크립트가 없습니다.{RESET}")

        elif choice == '3':
            # 다른 수집기 실행 (메인 메뉴로 복귀)
            print(f"{YELLOW}메인 메뉴로 돌아갑니다.{RESET}")
            return True  # 계속 진행 (메인 루프 유지)

        elif choice == '4':
            # 종료
            print(f"{GREEN}👋 종료합니다.{RESET}")
            sys.exit(0)

        else:
            print(f"{RED}잘못된 선택입니다.{RESET}")

def main():
    collectors = load_collectors()
    while True:
        print_header()
        col_choice = select_collector(collectors)
        if col_choice.isdigit():
            col_idx = int(col_choice) - 1
            if 0 <= col_idx < len(collectors):
                collector = collectors[col_idx]
                while True:
                    run_type = select_run_type()
                    if run_type == '6':  # 뒤로 가기
                        break
                    if run_type not in ('1', '2', '3', '4', '5'):
                        print(f"{RED}잘못된 선택입니다.{RESET}")
                        continue

                    if run_type == '4':  # 고급 모드(메뉴형)
                        args, is_parallel = menu_advanced_mode()
                        if is_parallel:
                            parallel_script = collector.get('parallel_script')
                            if parallel_script and os.path.exists(parallel_script):
                                run_parallel(parallel_script, args)
                            else:
                                print(f"{YELLOW}⚠️ 병렬 스크립트 없음, 순차 실행합니다.{RESET}")
                                run_collector(collector['script'], args + ['--shard', 'odd'])
                                run_collector(collector['script'], args + ['--shard', 'even'])
                        else:
                            run_collector(collector['script'], args)
                        if not post_run_menu(collector):
                            break  # 메뉴에서 종료 선택 시

                    elif run_type == '5':  # 고급 모드(직접 입력)
                        args = direct_advanced_mode()
                        if args:
                            run_collector(collector['script'], args)
                            if not post_run_menu(collector):
                                break
                        continue

                    else:  # 기본 유형 (1,2,3)
                        base_args = get_basic_options(run_type)
                        while True:
                            mode_choice = select_mode(collector)
                            if mode_choice.isdigit():
                                m = int(mode_choice) - 1
                                modes = collector.get('modes', ['통합', 'odd 샤드', 'even 샤드', '병렬 실행'])
                                if 0 <= m < len(modes):
                                    selected_mode = modes[m]
                                    args = base_args.copy()

                                    if selected_mode == "통합":
                                        args.extend(['--shard', 'none'])
                                        run_collector(collector['script'], args)
                                    elif "odd" in selected_mode:
                                        args.extend(['--shard', 'odd'])
                                        run_collector(collector['script'], args)
                                    elif "even" in selected_mode:
                                        args.extend(['--shard', 'even'])
                                        run_collector(collector['script'], args)
                                    elif "병렬" in selected_mode:
                                        parallel_script = collector.get('parallel_script')
                                        if parallel_script and os.path.exists(parallel_script):
                                            run_parallel(parallel_script, args)
                                        else:
                                            print(f"{YELLOW}⚠️ 병렬 스크립트 없음, 순차 실행합니다.{RESET}")
                                            run_collector(collector['script'], args + ['--shard', 'odd'])
                                            run_collector(collector['script'], args + ['--shard', 'even'])
                                    else:
                                        print(f"{RED}❌ 알 수 없는 모드{RESET}")
                                        continue

                                    if not post_run_menu(collector):
                                        break
                                elif m == len(modes):
                                    break  # 수집 방식 선택으로 돌아가기
                                else:
                                    print(f"{RED}잘못된 선택입니다.{RESET}")
                            else:
                                print(f"{RED}숫자를 입력하세요.{RESET}")
            elif col_idx == len(collectors):
                print(f"{GREEN}👋 종료합니다.{RESET}")
                break
            else:
                print(f"{RED}잘못된 선택입니다.{RESET}")
        else:
            print(f"{RED}숫자를 입력하세요.{RESET}")

if __name__ == "__main__":
    main()
    