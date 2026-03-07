#!/usr/bin/env python3
"""
마스터 수집기 - 실행 유형과 수집 방식을 계층적으로 선택
- 실행 후 결과 확인 메뉴 제공 (데이터 무결성, 병합, 메트릭 생성, 다른 수집기, 종료)
- core.metrics 를 활용한 메트릭 자동 생성 (개별/일괄)
"""
import os
import sys
import json
import subprocess
import logging
import sqlite3
import shlex
from pathlib import Path
from datetime import datetime

# core 모듈 import
from core.metrics import generate_and_save_metrics
from core.database import get_db_connection

# ANSI 색상
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"

BASE_DIR = Path(__file__).parent

# 로깅 설정
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
log_filename = LOG_DIR / f"master_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def setup_logging(debug_mode=False):
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    return logging.getLogger(__name__)

logger = setup_logging()

CONFIG_FILE = BASE_DIR / "collectors.json"

# 후속 메뉴 반환값 상수
POST_RUN_CONTINUE = "continue"
POST_RUN_EXIT = "exit"

ALLOWED_TABLES = {'schools', 'meals', 'timetable', 'schedule', 'staff'}

def resolve_path(path_str):
    if not path_str:
        return None
    p = Path(path_str)
    return str(p if p.is_absolute() else BASE_DIR / p)

def validate_collector(collector, idx):
    REQUIRED_KEYS = ['name', 'description', 'script', 'db_path', 'table_name']
    missing = [key for key in REQUIRED_KEYS if key not in collector]
    if missing:
        logger.error(f"Collector #{idx} 필수 키 누락: {missing}")
        return False
    return True

def load_collectors():
    if not CONFIG_FILE.exists():
        logger.error(f"설정 파일 없음: {CONFIG_FILE}")
        print(f"{RED}❌ 설정 파일이 없습니다: {CONFIG_FILE}{RESET}")
        sys.exit(1)
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            collectors = json.load(f)
        if not isinstance(collectors, list):
            logger.error("설정 파일 최상위가 리스트가 아님")
            sys.exit(1)

        valid = []
        for i, col in enumerate(collectors):
            if validate_collector(col, i+1):
                # 경로 변환
                for key in ['script', 'parallel_script', 'merge_script', 'db_path', 'shard_odd', 'shard_even']:
                    if key in col and col[key]:
                        col[key] = resolve_path(col[key])
                # 기본 modes 설정
                if 'modes' not in col:
                    col['modes'] = ['통합', 'odd 샤드', 'even 샤드', '병렬 실행']
                # 기본 metrics_config
                if 'metrics_config' not in col:
                    col['metrics_config'] = {'enabled': False}
                # 기본 parallel_config
                if 'parallel_config' not in col:
                    col['parallel_config'] = {}
                valid.append(col)
        if not valid:
            logger.error("유효한 collector가 없습니다.")
            sys.exit(1)
        return valid
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 오류: {e}")
        sys.exit(1)

def print_header():
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📊 마스터 수집기{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")
    logger.info("마스터 수집기 시작")

def select_collector(collectors):
    print(f"\n{YELLOW}실행할 수집기를 선택하세요:{RESET}")
    for i, col in enumerate(collectors, 1):
        print(f"  {i}) {col['description']}")
    print(f"  {len(collectors)+1}) 종료")
    choice = input("선택: ").strip()
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(collectors):
            logger.info(f"수집기 선택: {collectors[idx-1]['description']}")
            return collectors[idx-1]
        elif idx == len(collectors) + 1:
            logger.info("종료 선택")
            return None
    logger.warning(f"잘못된 선택: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return "retry"

def select_run_type():
    print(f"\n{YELLOW}실행 유형을 선택하세요:{RESET}")
    print("  1) 학교 기본정보 수집 (실제 수집, 전체)")
    print("  2) 테스트 모드 (간단 로그, 제한 수집)")
    print("  3) 디버그 모드 (상세 로그)")
    print("  4) 고급 모드(메뉴형) – 단계별 옵션 선택")
    print("  5) 고급 모드(직접 입력) – 옵션 직접 입력")
    print("  6) 뒤로 가기")
    choice = input("선택: ").strip()
    if choice in ('1','2','3','4','5'):
        logger.info(f"실행 유형 선택: {choice}")
        return choice
    elif choice == '6':
        return None
    logger.warning(f"잘못된 실행 유형: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return "retry"

def select_mode(collector):
    print(f"\n{YELLOW}수집 방식을 선택하세요 ({collector['description']}):{RESET}")
    modes = collector.get('modes', ['통합', 'odd 샤드', 'even 샤드', '병렬 실행'])
    for i, mode in enumerate(modes, 1):
        print(f"  {i}) {mode}")
    print(f"  {len(modes)+1}) 뒤로 가기")
    choice = input("선택: ").strip()
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(modes):
            logger.info(f"수집 방식 선택: {modes[idx-1]}")
            return modes[idx-1]
        elif idx == len(modes) + 1:
            return None
    logger.warning(f"잘못된 수집 방식 선택: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return "retry"

def get_basic_options(run_type):
    base_args = []
    if run_type == '1':
        logger.info("실제 수집 모드")
        print(f"{YELLOW}실제 수집 모드: 전체 데이터 수집{RESET}")
    elif run_type == '2':
        base_args.extend(['--limit', '50'])
        logger.info("테스트 모드: --limit 50")
        print(f"{YELLOW}테스트 모드: --limit 50 적용{RESET}")
    elif run_type == '3':
        base_args.append('--debug')
        logger.info("디버그 모드: --debug")
        print(f"{YELLOW}디버그 모드: --debug 적용{RESET}")

    regions = input("지역 코드 (기본 전체, 여러 개는 쉼표, 예: B10,C10): ").strip()
    if regions:
        if all(part.strip() for part in regions.split(',')):
            base_args.extend(['--regions', regions])
            logger.info(f"지역 옵션: {regions}")
        else:
            logger.warning(f"잘못된 지역 형식: {regions}")
            print(f"{YELLOW}⚠️ 잘못된 형식, 무시합니다.{RESET}")

    limit = input("수집 제한 개수 (기본: 전체): ").strip()
    if limit.isdigit():
        if '--limit' in base_args:
            idx = base_args.index('--limit')
            base_args.pop(idx)
            base_args.pop(idx)
        base_args.extend(['--limit', limit])
        logger.info(f"제한 개수: {limit}")
    return base_args

def menu_advanced_mode():
    args = []
    print(f"\n{YELLOW}[고급 모드 메뉴형] 옵션을 선택하세요.{RESET}")
    logger.info("고급 모드 메뉴형 시작")

    print("\n샤드 모드를 선택하세요:")
    print("  1) 통합 (none)")
    print("  2) odd 샤드")
    print("  3) even 샤드")
    print("  4) 병렬 실행 (odd+even 동시)")
    shard_choice = input("선택 (1-4, 기본 통합): ").strip()
    if shard_choice == '2':
        args.extend(['--shard', 'odd'])
        is_parallel = False
        logger.info("샤드 모드: odd")
    elif shard_choice == '3':
        args.extend(['--shard', 'even'])
        is_parallel = False
        logger.info("샤드 모드: even")
    elif shard_choice == '4':
        is_parallel = True
        logger.info("샤드 모드: 병렬")
    else:
        args.extend(['--shard', 'none'])
        is_parallel = False
        logger.info("샤드 모드: 통합")

    regions = input("\n지역 코드 (기본 전체, 여러 개는 쉼표, 예: B10,C10): ").strip()
    if regions:
        args.extend(['--regions', regions])
        logger.info(f"지역: {regions}")

    limit = input("수집 제한 개수 (기본 전체): ").strip()
    if limit.isdigit():
        args.extend(['--limit', limit])
        logger.info(f"제한: {limit}")

    debug = input("디버그 모드? (y/n) [n]: ").strip().lower()
    if debug == 'y':
        args.append('--debug')
        logger.info("디버그 모드 ON")

    return args, is_parallel

def direct_advanced_mode():
    print(f"\n{YELLOW}[고급 모드 직접 입력] 원하는 옵션을 한 줄로 입력하세요.{RESET}")
    print("예시: --shard odd --regions B10 --limit 50 --debug")
    custom = input("옵션: ").strip()
    if custom:
        try:
            args = shlex.split(custom)
            logger.info(f"직접 입력 옵션 (파싱됨): {args}")
            return args
        except ValueError as e:
            logger.error(f"옵션 파싱 오류: {e}")
            print(f"{RED}❌ 옵션 형식 오류: {e}{RESET}")
            return []
    else:
        logger.info("직접 입력 없음")
        print(f"{YELLOW}옵션이 없습니다. 기본 실행됩니다.{RESET}")
        return []

def run_collector(script, args):
    cmd = [sys.executable, script] + args
    logger.info(f"실행 명령: {' '.join(cmd)}")
    print(f"\n{GREEN}▶ 실행: {' '.join(cmd)}{RESET}\n")
    try:
        subprocess.run(cmd, check=True)
        logger.info("수집기 정상 종료")
    except subprocess.CalledProcessError as e:
        logger.error(f"수집기 실행 오류: {e}")
        print(f"{RED}❌ 수집기 실행 중 오류 발생 (종료 코드 {e.returncode}){RESET}")
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 인터럽트 발생")
        print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
        raise

def run_parallel(parallel_script, base_args):
    if not parallel_script or not os.path.exists(parallel_script):
        logger.error(f"병렬 스크립트 없음: {parallel_script}")
        print(f"{RED}❌ 병렬 실행 스크립트가 없습니다: {parallel_script}{RESET}")
        return False
    cmd = [sys.executable, parallel_script] + base_args
    logger.info(f"병렬 실행 명령: {' '.join(cmd)}")
    print(f"\n{GREEN}▶ 병렬 실행: {' '.join(cmd)}{RESET}\n")
    try:
        subprocess.run(cmd, check=True)
        logger.info("병렬 실행 정상 종료")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"병렬 실행 오류: {e}")
        print(f"{RED}❌ 병렬 실행 중 오류 발생{RESET}")
        return False
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 병렬 실행 중단")
        print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
        raise

def get_table_count(db_path, table_name):
    if table_name not in ALLOWED_TABLES:
        logger.error(f"허용되지 않은 테이블명: {table_name}")
        return None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"DB 조회 실패 {db_path}: {e}")
        return None

def post_run_menu(collector, all_collectors):
    """실행 후 결과 확인 및 후속 작업 메뉴"""
    while True:
        print(f"\n{YELLOW}후속 작업을 선택하세요.{RESET}")
        print("  1) 데이터 무결성 확인 (레코드 수, 샤드 합계 등)")
        print("  2) 병합 실행 (샤드 파일이 있을 경우)")
        print("  3) 이 수집기 메트릭 생성")
        print("  4) 모든 수집기 메트릭 일괄 생성")
        print("  5) 다른 수집기 실행 (메인 메뉴로)")
        print("  6) 종료")
        choice = input("선택: ").strip()

        if choice == '1':
            logger.info("데이터 무결성 확인 선택")
            print(f"\n{BLUE}📊 데이터 무결성 확인{RESET}")
            db_path = collector.get('db_path')
            table = collector['table_name']
            total_db = None
            if db_path and os.path.exists(db_path):
                total_db = get_table_count(db_path, table)
                print(f"   통합 DB ({db_path}): {total_db if total_db is not None else '오류'}건")
            else:
                print(f"   통합 DB 없음")

            odd_path = collector.get('shard_odd')
            even_path = collector.get('shard_even')
            odd_count = get_table_count(odd_path, table) if odd_path and os.path.exists(odd_path) else None
            even_count = get_table_count(even_path, table) if even_path and os.path.exists(even_path) else None

            if odd_count is not None:
                print(f"   odd 샤드 ({odd_path}): {odd_count}건")
            if even_count is not None:
                print(f"   even 샤드 ({even_path}): {even_count}건")

            if odd_count is not None and even_count is not None:
                total_shard = odd_count + even_count
                print(f"   샤드 합계: {total_shard}건")
                if total_db is not None:
                    if total_shard == total_db:
                        print(f"{GREEN}   ✅ 통합 DB와 샤드 합계가 일치합니다.{RESET}")
                        logger.info(f"무결성 일치: 통합 {total_db}, 샤드 합계 {total_shard}")
                    else:
                        print(f"{RED}   ❌ 불일치! 통합 DB: {total_db}, 샤드 합계: {total_shard}{RESET}")
                        logger.warning(f"무결성 불일치: 통합 {total_db}, 샤드 합계 {total_shard}")
                else:
                    print(f"{YELLOW}   통합 DB가 없어 비교 불가{RESET}")
            elif odd_count is not None or even_count is not None:
                print(f"{YELLOW}   하나의 샤드만 존재합니다.{RESET}")

        elif choice == '2':
            logger.info("병합 실행 선택")
            merge_script = collector.get('merge_script')
            if merge_script and os.path.exists(merge_script):
                print(f"\n{GREEN}🔗 병합 스크립트 실행{RESET}")
                try:
                    subprocess.run([sys.executable, merge_script], check=True)
                    logger.info("병합 스크립트 정상 종료")
                except subprocess.CalledProcessError as e:
                    logger.error(f"병합 스크립트 오류: {e}")
                    print(f"{RED}❌ 병합 스크립트 실행 오류{RESET}")
                except KeyboardInterrupt:
                    logger.warning("병합 스크립트 사용자 중단")
                    print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
            else:
                logger.warning("병합 스크립트 없음")
                print(f"{RED}❌ 병합 스크립트가 없습니다.{RESET}")

        elif choice == '3':
            logger.info(f"{collector['name']} 메트릭 생성 선택")
            if not collector.get('metrics_config', {}).get('enabled', False):
                print(f"{YELLOW}⚠️ {collector['name']}의 메트릭 생성이 비활성화되어 있습니다.{RESET}")
                continue
            backup_date = datetime.now().strftime("%Y%m%d_%H%M")
            metrics_dir = BASE_DIR / "metrics"
            domain_config = {
                collector['name']: {
                    "db_path": collector['db_path'],
                    "table": collector['table_name'],
                    "enabled": True
                }
            }
            generate_and_save_metrics(
                backup_date=backup_date,
                base_dir=str(BASE_DIR),
                metrics_dir=str(metrics_dir),
                domain_config=domain_config,
                global_dbs=[],  # 필요시 설정
                include_geo=collector.get('metrics_config', {}).get('collect_geo', False),
                include_global_tables=collector.get('metrics_config', {}).get('collect_global', False),
                print_to_stdout=True
            )
            print(f"{GREEN}✅ {collector['name']} 메트릭 생성 완료{RESET}")

        elif choice == '4':
            logger.info("모든 수집기 메트릭 일괄 생성 선택")
            enabled_collectors = [c for c in all_collectors if c.get('metrics_config', {}).get('enabled', False)]
            if not enabled_collectors:
                print(f"{YELLOW}⚠️ 활성화된 메트릭 생성 수집기가 없습니다.{RESET}")
                continue
            domain_config = {}
            for c in enabled_collectors:
                domain_config[c['name']] = {
                    "db_path": c['db_path'],
                    "table": c['table_name'],
                    "enabled": True
                }
            backup_date = datetime.now().strftime("%Y%m%d_%H%M")
            metrics_dir = BASE_DIR / "metrics"
            # include_geo, include_global_tables는 전체 중 하나라도 true면 활성화 (단순화)
            include_geo = any(c.get('metrics_config', {}).get('collect_geo', False) for c in enabled_collectors)
            include_global = any(c.get('metrics_config', {}).get('collect_global', False) for c in enabled_collectors)
            generate_and_save_metrics(
                backup_date=backup_date,
                base_dir=str(BASE_DIR),
                metrics_dir=str(metrics_dir),
                domain_config=domain_config,
                global_dbs=[],
                include_geo=include_geo,
                include_global_tables=include_global,
                print_to_stdout=True
            )
            print(f"{GREEN}✅ 전체 메트릭 생성 완료 (활성 수집기: {len(enabled_collectors)}개){RESET}")

        elif choice == '5':
            logger.info("다른 수집기 실행 선택")
            print(f"{YELLOW}메인 메뉴로 돌아갑니다.{RESET}")
            return POST_RUN_CONTINUE

        elif choice == '6':
            logger.info("프로그램 종료 선택")
            print(f"{GREEN}👋 종료합니다.{RESET}")
            return POST_RUN_EXIT

        else:
            logger.warning(f"잘못된 후속 작업 선택: {choice}")
            print(f"{RED}잘못된 선택입니다.{RESET}")

def main():
    global logger
    try:
        collectors = load_collectors()
        while True:
            print_header()
            collector = select_collector(collectors)
            if collector is None:
                break
            if collector == "retry":
                continue

            while True:
                run_type = select_run_type()
                if run_type is None:
                    break
                if run_type == "retry":
                    continue

                if run_type == '3':
                    logger = setup_logging(debug_mode=True)
                else:
                    logger = setup_logging(debug_mode=False)

                if run_type == '4':
                    args, is_parallel = menu_advanced_mode()
                    if is_parallel:
                        run_parallel(collector.get('parallel_script'), args)
                    else:
                        run_collector(collector['script'], args)
                    result = post_run_menu(collector, collectors)
                    if result == POST_RUN_EXIT:
                        sys.exit(0)
                    elif result == POST_RUN_CONTINUE:
                        break

                elif run_type == '5':
                    args = direct_advanced_mode()
                    if args:
                        run_collector(collector['script'], args)
                        result = post_run_menu(collector, collectors)
                        if result == POST_RUN_EXIT:
                            sys.exit(0)
                        elif result == POST_RUN_CONTINUE:
                            break

                else:
                    base_args = get_basic_options(run_type)
                    while True:
                        mode = select_mode(collector)
                        if mode is None:
                            break
                        if mode == "retry":
                            continue
                        args = base_args.copy()
                        if mode == "통합":
                            args.extend(['--shard', 'none'])
                            run_collector(collector['script'], args)
                        elif "odd" in mode:
                            args.extend(['--shard', 'odd'])
                            run_collector(collector['script'], args)
                        elif "even" in mode:
                            args.extend(['--shard', 'even'])
                            run_collector(collector['script'], args)
                        elif "병렬" in mode:
                            parallel_script = collector.get('parallel_script')
                            if parallel_script and os.path.exists(parallel_script):
                                run_parallel(parallel_script, args)
                            else:
                                print(f"{YELLOW}⚠️ 병렬 스크립트 없음, 순차 실행합니다.{RESET}")
                                run_collector(collector['script'], args + ['--shard', 'odd'])
                                run_collector(collector['script'], args + ['--shard', 'even'])
                        else:
                            logger.error(f"알 수 없는 모드: {mode}")
                            continue

                        result = post_run_menu(collector, collectors)
                        if result == POST_RUN_EXIT:
                            sys.exit(0)
                        elif result == POST_RUN_CONTINUE:
                            break

    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 종료")
        print(f"\n{YELLOW}👋 사용자 종료{RESET}")
        sys.exit(0)

if __name__ == "__main__":
    main()
    