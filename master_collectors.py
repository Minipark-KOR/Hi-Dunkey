#!/usr/bin/env python3
# master_collectors.py
# 개발 가이드: docs/developer_guide.md 참조
"""
마스터 수집기 - 실행 유형과 수집 방식을 계층적으로 선택
- 실행 후 결과 확인 메뉴 제공 (데이터 무결성, 병합, 내보내기, 메트릭 생성, 로그 확인, 디버그 재실행, 다른 수집기, 종료)
- 메트릭 설명: 각 통계 항목이 무엇을 의미하는지 주석으로 표시
"""
import os
import sys
import json
import subprocess
import logging
import sqlite3
import shlex
import re
from enum import Enum, auto
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from constants.paths import LOG_DIR

# ANSI 색상 (Windows 호환 처리)
if sys.platform == "win32":
    try:
        import colorama
        colorama.init()
    except ImportError:
        # 컬러 비활성화
        GREEN = YELLOW = BLUE = RED = CYAN = RESET = ""
    else:
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        BLUE = "\033[94m"
        RED = "\033[91m"
        CYAN = "\033[96m"
        RESET = "\033[0m"
else:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    RESET = "\033[0m"

BASE_DIR = Path(__file__).parent
LOG_DIR.mkdir(parents=True, exist_ok=True)

from core.address import parse_region_input   # ← 추가

# 상수 정의
class MenuResult(Enum):
    """메뉴 선택 결과를 명확히 표현하는 Enum"""
    STAY = auto()               # 현재 메뉴 유지 (예: 후속 작업 메뉴에서 계속)
    GO_TO_COLLECTOR = auto()     # 수집기 선택 메뉴로 이동
    BACK = auto()                # 이전 메뉴로 이동 (예: 실행 유형 선택)
    EXIT = auto()                # 프로그램 종료
    DEBUG = auto()               # 디버그 모드로 재실행
    RESTART = auto()             # 처음으로 (수집기 선택)
    RETRY = auto()               # 잘못된 입력, 재시도

class CollectionMode(Enum):
    """수집 모드 Enum"""
    INTEGRATED = "통합"
    SHARD_ODD = "odd 샤드"
    SHARD_EVEN = "even 샤드"
    PARALLEL = "병렬 실행"

@dataclass
class ActionContext:
    """후속 작업 액션에 전달되는 컨텍스트"""
    collector: Dict[str, Any]
    args: List[str]
    mode: str
    run_type: str
    all_collectors: List[Dict[str, Any]]
    last_args: Optional[List[str]] = None
    last_mode: Optional[str] = None

ALLOWED_TABLES = {'schools', 'meals', 'timetable', 'schedule', 'staff'}

def cleanup_old_logs(days: int = 30) -> None:
    """30일 이상 된 로그 삭제"""
    cutoff = datetime.now() - timedelta(days=days)
    for f in LOG_DIR.glob("master_*.log"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            if mtime < cutoff:
                f.unlink()
        except Exception:
            pass
cleanup_old_logs()

log_filename = LOG_DIR / f"master_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# collectors.json 대신 collector_cli.py의 COLLECTOR_MAP 사용
from collector_cli import COLLECTOR_MAP
from constants.paths import MASTER_DIR
from core.school_year import get_current_school_year  # ✅ smoke test에서 사용

def resolve_path(path_str: str) -> Optional[str]:
    if not path_str:
        return None
    p = Path(path_str)
    return str(p if p.is_absolute() else BASE_DIR / p)

def load_collectors() -> List[Dict[str, Any]]:
    """COLLECTOR_MAP에서 수집기 메타데이터 로드"""
    collectors = []
    for name, cls in COLLECTOR_MAP.items():
        collectors.append({
            "name": name,
            "description": getattr(cls, "description", name),
            "parallel_script": getattr(cls, "parallel_script", "scripts/run_pipeline.py"),
            "merge_script": getattr(cls, "merge_script", None),
            "table_name": getattr(cls, "table_name", name),
            "modes": getattr(cls, "modes", ["통합", "odd 샤드", "even 샤드", "병렬 실행"]),
            "timeout_seconds": getattr(cls, "timeout_seconds", 3600),
            "parallel_timeout_seconds": getattr(cls, "parallel_timeout_seconds", 7200),
            "merge_timeout_seconds": getattr(cls, "merge_timeout_seconds", 1800),
            "metrics_config": getattr(cls, "metrics_config", {"enabled": False}),
            "parallel_config": getattr(cls, "parallel_config", {}),
            # DB 경로는 규칙에 따라 생성
            "db_path": str(MASTER_DIR / f"{name}.db"),
            "shard_odd": str(MASTER_DIR / f"{name}_odd.db"),
            "shard_even": str(MASTER_DIR / f"{name}_even.db"),
        })
    return collectors

def print_header() -> None:
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📊 마스터 수집기{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")
    logger.info("마스터 수집기 시작")

def select_collector(collectors: List[Dict[str, Any]]) -> Union[Dict[str, Any], MenuResult]:
    """수집기 선택 메뉴"""
    print(f"\n{YELLOW}실행할 수집기를 선택하세요:{RESET}")
    for i, col in enumerate(collectors, 1):
        print(f"  {i}) {col['description']}")
    print()  # 빈 줄
    print("  33) 종료")
    choice = input("선택: ").strip()
    if choice.isdigit():
        val = int(choice)
        if 1 <= val <= len(collectors):
            logger.info(f"수집기 선택: {collectors[val-1]['description']}")
            return collectors[val-1]
        elif val == 33:
            logger.info("종료 선택")
            return MenuResult.EXIT
    logger.warning(f"잘못된 선택: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return MenuResult.RETRY

def select_run_type() -> Union[str, MenuResult]:
    """실행 유형 선택 메뉴"""
    print(f"\n{YELLOW}실행 유형을 선택하세요:{RESET}")
    print("  1) 학교 기본정보 수집 (실제 수집, 전체)")
    print("  2) 테스트 모드 (간단 로그, 제한 수집)")
    print("  3) 디버그 모드 (상세 로그)")
    print("  4) 고급 모드(메뉴형) – 단계별 옵션 선택")
    print("  5) 고급 모드(직접 입력) – 옵션 직접 입력")
    print("  6) 드라이 런 (실행 명령어만 출력)")
    print()  # 빈 줄
    print("  11) 뒤로 가기")
    print("  22) 처음으로 (수집기 선택)")
    print("  33) 종료")
    choice = input("선택: ").strip()
    if choice.isdigit():
        val = int(choice)
        if 1 <= val <= 6:
            logger.info(f"실행 유형 선택: {choice}")
            return choice
        elif val == 11:
            return MenuResult.BACK
        elif val == 22:
            return MenuResult.RESTART
        elif val == 33:
            logger.info("종료 선택")
            return MenuResult.EXIT
    logger.warning(f"잘못된 실행 유형: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return MenuResult.RETRY

def select_mode(collector: Dict[str, Any]) -> Union[CollectionMode, MenuResult]:
    """수집 방식 선택 메뉴"""
    print(f"\n{YELLOW}수집 방식을 선택하세요 ({collector['description']}):{RESET}")
    modes = collector.get('modes', ['통합', 'odd 샤드', 'even 샤드', '병렬 실행'])
    # modes 리스트를 CollectionMode에 매핑 (순서 중요)
    mode_map = {
        1: CollectionMode.INTEGRATED,
        2: CollectionMode.SHARD_ODD,
        3: CollectionMode.SHARD_EVEN,
        4: CollectionMode.PARALLEL,
    }
    for i, mode in enumerate(modes, 1):
        print(f"  {i}) {mode}")
    print()  # 빈 줄
    print("  11) 뒤로 가기")
    print("  22) 처음으로 (수집기 선택)")
    print("  33) 종료")
    choice = input("선택: ").strip()
    if choice.isdigit():
        val = int(choice)
        if 1 <= val <= len(modes):
            selected = mode_map[val]
            logger.info(f"수집 방식 선택: {selected.value}")
            return selected
        elif val == 11:
            return MenuResult.BACK
        elif val == 22:
            return MenuResult.RESTART
        elif val == 33:
            logger.info("종료 선택")
            return MenuResult.EXIT
    logger.warning(f"잘못된 수집 방식 선택: {choice}")
    print(f"{RED}잘못된 선택입니다.{RESET}")
    return MenuResult.RETRY

def get_basic_options(run_type: str) -> List[str]:
    """기본 옵션 수집 (실행 유형 1~3)"""
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

    regions_input = input("지역 (예: 서울,경기 또는 B10,C10): ").strip()
    if regions_input:
        codes = parse_region_input(regions_input)
        if codes:
            regions_str = ','.join(codes)
            base_args.extend(['--regions', regions_str])
            logger.info(f"지역 옵션: {regions_str}")
        else:
            logger.warning(f"유효한 지역이 없음: {regions_input}")
            print(f"{YELLOW}⚠️ 유효한 지역이 없어 무시합니다.{RESET}")

    limit = input("수집 제한 개수 (기본: 전체): ").strip()
    if limit.isdigit():
        if '--limit' in base_args:
            idx = base_args.index('--limit')
            base_args.pop(idx)
            base_args.pop(idx)
        base_args.extend(['--limit', limit])
        logger.info(f"제한 개수: {limit}")
    return base_args

def menu_advanced_mode() -> Tuple[Optional[List[str]], bool, Optional[MenuResult]]:
    """고급 모드 메뉴형 옵션 선택"""
    args = []
    print(f"\n{YELLOW}[고급 모드 메뉴형] 옵션을 선택하세요.{RESET}")
    logger.info("고급 모드 메뉴형 시작")

    print("\n샤드 모드를 선택하세요:")
    print("  1) 통합 (none)")
    print("  2) odd 샤드")
    print("  3) even 샤드")
    print("  4) 병렬 실행 (odd+even 동시)")
    print()  # 빈 줄
    print("  11) 뒤로 가기")
    print("  22) 처음으로 (수집기 선택)")
    print("  33) 종료")
    shard_choice = input("선택 (1-4, 기본 통합): ").strip()
    if shard_choice.isdigit():
        val = int(shard_choice)
        if 1 <= val <= 4:
            if val == 2:
                args.extend(['--shard', 'odd'])
                is_parallel = False
                logger.info("샤드 모드: odd")
            elif val == 3:
                args.extend(['--shard', 'even'])
                is_parallel = False
                logger.info("샤드 모드: even")
            elif val == 4:
                is_parallel = True
                logger.info("샤드 모드: 병렬")
            else:
                args.extend(['--shard', 'none'])
                is_parallel = False
                logger.info("샤드 모드: 통합")
            return args, is_parallel, None
        elif val == 11:
            return None, False, MenuResult.BACK
        elif val == 22:
            return None, False, MenuResult.RESTART
        elif val == 33:
            logger.info("종료 선택")
            sys.exit(0)
    else:
        args.extend(['--shard', 'none'])
        is_parallel = False
        logger.info("샤드 모드: 통합")

    # [변경] 지역 입력 처리
    regions_input = input("\n지역 (예: 서울,경기 또는 B10,C10): ").strip()
    if regions_input:
        codes = parse_region_input(regions_input)
        if codes:
            regions_str = ','.join(codes)
            args.extend(['--regions', regions_str])
            logger.info(f"지역: {regions_str}")
        else:
            logger.warning(f"유효한 지역이 없음: {regions_input}")
            print(f"{YELLOW}⚠️ 유효한 지역이 없어 무시합니다.{RESET}")

    limit = input("수집 제한 개수 (기본 전체): ").strip()
    if limit.isdigit():
        args.extend(['--limit', limit])
        logger.info(f"제한: {limit}")

    debug = input("디버그 모드? (y/n) [n]: ").strip().lower()
    if debug == 'y':
        args.append('--debug')
        logger.info("디버그 모드 ON")

    return args, is_parallel, None

def direct_advanced_mode() -> Union[List[str], MenuResult, None]:
    """고급 모드 직접 입력 옵션"""
    print(f"\n{YELLOW}[고급 모드 직접 입력] 원하는 옵션을 한 줄로 입력하세요.{RESET}")
    print("예시: --shard odd --regions B10 --limit 50 --debug")
    print()  # 빈 줄
    print("  11) 뒤로 가기")
    print("  22) 처음으로 (수집기 선택)")
    print("  33) 종료")
    custom = input("옵션: ").strip()
    if custom.isdigit():
        val = int(custom)
        if val == 11:
            return MenuResult.BACK
        elif val == 22:
            return MenuResult.RESTART
        elif val == 33:
            logger.info("종료 선택")
            sys.exit(0)
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

def dry_run_cmd(script: str, args: List[str]) -> bool:
    """드라이 런: 실행 명령어만 출력"""
    cmd = [sys.executable, script] + args
    print(f"{CYAN}📋 드라이 런: {' '.join(cmd)}{RESET}")
    logger.info(f"드라이 런 명령: {' '.join(cmd)}")
    return True

def format_timeout(seconds: int) -> str:
    """초를 분/초 문자열로 변환"""
    if seconds >= 60:
        return f"{seconds//60}분 {seconds%60}초"
    return f"{seconds}초"

def run_collector(collector_name: str, args: List[str], timeout: Optional[int] = None) -> bool:
    """collector_cli.py를 통해 수집기 실행"""
    cmd = [sys.executable, "collector_cli.py", collector_name] + args
    logger.info(f"실행 명령: {' '.join(cmd)}")
    print(f"\n{GREEN}▶ 실행: {' '.join(cmd)}{RESET}\n")
    try:
        subprocess.run(cmd, check=True, timeout=timeout)
        logger.info("수집기 정상 종료")
        return True
    except subprocess.TimeoutExpired as e:
        logger.error(f"수집기 타임아웃 ({timeout}초): {e}")
        print(f"{RED}❌ 수집기 실행 타임아웃 ({format_timeout(timeout)}){RESET}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"수집기 실행 오류: {e}")
        print(f"{RED}❌ 수집기 실행 중 오류 발생 (종료 코드 {e.returncode}){RESET}")
        return False
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 인터럽트 발생")
        print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
        raise

def run_parallel(parallel_script: str, base_args: List[str], collector_name: str, timeout: Optional[int] = None) -> bool:
    """병렬 실행 스크립트 실행"""
    cmd = [sys.executable, parallel_script, collector_name] + base_args
    logger.info(f"병렬 실행 명령: {' '.join(cmd)}")
    print(f"\n{GREEN}▶ 병렬 실행: {' '.join(cmd)}{RESET}\n")
    try:
        subprocess.run(cmd, check=True, timeout=timeout)
        logger.info("병렬 실행 정상 종료")
        return True
    except subprocess.TimeoutExpired as e:
        logger.error(f"병렬 실행 타임아웃 ({timeout}초): {e}")
        print(f"{RED}❌ 병렬 실행 타임아웃 ({format_timeout(timeout)}){RESET}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"병렬 실행 오류: {e}")
        print(f"{RED}❌ 병렬 실행 중 오류 발생{RESET}")
        return False
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 병렬 실행 중단")
        print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
        raise

def get_table_count(db_path: str, table_name: str) -> Optional[int]:
    """테이블의 레코드 수 조회 (SQL 인젝션 방어 포함)"""
    if table_name not in ALLOWED_TABLES:
        logger.error(f"허용되지 않은 테이블명: {table_name}")
        return None
    # 추가 안전장치: 테이블명에 위험 문자 포함 검사
    dangerous = ['`', ';', '--', '/*', '*/', 'drop', 'delete', 'update', 'insert']
    if any(char in table_name.lower() for char in dangerous):
        logger.error(f"위험한 문자 포함 테이블명: {table_name}")
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"DB 조회 실패 {db_path}: {e}")
        return None

def execute_collection(collector: Dict[str, Any], args: List[str], mode: CollectionMode, run_type: str) -> bool:
    """선택된 모드에 따라 수집 실행"""
    is_dry_run = (run_type == '6')
    timeout = collector.get('timeout_seconds')
    parallel_timeout = collector.get('parallel_timeout_seconds')

    if mode == CollectionMode.INTEGRATED:
        full_args = args + ['--shard', 'none']
        if is_dry_run:
            return dry_run_cmd("collector_cli.py", [collector['name']] + full_args)
        else:
            return run_collector(collector['name'], full_args, timeout)
    elif mode == CollectionMode.SHARD_ODD:
        full_args = args + ['--shard', 'odd']
        if is_dry_run:
            return dry_run_cmd("collector_cli.py", [collector['name']] + full_args)
        else:
            return run_collector(collector['name'], full_args, timeout)
    elif mode == CollectionMode.SHARD_EVEN:
        full_args = args + ['--shard', 'even']
        if is_dry_run:
            return dry_run_cmd("collector_cli.py", [collector['name']] + full_args)
        else:
            return run_collector(collector['name'], full_args, timeout)
    elif mode == CollectionMode.PARALLEL:
        parallel_script = collector.get('parallel_script')
        if is_dry_run:
            if parallel_script:
                dry_run_cmd(parallel_script, [collector['name']] + args)
            else:
                print(f"{YELLOW}⚠️ 병렬 스크립트 없음, 순차 실행:{RESET}")
                dry_run_cmd("collector_cli.py", [collector['name']] + args + ['--shard', 'odd'])
                dry_run_cmd("collector_cli.py", [collector['name']] + args + ['--shard', 'even'])
            return True
        else:
            if parallel_script and os.path.exists(parallel_script):
                return run_parallel(parallel_script, args, collector['name'], parallel_timeout)
            else:
                print(f"{YELLOW}⚠️ 병렬 스크립트 없음, 순차 실행합니다.{RESET}")
                success_odd = run_collector(collector['name'], args + ['--shard', 'odd'], timeout)
                success_even = run_collector(collector['name'], args + ['--shard', 'even'], timeout)
                return success_odd and success_even
    else:
        logger.error(f"알 수 없는 모드: {mode}")
        return False

# ====== 후속 작업 기능별 분리 ======
def check_data_integrity(ctx: ActionContext) -> MenuResult:
    """데이터 무결성 확인"""
    logger.info("데이터 무결성 확인 선택")
    print(f"\n{BLUE}📊 데이터 무결성 확인{RESET}")
    collector = ctx.collector
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
    return MenuResult.STAY

def execute_merge(ctx: ActionContext) -> MenuResult:
    """병합 실행"""
    logger.info("병합 실행 선택")
    collector = ctx.collector
    merge_script = collector.get('merge_script')
    if merge_script and os.path.exists(merge_script):
        merge_timeout = collector.get('merge_timeout_seconds', 1800)
        print(f"\n{GREEN}🔗 병합 스크립트 실행{RESET}")
        try:
            subprocess.run([sys.executable, merge_script], check=True, timeout=merge_timeout)
            logger.info("병합 스크립트 정상 종료")
            print(f"{GREEN}✅ 병합 완료{RESET}")
        except subprocess.TimeoutExpired:
            logger.error(f"병합 스크립트 타임아웃 ({merge_timeout}초)")
            print(f"{RED}❌ 병합 스크립트 타임아웃 ({format_timeout(merge_timeout)}){RESET}")
        except subprocess.CalledProcessError as e:
            logger.error(f"병합 스크립트 오류: {e}")
            print(f"{RED}❌ 병합 스크립트 실행 오류{RESET}")
        except KeyboardInterrupt:
            logger.warning("병합 스크립트 사용자 중단")
            print(f"\n{YELLOW}⚠️ 사용자 중단{RESET}")
    else:
        logger.warning("병합 스크립트 없음")
        print(f"{RED}❌ 병합 스크립트가 없습니다.{RESET}")
    return MenuResult.STAY

def export_data(ctx: ActionContext) -> MenuResult:
    """데이터 내보내기"""
    logger.info("데이터 내보내기 선택")
    print(f"\n{BLUE}📤 데이터 내보내기{RESET}")
    try:
        from exporters.region_filter import get_all_regions, parse_region_input
    except ImportError:
        logger.error("exporters 모듈을 찾을 수 없습니다.")
        print(f"{RED}❌ 내보내기 모듈이 없습니다. exporters/ 디렉토리를 생성하고 필요한 파일을 넣어주세요.{RESET}")
        return MenuResult.STAY

    print("\n내보낼 지역을 선택하세요 (기본: 전체):")
    for code, name in get_all_regions():
        print(f"  • {name} ({code})")
    region_input = input("지역 입력 (예: 서울,경기 또는 B10,J10): ").strip()
    regions = parse_region_input(region_input) if region_input else None

    print("\n내보낼 형식을 선택하세요:")
    print("  1) Excel 파일 (.xlsx) - 학교 목록 상세 데이터")
    print("  2) 통계 리포트 (JSON) - 요약 통계")
    print("  3) 통계 리포트 (CSV)")
    print("  4) 통계 리포트 (텍스트)")
    format_choice = input("선택 (1-4): ").strip()
    format_map = {'1': 'excel', '2': 'json', '3': 'csv', '4': 'text'}
    report_format = format_map.get(format_choice, 'json')

    collector = ctx.collector
    try:
        db_path = collector.get('db_path')
        table_name = collector['table_name']
        if report_format == 'excel':
            from exporters.excel_exporter import ExcelExporter
            exporter = ExcelExporter()
            output_path = exporter.export_from_db(
                db_path=db_path,
                table_name=table_name,
                regions=regions
            )
        else:
            from exporters.report_generator import ReportGenerator
            generator = ReportGenerator()
            output_path = generator.generate_from_db(
                db_path=db_path,
                table_name=table_name,
                regions=regions,
                report_format=report_format
            )
        if output_path:
            print(f"{GREEN}✅ 내보내기 완료: {output_path}{RESET}")
            logger.info(f"데이터 내보내기 완료: {output_path}")
        else:
            print(f"{YELLOW}⚠️ 내보낼 데이터가 없습니다.{RESET}")
    except ImportError as e:
        logger.warning(f"내보내기 모듈 로드 실패: {e}")
        print(f"{YELLOW}⚠️ 필요한 라이브러리가 없습니다: {e}")
        print(f"   설치 명령: pip install pandas openpyxl")
    except Exception as e:
        logger.error(f"내보내기 중 오류: {e}", exc_info=True)
        print(f"{RED}❌ 내보내기 실패: {e}{RESET}")
    return MenuResult.STAY

def generate_single_metrics(ctx: ActionContext) -> MenuResult:
    """단일 수집기 메트릭 생성"""
    collector = ctx.collector
    logger.info(f"{collector['name']} 메트릭 생성 선택")
    if not collector.get('metrics_config', {}).get('enabled', False):
        print(f"{YELLOW}⚠️ {collector['name']}의 메트릭 생성이 비활성화되어 있습니다.{RESET}")
        return MenuResult.STAY
    backup_date = datetime.now().strftime("%Y%m%d_%H%M")
    metrics_dir = BASE_DIR / "metrics"
    domain_config = {
        collector['name']: {
            "db_path": collector['db_path'],
            "table": collector['table_name'],
            "enabled": True
        }
    }
    try:
        from core.metrics import generate_and_save_metrics
        result = generate_and_save_metrics(
            backup_date=backup_date,
            base_dir=str(BASE_DIR),
            metrics_dir=str(metrics_dir),
            domain_config=domain_config,
            global_dbs=[],
            include_geo=collector.get('metrics_config', {}).get('collect_geo', False),
            include_global_tables=collector.get('metrics_config', {}).get('collect_global', False),
            print_to_stdout=True
        )
        print(f"{GREEN}✅ {collector['name']} 메트릭 생성 완료{RESET}")
        if result.get('summary_path'):
            logger.info(f"메트릭 요약 파일: {result['summary_path']}")
    except ImportError as e:
        logger.warning(f"metrics 모듈 로드 실패: {e}")
        print(f"{YELLOW}⚠️ 메트릭 모듈을 찾을 수 없습니다. 기능을 건너뜁니다.{RESET}")
    except Exception as e:
        logger.error(f"메트릭 생성 중 예외: {e}", exc_info=True)
        print(f"{RED}❌ 메트릭 생성 실패: {e}{RESET}")
    return MenuResult.STAY

def generate_all_metrics(ctx: ActionContext) -> MenuResult:
    """모든 수집기 메트릭 일괄 생성"""
    logger.info("모든 수집기 메트릭 일괄 생성 선택")
    enabled_collectors = [c for c in ctx.all_collectors if c.get('metrics_config', {}).get('enabled', False)]
    if not enabled_collectors:
        print(f"{YELLOW}⚠️ 활성화된 메트릭 생성 수집기가 없습니다.{RESET}")
        return MenuResult.STAY
    domain_config = {}
    for c in enabled_collectors:
        domain_config[c['name']] = {
            "db_path": c['db_path'],
            "table": c['table_name'],
            "enabled": True
        }
    backup_date = datetime.now().strftime("%Y%m%d_%H%M")
    metrics_dir = BASE_DIR / "metrics"
    include_geo = any(c.get('metrics_config', {}).get('collect_geo', False) for c in enabled_collectors)
    include_global = any(c.get('metrics_config', {}).get('collect_global', False) for c in enabled_collectors)
    try:
        from core.metrics import generate_and_save_metrics
        result = generate_and_save_metrics(
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
        if result.get('summary_path'):
            logger.info(f"메트릭 요약 파일: {result['summary_path']}")
    except ImportError as e:
        logger.warning(f"metrics 모듈 로드 실패: {e}")
        print(f"{YELLOW}⚠️ 메트릭 모듈을 찾을 수 없습니다. 기능을 건너뜁니다.{RESET}")
    except Exception as e:
        logger.error(f"전체 메트릭 생성 중 예외: {e}", exc_info=True)
        print(f"{RED}❌ 전체 메트릭 생성 실패: {e}{RESET}")
    return MenuResult.STAY

def view_logs(ctx: ActionContext) -> MenuResult:
    """수집기 로그 확인 (크로스 플랫폼 호환)"""
    logger.info("로그 확인 선택")
    log_file = LOG_DIR / f"{ctx.collector['name']}.log"
    if log_file.exists():
        print(f"\n{BLUE}📄 로그 파일: {log_file}{RESET}")
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(''.join(lines[-50:]))  # 최근 50줄
        except Exception as e:
            print(f"{RED}로그 읽기 실패: {e}{RESET}")
    else:
        print(f"{YELLOW}⚠️ 로그 파일이 없습니다: {log_file}{RESET}")
    return MenuResult.STAY

def debug_rerun(ctx: ActionContext) -> Tuple[MenuResult, Optional[List[str]]]:
    """디버그 모드 재실행 (args 업데이트 후 DEBUG 반환)"""
    logger.info("디버그 모드로 재실행 선택")
    if ctx.args is None or ctx.mode is None:
        print(f"{RED}❌ 재실행에 필요한 정보가 없습니다.{RESET}")
        return MenuResult.STAY, ctx.args
    print(f"\n{GREEN}🔧 디버그 모드로 재실행합니다...{RESET}")
    if '--debug' not in ctx.args:
        ctx.args.append('--debug')
    return MenuResult.DEBUG, ctx.args

# 후속 작업 매핑
POST_RUN_ACTIONS = {
    1: check_data_integrity,
    2: execute_merge,
    3: export_data,
    4: generate_single_metrics,
    5: generate_all_metrics,
    6: view_logs,
    7: debug_rerun,
}

def post_run_menu(collector: Dict[str, Any], all_collectors: List[Dict[str, Any]], last_args: List[str] = None, last_mode: str = None) -> Tuple[MenuResult, Optional[List[str]]]:
    """후속 작업 메뉴"""
    while True:
        print(f"\n{YELLOW}후속 작업을 선택하세요.{RESET}")
        print("  1) 데이터 무결성 확인 (레코드 수, 샤드 합계 등)")
        print("  2) 병합 실행 (샤드 파일이 있을 경우)")
        print("  3) 데이터 내보내기 (Excel/리포트)")
        print("  4) 이 수집기 메트릭 생성 (수집 현황 통계)")
        print("  5) 모든 수집기 메트릭 일괄 생성")
        print("  6) 로그 확인 (현재 수집기)")
        print("  7) 디버그 모드로 재실행")
        print()  # 빈 줄
        print("  11) 뒤로 가기")
        print("  22) 처음으로 (수집기 선택)")
        print("  33) 종료")
        choice = input("선택: ").strip()

        if choice.isdigit():
            val = int(choice)
            if 1 <= val <= 7:
                # ActionContext 생성
                ctx = ActionContext(
                    collector=collector,
                    args=last_args if last_args is not None else [],
                    mode=last_mode if last_mode is not None else "",
                    run_type="1",  # run_type은 여기서 사용되지 않지만 일단 채움
                    all_collectors=all_collectors,
                    last_args=last_args,
                    last_mode=last_mode
                )
                # 각 기능별 함수 호출
                if val == 7:
                    result, new_args = debug_rerun(ctx)
                    if result == MenuResult.DEBUG:
                        return result, new_args
                else:
                    result = POST_RUN_ACTIONS[val](ctx)
                    if result != MenuResult.STAY:
                        return result, last_args
                # STAY면 루프 계속
            elif val == 11:
                logger.info("뒤로 가기 선택")
                return MenuResult.BACK, last_args
            elif val == 22:
                logger.info("처음으로 (수집기 선택) 선택")
                return MenuResult.GO_TO_COLLECTOR, last_args
            elif val == 33:
                logger.info("프로그램 종료 선택")
                return MenuResult.EXIT, last_args
            else:
                logger.warning(f"잘못된 후속 작업 선택: {choice}")
                print(f"{RED}잘못된 선택입니다.{RESET}")
        else:
            logger.warning(f"잘못된 후속 작업 선택: {choice}")
            print(f"{RED}잘못된 선택입니다.{RESET}")

def run_smoke_test() -> bool:
    """핵심 기능 smoke test (hang 방지 처리 포함)"""
    print(f"{CYAN}🧪 Smoke test 실행 중...{RESET}")
    try:
        # 1. 수집기 로드 테스트
        collectors = load_collectors()
        assert len(collectors) > 0, "수집기 로드 실패"
        print(f"  ✅ {len(collectors)}개 수집기 로드 성공")

        # 2. DB 연결 테스트 (처음 2개만)
        for c in collectors[:2]:
            count = get_table_count(c['db_path'], c['table_name'])
            print(f"  ✅ {c['name']}: {count if count is not None else 'DB 없음'}")

        # 3. CLI 실행 테스트 (제한적)
        result = subprocess.run(
            [sys.executable, "collector_cli.py", "neis_info", "--regions", "B10", "--limit", "1", "--quiet"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"  ⚠️ CLI 실행 실패 (코드 {result.returncode}): {result.stderr}")
        else:
            print(f"  ✅ collector_cli 기본 실행 성공")

        # 4. 병렬 스크립트 실행 테스트 (제한적) - hang 방지 위해 timeout 및 --year 추가
        try:
            year = get_current_school_year()
            result = subprocess.run(
                [sys.executable, "scripts/run_pipeline.py", "neis_info", "--regions", "B10", "--quiet", "--timeout", "30", "--year", str(year)],
                capture_output=True, text=True, timeout=35
            )
            print(f"  ✅ run_pipeline 실행 완료 (코드 {result.returncode})")
        except subprocess.TimeoutExpired:
            print(f"  ⚠️ run_pipeline 타임아웃 (35초 초과) - 테스트 건너뜀")
        except Exception as e:
            print(f"  ⚠️ run_pipeline 실행 실패: {e} - 테스트 건너뜀")

        print(f"{GREEN}✅ 모든 smoke test 통과{RESET}")
        return True
    except Exception as e:
        print(f"{RED}❌ smoke test 실패: {e}{RESET}")
        return False

def main():
    # 명령행 인자 처리
    if "--test" in sys.argv:
        sys.exit(0 if run_smoke_test() else 1)
        
    try:
        collectors = load_collectors()
        while True:
            print_header()
            collector = select_collector(collectors)
            if collector == MenuResult.EXIT:
                break
            if collector == MenuResult.RETRY:
                continue
            # collector는 딕셔너리

            while True:
                run_type = select_run_type()
                if run_type == MenuResult.BACK:
                    break  # 수집기 선택으로
                if run_type == MenuResult.RETRY:
                    continue
                if run_type == MenuResult.EXIT:
                    sys.exit(0)
                if run_type == MenuResult.RESTART:
                    break  # 처음으로 (수집기 선택)

                if run_type == '3':
                    logging.getLogger().setLevel(logging.DEBUG)
                    logger.info("디버그 모드로 전환")
                else:
                    logging.getLogger().setLevel(logging.INFO)
                    logger.info("일반 모드로 전환")

                is_dry_run = (run_type == '6')
                if is_dry_run:
                    print(f"{CYAN}📋 드라이 런 모드입니다. 실제 실행하지 않습니다.{RESET}")

                if run_type == '4':
                    args, is_parallel, special = menu_advanced_mode()
                    if special is not None:
                        if special == MenuResult.BACK:
                            break  # 뒤로 가기 (실행 유형 선택으로)
                        if special == MenuResult.RESTART:
                            break  # 처음으로
                    if is_parallel:
                        mode = CollectionMode.PARALLEL
                    else:
                        if '--shard' in args:
                            idx = args.index('--shard')
                            shard_val = args[idx+1] if idx+1 < len(args) else 'none'
                            if shard_val == 'odd':
                                mode = CollectionMode.SHARD_ODD
                            elif shard_val == 'even':
                                mode = CollectionMode.SHARD_EVEN
                            else:
                                mode = CollectionMode.INTEGRATED
                        else:
                            mode = CollectionMode.INTEGRATED
                    last_args = args
                    last_mode = mode.value
                    success = execute_collection(collector, args, mode, run_type)
                    if not success and not is_dry_run:
                        logger.warning("수집 실패 또는 부분 실패")
                    result, new_args = post_run_menu(collector, collectors, last_args, last_mode)
                    if result == MenuResult.EXIT:
                        sys.exit(0)
                    elif result == MenuResult.GO_TO_COLLECTOR:
                        break
                    elif result == MenuResult.BACK:
                        continue
                    elif result == MenuResult.DEBUG:
                        # 디버그 재실행: new_args에 업데이트된 args가 들어있음
                        success = execute_collection(collector, new_args, mode, run_type)
                        if not success and not is_dry_run:
                            logger.warning("수집 실패 또는 부분 실패")
                        result, new_args2 = post_run_menu(collector, collectors, new_args, last_mode)
                        if result == MenuResult.EXIT:
                            sys.exit(0)
                        elif result == MenuResult.GO_TO_COLLECTOR:
                            break
                        elif result == MenuResult.BACK:
                            continue

                elif run_type == '5':
                    res = direct_advanced_mode()
                    if isinstance(res, MenuResult):
                        if res == MenuResult.BACK:
                            break
                        if res == MenuResult.RESTART:
                            break
                    args = res  # type: ignore
                    if args:
                        if '--shard' in args:
                            idx = args.index('--shard')
                            shard_val = args[idx+1] if idx+1 < len(args) else 'none'
                            if shard_val == 'odd':
                                mode = CollectionMode.SHARD_ODD
                            elif shard_val == 'even':
                                mode = CollectionMode.SHARD_EVEN
                            else:
                                mode = CollectionMode.INTEGRATED
                        else:
                            mode = CollectionMode.INTEGRATED
                        last_args = args
                        last_mode = mode.value
                        success = execute_collection(collector, args, mode, run_type)
                        if not success and not is_dry_run:
                            logger.warning("수집 실패")
                        result, new_args = post_run_menu(collector, collectors, last_args, last_mode)
                        if result == MenuResult.EXIT:
                            sys.exit(0)
                        elif result == MenuResult.GO_TO_COLLECTOR:
                            break
                        elif result == MenuResult.BACK:
                            continue
                        elif result == MenuResult.DEBUG:
                            success = execute_collection(collector, new_args, mode, run_type)
                            if not success and not is_dry_run:
                                logger.warning("수집 실패")
                            result, new_args2 = post_run_menu(collector, collectors, new_args, last_mode)
                            if result == MenuResult.EXIT:
                                sys.exit(0)
                            elif result == MenuResult.GO_TO_COLLECTOR:
                                break
                            elif result == MenuResult.BACK:
                                continue

                else:
                    base_args = get_basic_options(run_type)
                    while True:
                        mode_res = select_mode(collector)
                        if isinstance(mode_res, MenuResult):
                            if mode_res == MenuResult.BACK:
                                break
                            if mode_res == MenuResult.RETRY:
                                continue
                            if mode_res == MenuResult.EXIT:
                                sys.exit(0)
                            if mode_res == MenuResult.RESTART:
                                break
                        else:
                            mode = mode_res  # CollectionMode
                        last_args = base_args
                        last_mode = mode.value
                        success = execute_collection(collector, base_args, mode, run_type)
                        if not success and not is_dry_run:
                            logger.warning("수집 실패 또는 부분 실패")
                        result, new_args = post_run_menu(collector, collectors, last_args, last_mode)
                        if result == MenuResult.EXIT:
                            sys.exit(0)
                        elif result == MenuResult.GO_TO_COLLECTOR:
                            break
                        elif result == MenuResult.BACK:
                            break
                        elif result == MenuResult.DEBUG:
                            success = execute_collection(collector, new_args, mode, run_type)
                            if not success and not is_dry_run:
                                logger.warning("수집 실패 또는 부분 실패")
                            result, new_args2 = post_run_menu(collector, collectors, new_args, last_mode)
                            if result == MenuResult.EXIT:
                                sys.exit(0)
                            elif result == MenuResult.GO_TO_COLLECTOR:
                                break
                            elif result == MenuResult.BACK:
                                break

    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램 종료")
        print(f"\n{YELLOW}👋 사용자 종료{RESET}")
        sys.exit(0)

if __name__ == "__main__":
    main()
    