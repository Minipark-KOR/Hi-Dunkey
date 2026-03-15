#!/usr/bin/env python3
# master_collectors.py
# 중앙 제어탑 + 대시보드

import sys
import argparse
import sqlite3
import os
import shlex
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 sys.path 에 추가
sys.path.append(str(Path(__file__).parent))

from scripts.collector import get_registered_collectors
from constants.paths import MASTER_DIR
from constants.codes import REGION_NAMES, ALL_REGIONS
from constants.domains import resolve_collector_name, validate_name_resolution_map
from core.school.year import get_current_school_year
from core.kst_time import now_kst


def list_collectors():
    """모든 수집기 목록 표시"""
    collectors = get_registered_collectors()
    
    print("\n" + "="*80)
    print("📋 등록됨 수집기 목록")
    print("="*80)
    print(f"{'이름':<20} {'테이블':<25} {'스키마':<20} {'설명':<30}")
    print("-"*80)
    
    for name, cls in sorted(collectors.items()):
        table = getattr(cls, 'table_name', None) or 'N/A'
        schema = getattr(cls, 'schema_name', None) or 'N/A'
        desc = str(getattr(cls, 'description', None) or 'N/A')[:28]
        print(f"{name:<20} {table:<25} {schema:<20} {desc:<30}")
    
    print("="*80)
    print(f"총 {len(collectors)}개 수집기 등록됨\n")


def get_collector_stats(collector_name: str) -> dict:
    """수집기 DB 통계 조회"""
    collectors = get_registered_collectors()
    if collector_name not in collectors:
        return None
    
    cls = collectors[collector_name]
    db_pattern = f"{collector_name}*.db"
    
    stats = {
        'total_records': 0,
        'db_files': [],
        'last_modified': None,
        'file_size_mb': 0
    }
    
    for db_file in MASTER_DIR.glob(db_pattern):
        if not db_file.exists():
            continue
        
        try:
            with sqlite3.connect(str(db_file)) as conn:
                table = cls.table_name
                cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                stats['total_records'] += count
                
                mtime = datetime.fromtimestamp(db_file.stat().st_mtime)
                if stats['last_modified'] is None or mtime > stats['last_modified']:
                    stats['last_modified'] = mtime
                
                stats['file_size_mb'] += db_file.stat().st_size / (1024*1024)
                stats['db_files'].append({
                    'path': str(db_file),
                    'records': count,
                    'size_mb': db_file.stat().st_size / (1024*1024)
                })
        except Exception as e:
            stats['db_files'].append({
                'path': str(db_file),
                'error': str(e)
            })
    
    return stats


def show_dashboard():
    """중앙 대시보드 표시"""
    collectors = get_registered_collectors()
    
    print("\n" + "="*80)
    print("📊 Hi-Dunkey 중앙 대시보드")
    print(f"   생성시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    total_records = 0
    total_size = 0
    
    print(f"\n{'수집기':<20} {'레코드':>12} {'크기 (MB)':>12} {'마지막수정':<20} {'상태':<10}")
    print("-"*80)
    
    for name in sorted(collectors.keys()):
        stats = get_collector_stats(name)
        if stats:
            records = stats['total_records']
            size = stats['file_size_mb']
            last_mod = stats['last_modified'].strftime('%Y-%m-%d %H:%M') if stats['last_modified'] else 'N/A'
            status = '✅ 정상' if records > 0 else '⚠️ 데이터없음'
            
            print(f"{name:<20} {records:>12,} {size:>12.2f} {last_mod:<20} {status:<10}")
            
            total_records += records
            total_size += size
        else:
            print(f"{name:<20} {'N/A':>12} {'N/A':>12} {'N/A':<20} {'❌ 오류':<10}")
    
    print("-"*80)
    print(f"{'총계':<20} {total_records:>12,} {total_size:>12.2f} MB")
    print("="*80 + "\n")


def test_all_collectors():
    """전체 수집기 Smoke 테스트"""
    collectors = get_registered_collectors()
    passed = 0
    failed = 0
    
    print("\n" + "="*80)
    print("🧪 수집기 Smoke 테스트")
    print("="*80)
    
    for name, cls in sorted(collectors.items()):
        try:
            # 현재 CollectorEngine 기반 생성자 규약으로 인스턴스화
            collector = cls(shard="none", quiet_mode=True)
            
            # 필수 속성 확인
            assert hasattr(cls, 'schema_name'), "schema_name 없음"
            assert hasattr(cls, 'table_name'), "table_name 없음"
            
            collector.close()
            
            print(f"✅ {name:<20} 통과")
            passed += 1
            
        except Exception as e:
            print(f"❌ {name:<20} 실패: {e}")
            failed += 1
    
    print("="*80)
    print(f"결과: {passed}개 통과, {failed}개 실패\n")
    return failed == 0


def show_collector_stats(name: str):
    """특정 수집기 상세 통계"""
    stats = get_collector_stats(name)
    if stats:
        print(f"\n📊 {name} 통계")
        print(f"   총 레코드: {stats['total_records']:,}")
        print(f"   총 크기: {stats['file_size_mb']:.2f} MB")
        print(f"   마지막 수정: {stats['last_modified']}")
        print(f"   DB 파일 수: {len(stats['db_files'])}")
        for db in stats['db_files']:
            if 'error' not in db:
                print(f"     - {db['path']}: {db['records']:,} records, {db['size_mb']:.2f} MB")
    else:
        print(f"❌ 수집기 '{name}' 을 찾을 수 없습니다.")


def run_collector_by_name(
    name: str,
    regions: str = "ALL",
    debug_mode: bool = False,
    year: int = None,
    limit: int = None,
):
    """collector_cli를 통해 수집기 실행"""
    from collector_cli import run_collector_cli
    run_collector_cli(
        name,
        regions=regions,
        debug_mode=debug_mode,
        year=year,
        limit=limit,
    )


def run_interactive_menu(collectors: dict):
    """숫자 입력형 인터랙티브 메뉴"""
    valid_choices = {"1", "2", "3", "4", "5", "9", "33"}
    back_choice = "22"
    exit_choice = "33"

    def ask_non_empty(prompt: str):
        while True:
            value = input(prompt).strip()
            if value:
                return value
            print("⚠️ 입력이 비어 있습니다. 다시 입력해주세요.")

    def print_fixed_footer():
        print(f"\n  {back_choice}. 뒤로 가기")
        print(f"  {exit_choice}. 종료")

    def parse_year_input(value: str, default_year: int):
        text = value.strip()
        if not text:
            return default_year
        if text.lower() in {"all", "전체"}:
            return default_year
        if text.endswith("학년도"):
            text = text[:-3].strip()
        try:
            return int(text)
        except ValueError:
            print(f"⚠️ 학년도 입력이 올바르지 않습니다. 기본값 {default_year} 사용")
            return default_year

    def parse_region_input(value: str, default_region: str):
        text = value.strip()
        if not text:
            return default_region
        if text.lower() in {"all", "전체"}:
            return "ALL"
        if text.isdigit():
            idx = int(text)
            if 1 <= idx <= len(ALL_REGIONS):
                return ALL_REGIONS[idx - 1]
            print("⚠️ 지역 번호가 범위를 벗어났습니다. 기본값 사용")
            return default_region
        return text.upper()

    def print_region_grid():
        print("\n교육청 선택 목록 (번호 입력 가능)")
        items = [f"{i:02d}. {REGION_NAMES.get(code, code)}({code})" for i, code in enumerate(ALL_REGIONS, 1)]
        cols = 5
        width = max(len(item) for item in items) + 2
        for row_start in range(0, len(items), cols):
            row = items[row_start:row_start + cols]
            print("".join(item.ljust(width) for item in row).rstrip())

    def parse_limit_input(value: str, default_limit):
        text = value.strip()
        if not text:
            return default_limit
        if text.lower() in {"all", "전체"}:
            return None
        try:
            n = int(text)
            if n <= 0:
                raise ValueError()
            return n
        except ValueError:
            print("⚠️ 학교 수 입력이 올바르지 않습니다. 기본값 사용")
            return default_limit

    def choose_collector(prompt_text: str):
        preferred = ["neis_info", "school_info", "schedule", "meal", "timetable"]
        display_names = {
            "neis_info": "나이스 학교 정보",
            "meal": "급식 정보",
            "schedule": "학사일정",
            "school_info": "학교알리미 기본정보",
            "timetable": "시간표",
        }

        ordered = [name for name in preferred if name in collectors]
        for name in sorted(collectors.keys()):
            if name not in ordered:
                ordered.append(name)

        print("\n수집기 목록")
        for idx, name in enumerate(ordered, 1):
            label = display_names.get(name, name)
            print(f"  {idx}. {label}({name})")

        while True:
            raw_name = ask_non_empty("\n수집기 선택(또는 alias): ")
            if raw_name.isdigit():
                n = int(raw_name)
                if 1 <= n <= len(ordered):
                    return ordered[n - 1]
                print(f"⚠️ 번호 선택 범위를 벗어났습니다. 1~{len(ordered)} 중에서 입력하세요.")
                continue

            try:
                return resolve_collector_name(raw_name, collectors)
            except ValueError as e:
                print(f"❌ {e}")

    def run_direct_input_mode(default_debug: bool = False):
        print("\n예시: --list | --dashboard | --test | --stats neis_info | --run neis_info --regions B10")
        direct = ask_non_empty("직접 입력 > ")

        tokens = shlex.split(direct)
        if not tokens:
            print("⚠️ 입력을 해석할 수 없습니다.")
            return

        direct_parser = argparse.ArgumentParser(add_help=False)
        direct_parser.add_argument("--list", action="store_true")
        direct_parser.add_argument("--dashboard", action="store_true")
        direct_parser.add_argument("--test", action="store_true")
        direct_parser.add_argument("--stats", type=str)
        direct_parser.add_argument("--run", type=str)
        direct_parser.add_argument("--regions", type=str, default="ALL")
        direct_parser.add_argument("--year", type=int)
        direct_parser.add_argument("--limit", type=int)
        direct_parser.add_argument("--debug", action="store_true")

        try:
            dargs = direct_parser.parse_args(tokens)
        except SystemExit:
            print("⚠️ 입력 형식이 잘못되었습니다. 예시를 참고하세요.")
            return

        try:
            if dargs.stats:
                dargs.stats = resolve_collector_name(dargs.stats, collectors)
            if dargs.run:
                dargs.run = resolve_collector_name(dargs.run, collectors)
        except ValueError as e:
            print(f"❌ {e}")
            return

        if dargs.list:
            list_collectors()
        elif dargs.dashboard:
            show_dashboard()
        elif dargs.test:
            test_all_collectors()
        elif dargs.stats:
            show_collector_stats(dargs.stats)
        elif dargs.run:
            effective_debug = dargs.debug or default_debug
            confirm = input(
                f"실행: {dargs.run} / regions={dargs.regions} / year={dargs.year or '기본'} / limit={dargs.limit or 'ALL'} / debug={effective_debug} 맞나요? [y/N] "
            ).strip().lower()
            if confirm not in ("y", "yes"):
                print("취소되었습니다.")
                return
            run_collector_by_name(
                dargs.run,
                regions=dargs.regions,
                debug_mode=effective_debug,
                year=dargs.year,
                limit=dargs.limit,
            )
        else:
            print("⚠️ 지원하지 않는 직접 입력입니다.")

    def show_recent_logs():
        logs_dir = Path("data/logs")
        if not logs_dir.exists():
            print("⚠️ 로그 디렉토리가 없습니다: data/logs")
            return

        files = sorted(logs_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print("⚠️ 로그 파일이 없습니다.")
            return

        print("\n로그 파일 목록 (최신순)")
        for idx, f in enumerate(files[:10], 1):
            print(f"  {idx}. {f.name}")

        choice = input("볼 로그 번호(기본: 1) > ").strip() or "1"
        try:
            index = int(choice)
            if index < 1 or index > min(10, len(files)):
                raise ValueError()
        except ValueError:
            print("⚠️ 잘못된 번호입니다.")
            return

        target = files[index - 1]
        lines = target.read_text(encoding="utf-8", errors="replace").splitlines()
        tail_count_text = input("마지막 몇 줄을 볼까요? (기본: 30) > ").strip() or "30"
        try:
            tail_count = max(1, int(tail_count_text))
        except ValueError:
            tail_count = 30

        print(f"\n--- {target.name} (last {tail_count}) ---")
        for line in lines[-tail_count:]:
            print(line)

    def post_run_menu(default_debug: bool = False):
        while True:
            print("\n수집기 실행 후 메뉴")
            print("  1. 로그 보기")
            print("  2. 직접 입력 모드")
            print_fixed_footer()

            choice = input("\n번호 선택 > ").strip()
            if choice == back_choice:
                return
            if choice == exit_choice:
                print("종료합니다.")
                raise SystemExit(0)
            if choice == "1":
                show_recent_logs()
                continue
            if choice == "2":
                run_direct_input_mode(default_debug=default_debug)
                continue
            print(f"⚠️ 잘못된 입력입니다: '{choice}'. 허용 번호: 1, 2, {back_choice}, {exit_choice}")

    def run_collector_wizard(default_region: str = "ALL", default_limit=None, force_debug: bool = False):
        collector_name = choose_collector("수집기 이름(또는 alias) > ")
        if not collector_name:
            return

        current_year = get_current_school_year(now_kst())
        region_label = "전체" if default_region == "ALL" else f"{REGION_NAMES.get(default_region, default_region)}시교육청"
        region_default_text = "all" if default_region == "ALL" else default_region
        limit_label = "없음" if default_limit is None else str(default_limit)
        limit_default_text = "all" if default_limit is None else str(default_limit)

        print("\n수집기 옵션")
        print(f"  1. 수집할 학년도(현재학년도): {current_year}학년도")
        print(f"  2. 수집할 지역({region_label}): {region_default_text}")
        print(f"  3. 수집 제한({limit_label}): {limit_default_text}")
        print("  Enter를 누르면 기본값을 사용합니다.")

        year_input = input(f"수집할 학년도(현재학년도: {current_year}학년도) > ")
        print_region_grid()
        region_input = input(f"수집할 지역({region_label}): {region_default_text} > ")
        limit_input = input(f"수집 제한({limit_label}): {limit_default_text} > ")

        year = parse_year_input(year_input, current_year)
        region = parse_region_input(region_input, default_region)
        limit = parse_limit_input(limit_input, default_limit)

        limit_text = "all" if limit is None else str(limit)
        debug_text = "ON" if force_debug else "OFF"
        confirm = input(
            f"실행: collector={collector_name}, year={year}, region={region}, limit={limit_text}, debug={debug_text} 맞나요? [y/N] "
        ).strip().lower()
        if confirm not in ("y", "yes"):
            print("취소되었습니다.")
            return

        run_collector_by_name(
            collector_name,
            regions=region,
            debug_mode=force_debug,
            year=year,
            limit=limit,
        )
        post_run_menu(default_debug=force_debug)

    def run_debug_menu():
        while True:
            print("\nDebug 모드")
            print("  1. 수집기 실행")
            print("  2. 중앙 대시보드")
            print("  3. 로그 확인")
            print("  9. 직접 입력 모드")
            print_fixed_footer()

            choice = input("\n번호 선택 > ").strip()
            if choice == back_choice:
                return
            if choice == exit_choice:
                print("종료합니다.")
                raise SystemExit(0)
            if choice == "1":
                run_collector_wizard(default_region="B10", default_limit=10, force_debug=True)
                continue
            if choice == "2":
                show_dashboard()
                continue
            if choice == "3":
                show_recent_logs()
                continue
            if choice == "9":
                run_direct_input_mode(default_debug=True)
                continue
            print(f"⚠️ 잘못된 입력입니다: '{choice}'. 허용 번호: 1, 2, 3, 9, {back_choice}, {exit_choice}")

    while True:
        collector_names = ", ".join(sorted(collectors.keys()))

        print(f"\n등록 수집기 목록: {collector_names}")
        print("  1. 수집기 실행")
        print("  2. 중앙 대시보드")
        print("  3. Smoke 테스트")
        print("  4. 수집기 통계")
        print("  5. Debug 모드")
        print("  9. 직접 입력 모드")
        print("\n  33. 종료")

        choice = input("\n번호 선택 > ").strip()
        if choice not in valid_choices:
            allowed = ", ".join(sorted(valid_choices, key=int))
            print(f"⚠️ 잘못된 입력입니다: '{choice}'. 허용 번호: {allowed}")
            continue

        if choice == exit_choice:
            print("종료합니다.")
            return

        if choice == "1":
            run_collector_wizard(default_region="ALL", default_limit=None, force_debug=False)
            continue

        if choice == "2":
            step_title("중앙 대시보드 실행", 1, 1)
            show_dashboard()
            continue

        if choice == "3":
            total_steps = 2
            step_title("Smoke 테스트 실행 확인", 1, total_steps)
            confirm = input("Smoke 테스트를 실행할까요? [y/N] ").strip().lower()
            if confirm not in ("y", "yes"):
                print("취소되었습니다.")
                continue
            step_title("Smoke 테스트 실행", 2, total_steps)
            test_all_collectors()
            continue

        if choice == "4":
            total_steps = 3
            step_title("통계 조회할 수집기 입력", 1, total_steps)
            raw_name = ask_non_empty("수집기 이름(또는 alias) > ")

            step_title("입력값 검증", 2, total_steps)
            try:
                name = resolve_collector_name(raw_name, collectors)
            except ValueError as e:
                print(f"❌ {e}")
                continue

            step_title("통계 출력", 3, total_steps)
            show_collector_stats(name)
            continue

        if choice == "5":
            run_debug_menu()
            continue

        if choice == "9":
            run_direct_input_mode(default_debug=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="마스터 수집기 제어")
    parser.add_argument("--list", action="store_true", help="수집기 목록 표시")
    parser.add_argument("--dashboard", action="store_true", help="중앙 대시보드 표시")
    parser.add_argument("--test", action="store_true", help="전체 수집기 테스트")
    parser.add_argument("--stats", type=str, help="특정 수집기 통계 표시")
    parser.add_argument("--run", type=str, help="특정 수집기 실행")
    parser.add_argument("--regions", type=str, default="ALL", help="지역 코드")
    
    args = parser.parse_args()

    # 이름 정규화: collector name을 받는 모든 인자는 여기서 일괄 해석
    _collectors = get_registered_collectors()
    validate_name_resolution_map(_collectors)
    if args.stats:
        args.stats = resolve_collector_name(args.stats, _collectors)
    if args.run:
        args.run = resolve_collector_name(args.run, _collectors)

    if args.list:
        list_collectors()
    elif args.dashboard:
        show_dashboard()
    elif args.test:
        success = test_all_collectors()
        sys.exit(0 if success else 1)
    elif args.stats:
        show_collector_stats(args.stats)
    elif args.run:
        run_collector_by_name(args.run, regions=args.regions)
    else:
        run_interactive_menu(_collectors)
        