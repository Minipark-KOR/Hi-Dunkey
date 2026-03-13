#!/usr/bin/env python3
# collector_cli.py
"""
Collector 공통 CLI 진입점
"""
import sys
import argparse
import os
from typing import Optional

# rich 라이브러리 임포트
try:
    from rich.console import Console
    from rich.table import Table
    from rich.live import Live
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from constants.codes import ALL_REGIONS, NEIS_API_KEY
from core.kst_time import now_kst
from core.school_year import get_current_school_year

# 모든 수집기 임포트
from collectors.neis_info_collector import NeisInfoCollector
from collectors.school_info_collector import SchoolInfoCollector
from collectors.meal_collector import MealCollector
#from collectors.schedule_collector import AnnualFullScheduleCollector
#from collectors.timetable_collector import AnnualFullTimetableCollector

# collector_name -> collector_class 매핑
COLLECTOR_MAP = {
    "neis_info": NeisInfoCollector,
    "school_info": SchoolInfoCollector,
    "meal_collector": MealCollector,
    #"schedule_collector": AnnualFullScheduleCollector,
    #"timetable_collector": AnnualFullTimetableCollector,
}

def build_common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--regions", default="ALL", help="교육청 코드 (쉼표 구분, 기본: ALL)")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--year", type=int, default=None, help="학년도 (기본: 현재)")
    parser.add_argument("--date", default=None, help="수집일 YYYYMMDD")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="출력 최소화")
    return parser

def parse_regions(regions_arg: str):
    if regions_arg.upper() == "ALL":
        return ALL_REGIONS
    return [r.strip() for r in regions_arg.split(",") if r.strip()]

def create_status_table(region_status: dict, current_idx: int, total: int) -> Table:
    """진행 상황 테이블 생성"""
    table = Table(
        title=f"📊 수집 현황 [{current_idx}/{total}]",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta"
    )
    table.add_column("지역", style="cyan", width=10)
    table.add_column("교육청", style="green")
    table.add_column("상태", justify="center")
    table.add_column("처리건수", justify="right")

    for region, info in region_status.items():
        status_display = {
            "waiting": "⏳ 대기",
            "processing": "⚙️ 수집중",
            "success": "✅ 완료",
            "failed": "❌ 실패"
        }.get(info["status"], info["status"])

        processed = info.get("processed", 0)
        processed_str = f"{processed:,}" if processed else "-"

        table.add_row(
            region,
            info.get("name", region),
            status_display,
            processed_str
        )
    return table

def run_collector(collector_name: str):
    """collector_name에 해당하는 수집기 실행"""
    extra_parsers = {
        "timetable_collector": lambda p: p.add_argument("--semester", type=int, default=1, choices=[1,2]),
    }

    parser = build_common_parser(f"{collector_name} 수집기")
    if collector_name in extra_parsers:
        extra_parsers[collector_name](parser)
    parser.add_argument("--limit", type=int, help="테스트용 제한 개수")
    args = parser.parse_args()
    
    # 연도 프롬프트 (대화형 터미널에서만)
    if args.year is None and sys.stdin.isatty():
        default_year = get_current_school_year(now_kst())
        try:
            user_input = input(f"수집할 학년도를 입력하세요 (기본값: {default_year}): ").strip()
            if user_input:
                args.year = int(user_input)
            else:
                args.year = default_year
        except (EOFError, KeyboardInterrupt):
            args.year = default_year

    # GitHub Actions 환경에서는 자동으로 quiet 모드
    if os.getenv('GITHUB_ACTIONS') == 'true' and not args.quiet:
        args.quiet = True

    if collector_name == "neis_info" and not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수 없음", file=sys.stderr)
        sys.exit(2)

    collector_cls = COLLECTOR_MAP.get(collector_name)
    if not collector_cls:
        print(f"❌ 알 수 없는 수집기: {collector_name}", file=sys.stderr)
        sys.exit(1)

    school_range = None if args.school_range == "none" else args.school_range
    year = args.year or get_current_school_year(now_kst())
    target_date = args.date or now_kst().strftime("%Y%m%d")
    regions = parse_regions(args.regions)
    total_regions = len(regions)

    collector = collector_cls(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug,
        quiet_mode=args.quiet
    )

    # 지역별 상태 저장 (테이블용)
    from constants.codes import REGION_NAMES
    region_status = {
        r: {
            "status": "waiting",
            "processed": 0,
            "name": REGION_NAMES.get(r, r)
        }
        for r in regions
    }

    failed = []

    # rich 테이블 표시 (quiet 모드가 아닐 때만)
    if RICH_AVAILABLE and not args.quiet:
        console = Console()
        with Live(console=console, refresh_per_second=4, screen=False) as live:
            for idx, region in enumerate(regions, 1):
                region_status[region]["status"] = "processing"
                live.update(create_status_table(region_status, idx, total_regions))

                try:
                    if collector_name == "meal_collector":
                        processed = collector.fetch_daily(region, target_date)
                    elif collector_name == "schedule_collector":
                        processed = collector.fetch_year(region, year)
                    elif collector_name == "timetable_collector":
                        processed = collector.fetch_year(region, year, args.semester)
                    elif collector_name in ["neis_info", "school_info"]:
                        processed = collector.fetch_region(region, year=year, date=target_date)
                    else:
                        if hasattr(collector, 'fetch_region'):
                            processed = collector.fetch_region(region, year=year, date=target_date)
                        else:
                            raise AttributeError(f"{collector_name}에 적절한 fetch 메서드가 없습니다.")

                    region_status[region]["status"] = "success"
                    region_status[region]["processed"] = processed if processed else 0

                except Exception as e:
                    region_status[region]["status"] = "failed"
                    failed.append(region)
                    print(f"❌ [{region}] 수집 실패: {e}", file=sys.stderr)

                live.update(create_status_table(region_status, idx, total_regions))

                if args.limit and idx >= args.limit:
                    break
    else:
        # 기존 방식 (quiet 모드 또는 rich 미설치 시)
        for idx, region in enumerate(regions, 1):
            if not args.quiet:
                print(f"\n📌 [{idx}/{total_regions}] {region} 수집 중...")

            try:
                if collector_name == "meal_collector":
                    collector.fetch_daily(region, target_date)
                elif collector_name == "schedule_collector":
                    collector.fetch_year(region, year)
                elif collector_name == "timetable_collector":
                    collector.fetch_year(region, year, args.semester)
                elif collector_name in ["neis_info", "school_info"]:
                    collector.fetch_region(region, year=year, date=target_date)
                else:
                    if hasattr(collector, 'fetch_region'):
                        collector.fetch_region(region, year=year, date=target_date)
                    else:
                        raise AttributeError(f"{collector_name}에 적절한 fetch 메서드가 없습니다.")
            except Exception as e:
                print(f"❌ [{region}] 수집 실패: {e}", file=sys.stderr)
                failed.append(region)

            if args.limit and idx >= args.limit:
                break

    collector.close()

    if failed:
        print(f"⚠️ 실패 지역: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)

    if not args.quiet:
        print("\n✅ 모든 지역 수집 완료")
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: collector_cli.py [neis_info|school_info|meal_collector|schedule_collector|timetable_collector] [옵션...]", file=sys.stderr)
        sys.exit(1)
    collector_name = sys.argv[1]
    sys.argv.pop(1)
    run_collector(collector_name)
    