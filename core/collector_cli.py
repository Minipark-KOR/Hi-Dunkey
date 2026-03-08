#!/usr/bin/env python3
"""
Collector 공통 CLI 진입점
"""
import sys
import argparse
from typing import Optional

from constants.codes import ALL_REGIONS, NEIS_API_KEY
from core.kst_time import now_kst
from core.school_year import get_current_school_year

# 모든 수집기 임포트
from collectors.neis_info_collector import NeisInfoCollector
from collectors.school_info_collector import SchoolInfoCollector
from collectors.meal_collector import MealCollector
from collectors.schedule_collector import AnnualFullScheduleCollector
from collectors.timetable_collector import AnnualFullTimetableCollector

# collector_name -> collector_class 매핑
COLLECTOR_MAP = {
    "neis_info": NeisInfoCollector,
    "school_info": SchoolInfoCollector,
    "meal_collector": MealCollector,
    "schedule_collector": AnnualFullScheduleCollector,
    "timetable_collector": AnnualFullTimetableCollector,
}


def build_common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--regions", default="ALL", help="교육청 코드 (쉼표 구분, 기본: ALL)")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--year", type=int, default=None, help="학년도 (기본: 현재)")
    parser.add_argument("--date", default=None, help="수집일 YYYYMMDD")
    parser.add_argument("--debug", action="store_true")
    return parser


def parse_regions(regions_arg: str):
    if regions_arg.upper() == "ALL":
        return ALL_REGIONS
    return [r.strip() for r in regions_arg.split(",") if r.strip()]


def run_collector(collector_name: str):
    """collector_name에 해당하는 수집기 실행"""
    # collector별 추가 인자 처리 (예: timetable은 --semester 필요)
    extra_parsers = {
        "timetable_collector": lambda p: p.add_argument("--semester", type=int, default=1, choices=[1,2]),
    }

    parser = build_common_parser(f"{collector_name} 수집기")
    if collector_name in extra_parsers:
        extra_parsers[collector_name](parser)
    parser.add_argument("--limit", type=int, help="테스트용 제한 개수")
    args = parser.parse_args()

    # API 키 체크 (neis_info만 필요)
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

    collector = collector_cls(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug
    )

    failed = []
    try:
        for region in regions:
            try:
                # 각 collector에 맞는 fetch 메서드 호출
                if collector_name == "meal_collector":
                    collector.fetch_daily(region, target_date)
                elif collector_name == "schedule_collector":
                    collector.fetch_year(region, year)
                elif collector_name == "timetable_collector":
                    collector.fetch_year(region, year, args.semester)
                elif collector_name in ["neis_info", "school_info"]:
                    collector.fetch_region(region, year=year, date=target_date)
                else:
                    # 기본 fetch_region 시도 (없으면 에러)
                    if hasattr(collector, 'fetch_region'):
                        collector.fetch_region(region, year=year, date=target_date)
                    else:
                        raise AttributeError(f"{collector_name}에 적절한 fetch 메서드가 없습니다.")
            except Exception as e:
                print(f"❌ [{region}] 수집 실패: {e}", file=sys.stderr)
                failed.append(region)
    except KeyboardInterrupt:
        collector.logger.warning("⚠️ 수집 중단 (KeyboardInterrupt)")
    finally:
        collector.close()

    if failed:
        print(f"⚠️ 실패 지역: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)

    print("✅ 수집 완료")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: collector_cli.py [neis_info|school_info|meal_collector|schedule_collector|timetable_collector] [옵션...]", file=sys.stderr)
        sys.exit(1)
    collector_name = sys.argv[1]
    sys.argv.pop(1)
    run_collector(collector_name)
    