#!/usr/bin/env python3
"""
Collector 공통 CLI 진입점
"""
import sys
import argparse
from typing import Type, Callable, Optional
from constants.codes import ALL_REGIONS, NEIS_API_KEY
from core.kst_time import now_kst
from core.school_year import get_current_school_year


def build_common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--regions", default="ALL", help="교육청 코드 (쉼표 구분, 기본: ALL)"
    )
    parser.add_argument(
        "--shard", choices=["odd", "even", "none"], default="none"
    )
    parser.add_argument(
        "--school_range", choices=["A", "B", "none"], default="none"
    )
    parser.add_argument(
        "--year", type=int, default=None, help="학년도 (기본: 현재)"
    )
    parser.add_argument(
        "--date", default=None, help="수집일 YYYYMMDD"
    )
    parser.add_argument("--debug", action="store_true")
    return parser


def parse_regions(regions_arg: str):
    if regions_arg.upper() == "ALL":
        return ALL_REGIONS
    return [r.strip() for r in regions_arg.split(",") if r.strip()]


def run_collector(
    collector_cls: Type,
    fetch_fn: Callable,
    description: str,
    extra_args_fn: Optional[Callable] = None
):
    parser = build_common_parser(description)
    if extra_args_fn:
        extra_args_fn(parser)
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수 없음", file=sys.stderr)
        sys.exit(2)

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
                fetch_fn(collector, region, year=year, date=target_date)
            except Exception as e:
                print(f"❌ [{region}] 수집 실패: {e}", file=sys.stderr)
                failed.append(region)
    except KeyboardInterrupt:
        collector.logger.warning("⚠️ 수집 중단 (KeyboardInterrupt)")
    finally:
        # close()가 내부적으로 q.join() → writer 종료 → 리소스 정리 순서 보장
        collector.close()

    if failed:
        print(f"⚠️ 실패 지역: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)

    print("✅ 수집 완료")
    print("👉 엔터를 눌러주세요.")  # 추가
    sys.exit(0)
