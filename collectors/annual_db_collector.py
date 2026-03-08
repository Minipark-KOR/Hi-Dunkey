#!/usr/bin/env python3
# 개발 가이드: docs/developer_guide.md 참조
"""
전체 데이터 수집기 (오케스트레이터) + 체크포인트 재시도
"""
import sys
import json
import argparse

from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.collector_cli import parse_regions

from collectors.neis_info_collector import NeisInfoCollector
from collectors.annual_meal_collector import AnnualMealCollector
from collectors.timetable_collector import AnnualFullTimetableCollector
from collectors.schedule_collector import AnnualFullScheduleCollector

from constants.paths import ACTIVE_DIR

CHECKPOINT_PATH = ACTIVE_DIR / "collect_checkpoint.json"

STEP_MAP = {
    "학교 기본정보": (SchoolInfoCollector, lambda c, r, **kw: c.fetch_region(r)),
    "급식":          (AnnualMealCollector, lambda c, r, **kw: c.fetch_year(r, kw['year'])),
    "시간표 1학기":  (AnnualFullTimetableCollector, lambda c, r, **kw: c.fetch_year(r, kw['year'], 1)),
    "시간표 2학기":  (AnnualFullTimetableCollector, lambda c, r, **kw: c.fetch_year(r, kw['year'], 2)),
    "학사일정":      (AnnualFullScheduleCollector, lambda c, r, **kw: c.fetch_year(r, kw['year'])),
}


def save_checkpoint(step: str, region: str, error: str):
    data = {}
    if CHECKPOINT_PATH.exists():
        data = json.loads(CHECKPOINT_PATH.read_text())
    data.setdefault(step, {})[region] = {"error": error, "ts": now_kst().isoformat()}
    CHECKPOINT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def load_failed_regions(step: str) -> list:
    if not CHECKPOINT_PATH.exists():
        return []
    data = json.loads(CHECKPOINT_PATH.read_text())
    return list(data.get(step, {}).keys())


def clear_checkpoint(step: str, region: str):
    if not CHECKPOINT_PATH.exists():
        return
    data = json.loads(CHECKPOINT_PATH.read_text())
    data.get(step, {}).pop(region, None)
    if not data.get(step):
        data.pop(step, None)
    CHECKPOINT_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _run_step(label, collector_cls, fetch_fn, regions, **kwargs) -> list:
    print(f"\n🚀 {label} 시작")
    failed = []
    collector = collector_cls()
    try:
        for region in regions:
            try:
                fetch_fn(collector, region, **kwargs)
                clear_checkpoint(label, region)
            except Exception as e:
                print(f"  ❌ [{region}] 실패: {e}", file=sys.stderr)
                failed.append(region)
                save_checkpoint(label, region, str(e))
    finally:
        collector.close()
    print(f"{'✅' if not failed else '⚠️'} {label} 완료 (실패: {failed or '없음'})")
    return failed


def full_collect(year, regions, skip_school=False, retry_failed=False) -> int:
    total_failed = []

    if retry_failed:
        for step, (cls, fn) in STEP_MAP.items():
            failed_regions = load_failed_regions(step)
            if failed_regions:
                print(f"🔄 [{step}] 실패 지역 재시도: {failed_regions}")
                total_failed.extend(_run_step(step, cls, fn, failed_regions, year=year))
        return len(total_failed)

    if not skip_school:
        cls, fn = STEP_MAP["학교 기본정보"]
        total_failed.extend(_run_step("학교 기본정보", cls, fn, regions))

    for label in ["급식", "시간표 1학기", "시간표 2학기", "학사일정"]:
        cls, fn = STEP_MAP[label]
        total_failed.extend(_run_step(label, cls, fn, regions, year=year))

    return len(total_failed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=get_current_school_year(now_kst()))
    parser.add_argument("--regions", default="ALL")
    parser.add_argument("--skip_school", action="store_true")
    parser.add_argument("--retry_failed", action="store_true")
    args = parser.parse_args()

    regions = parse_regions(args.regions)
    print(f"🎯 수집 시작 | 학년도={args.year} | 교육청={len(regions)}개" + (" | 재시도 모드" if args.retry_failed else ""))
    fail_count = full_collect(args.year, regions, args.skip_school, args.retry_failed)

    if fail_count > 0:
        print(f"\n❌ 총 {fail_count}건 실패", file=sys.stderr)
        sys.exit(1)
    print("\n🎉 전체 수집 완료")
    sys.exit(0)


if __name__ == "__main__":
    main()
    