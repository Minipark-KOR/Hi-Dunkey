#!/usr/bin/env python3
# collector_cli.py
# 공통 CLI 진입점 (자동 등록 시스템 통합)

import sys
import argparse
from pathlib import Path

# 프로젝트 루트를 sys.path 에 추가
sys.path.append(str(Path(__file__).parent))

from scripts.collector import get_registered_collectors
from constants.codes import ALL_REGIONS
from constants.domains import resolve_collector_name, validate_name_resolution_map


def get_collector(name: str):
    """수집기 클래스 조회"""
    collectors = get_registered_collectors()
    validate_name_resolution_map(collectors)
    resolved_name = resolve_collector_name(name, collectors)

    if resolved_name not in collectors:
        available = ", ".join(sorted(collectors.keys()))
        raise ValueError(f"❌ 수집기 '{name}' 을 찾을 수 없습니다.\n   사용 가능: {available}")
    return collectors[resolved_name]


def run_collector_cli(name: str, regions=None, shard="none", **kwargs):
    """수집기 실행 래퍼"""
    collector_class = get_collector(name)
    
    # 수집기 인스턴스 생성
    collector = collector_class(
        shard=shard,
        debug_mode=kwargs.get('debug_mode', False),
        quiet_mode=kwargs.get('quiet_mode', False),
        school_range=kwargs.get('school_range', None),
    )
    
    # 지역 결정
    if regions is None or regions == "ALL":
        regions = ALL_REGIONS
    elif isinstance(regions, str):
        regions = [regions]
    
    # 수집 실행
    try:
        for region in regions:
            if hasattr(collector, 'fetch_region'):
                collector.fetch_region(region, **kwargs)
            else:
                raise NotImplementedError(f"{name} 에 fetch_region 메서드가 없습니다.")
        
        # 종료 처리
        collector.close()
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자 중단")
        collector.close()
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        collector.close()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="수집기 CLI")
    parser.add_argument("collector", nargs="?", help="수집기 이름 (예: neis_info)")
    parser.add_argument("--regions", default="ALL", help="지역 코드 (예: B10 또는 ALL)")
    parser.add_argument("--shard", default="none", choices=["none", "odd", "even"], help="샤드")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    parser.add_argument("--quiet", action="store_true", help="조용 모드")
    parser.add_argument("--list", action="store_true", help="수집기 목록 표시")
    parser.add_argument("--year", type=int, help="학년도")
    parser.add_argument("--limit", type=int, help="수집 제한 (테스트용)")
    
    args = parser.parse_args()
    validate_name_resolution_map(get_registered_collectors())
    # 목록 표시
    if args.list or args.collector is None:
        collectors = get_registered_collectors()
        print("\n" + "="*80)
        print("📋 등록됨 수집기 목록")
        print("="*80)
        for name in sorted(collectors.keys()):
            cls = collectors[name]
            desc = getattr(cls, 'description', 'N/A')
            print(f"  • {name:<20} - {desc}")
        print("="*80)
        print(f"총 {len(collectors)}개 수집기 등록됨\n")
        sys.exit(0)
    
    # 수집기 실행
    try:
        run_collector_cli(
            name=args.collector,
            regions=args.regions,
            shard=args.shard,
            debug_mode=args.debug,
            quiet_mode=args.quiet,
            year=args.year,
            limit=args.limit,
        )
    except Exception as e:
        print(f"❌ 오류: {e}")
        sys.exit(1)
        