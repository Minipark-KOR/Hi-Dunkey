#!/usr/bin/env python3
# master_collectors.py
# 중앙 제어탑 + 대시보드

import sys
import argparse
import sqlite3
import os
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 sys.path 에 추가
sys.path.append(str(Path(__file__).parent))

from scripts.collector import get_registered_collectors
from constants.paths import MASTER_DIR


def list_collectors():
    """모든 수집기 목록 표시"""
    collectors = get_registered_collectors()
    
    print("\n" + "="*80)
    print("📋 등록됨 수집기 목록")
    print("="*80)
    print(f"{'이름':<20} {'테이블':<25} {'스키마':<20} {'설명':<30}")
    print("-"*80)
    
    for name, cls in sorted(collectors.items()):
        table = getattr(cls, 'table_name', 'N/A')
        schema = getattr(cls, 'schema_name', 'N/A')
        desc = getattr(cls, 'description', 'N/A')[:28]
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
            # 클래스 인스턴스화 테스트
            collector = cls(name, str(MASTER_DIR), shard="none", quiet_mode=True)
            
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
    from constants.domains import resolve_collector_name
    _collectors = get_registered_collectors()
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
        from collector_cli import run_collector_cli
        run_collector_cli(args.run, regions=args.regions)
    else:
        # 기본: 대화형 메뉴
        print("\n🎯 Hi-Dunkey 마스터 제어")
        print("  1. 수집기 목록 (--list)")
        print("  2. 중앙 대시보드 (--dashboard)")
        print("  3. Smoke 테스트 (--test)")
        print("  4. 수집기 통계 (--stats <name>)")
        print("  5. 수집기 실행 (--run <name>)")
        print("\n  예: python master_collectors.py --dashboard\n")
        