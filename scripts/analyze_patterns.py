#!/usr/bin/env python3
"""
unknown 패턴 분석 스크립트
- Phase 2에서 새로운 메타타입 발견을 위해 사용
- WAL 모드 활성화
- 빈도수 기반 필터링
"""
import os
import sys
import sqlite3
from typing import Dict, Any
from core.meal_extractor import UnknownPatternAnalyzer
from constants.paths import UNKNOWN_DB_PATH, GLOBAL_VOCAB_DB_PATH


def analyze_patterns(min_frequency: int = 10, limit: int = 50):
    """unknown 패턴 분석 및 새 메타타입 제안"""
    
    if not os.path.exists(UNKNOWN_DB_PATH):
        print("❌ unknown_patterns.db 파일이 없습니다.")
        return
    
    analyzer = UnknownPatternAnalyzer(UNKNOWN_DB_PATH)
    
    print("=" * 90)
    print("🔍 unknown 패턴 분석 리포트")
    print("=" * 90)
    
    # 통계 정보
    stats = analyzer.get_statistics()
    print(f"\n📊 전체 통계")
    print(f"  - 전체 패턴: {stats['total']}개")
    print(f"  - 검토 완료: {stats['reviewed']}개")
    print(f"  - 미검토: {stats['pending']}개")
    
    print(f"\n📈 패턴 타입별 분포")
    for ptype, count in stats['by_type'].items():
        print(f"  - {ptype}: {count}개")
    
    # 상위 패턴 조회
    patterns = analyzer.get_top_patterns(limit=limit, min_frequency=min_frequency)
    
    if not patterns:
        print(f"\n⚠️ 빈도수 {min_frequency} 이상인 미검토 패턴이 없습니다.")
        return
    
    print(f"\n📊 상위 {len(patterns)}개 미등록 패턴 (최소 빈도: {min_frequency})")
    print("-" * 90)
    print(f"{'순위':<4} {'타입':<12} {'값':<25} {'빈도':<8} {'제안타입':<12}")
    print("-" * 90)
    
    suggestions = {}
    for i, p in enumerate(patterns, 1):
        suggested = analyzer.suggest_meta_type(p)
        suggestions[suggested] = suggestions.get(suggested, 0) + 1
        
        # 값이 너무 길면 자르기
        display_value = p['value'][:22] + '...' if len(p['value']) > 25 else p['value']
        print(f"{i:<4} {p['part_type']:<12} {display_value:<25} "
              f"{p['frequency']:<8} {suggested:<12}")
    
    print("\n✨ 제안된 새 메타타입:")
    for st, count in suggestions.items():
        if st != 'unknown':
            print(f"  - {st}: {count}개 패턴")
    
    print(f"\n💡 다음 단계:")
    print(f"  1. 위 패턴들을 검토하여 새로운 meta_type 결정")
    print(f"  2. 결정된 타입은 meta_vocab에 등록")
    print(f"  3. scripts/migrate_patterns.py로 기존 데이터 마이그레이션")
    print(f"\n💾 분석 결과 저장: unknown_patterns_analysis.txt")
    
    # 분석 결과 저장
    with open("unknown_patterns_analysis.txt", "w", encoding="utf-8") as f:
        f.write("🔍 unknown 패턴 분석 리포트\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"분석 시간: {__import__('core.kst_time').kst_time.now_kst().isoformat()}\n")
        f.write(f"전체 패턴: {stats['total']}개\n\n")
        
        for p in patterns:
            suggested = analyzer.suggest_meta_type(p)
            f.write(f"{p['value']}\t{p['frequency']}회\t{suggested}\n")


def migrate_pattern_to_meta(meta_type: str, values: list):
    """발견된 패턴을 정식 meta_type으로 등록"""
    
    if not os.path.exists(GLOBAL_VOCAB_PATH):
        print(f"❌ {GLOBAL_VOCAB_PATH} 파일이 없습니다.")
        return
    
    conn = sqlite3.connect(GLOBAL_VOCAB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    success = 0
    for value in values:
        try:
            # ID 생성 (해시 기반)
            import hashlib
            key = f"meal:{meta_type}:{value}"
            meta_id = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) % 10**12
            
            conn.execute("""
                INSERT OR IGNORE INTO meta_vocab 
                (meta_id, domain, meta_type, meta_key, meta_value, display_value)
                VALUES (?, 'meal', ?, ?, ?, ?)
            """, (meta_id, meta_type, value, value, value))
            success += 1
        except Exception as e:
            print(f"  ❌ {value} 등록 실패: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ {success}개 패턴을 {meta_type}으로 등록 완료")
    
    # unknown_patterns 업데이트
    update_unknown_patterns(values, meta_type)


def update_unknown_patterns(values: list, meta_type: str):
    """처리된 unknown 패턴을 검토 완료로 표시"""
    if not os.path.exists(UNKNOWN_DB_PATH):
        return
    
    conn = sqlite3.connect(UNKNOWN_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    placeholders = ','.join(['?'] * len(values))
    conn.execute(f"""
        UPDATE unknown_patterns
        SET is_reviewed = 1, suggested_type = ?
        WHERE value IN ({placeholders})
    """, (meta_type, *values))
    
    affected = conn.total_changes
    conn.commit()
    conn.close()
    
    print(f"✅ {affected}개 unknown 패턴 검토 완료 처리")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="unknown 패턴 분석기")
    parser.add_argument("--min-freq", type=int, default=10, help="최소 빈도수 (기본: 10)")
    parser.add_argument("--limit", type=int, default=50, help="분석할 패턴 수 (기본: 50)")
    
    subparsers = parser.add_subparsers(dest="command", help="명령어")
    
    # 분석 명령
    analyze_parser = subparsers.add_parser("analyze", help="패턴分析")
    
    # 마이그레이션 명령
    migrate_parser = subparsers.add_parser("migrate", help="패턴 마이그레이션")
    migrate_parser.add_argument("--type", required=True, help="메타 타입")
    migrate_parser.add_argument("--values", required=True, help="콤마로 구분된 값 리스트")
    
    args = parser.parse_args()
    
    if args.command == "analyze" or len(sys.argv) == 1:
        analyze_patterns(min_frequency=args.min_freq, limit=args.limit)
    
    elif args.command == "migrate":
        values = [v.strip() for v in args.values.split(",")]
        migrate_pattern_to_meta(args.type, values)
    
    else:
        parser.print_help()
        