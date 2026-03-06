#!/usr/bin/env python3
"""
학교 기본정보 수집기 (Diff 기반 좌표 갱신)
- GeoCollector 통합으로 캐시 및 API 사용량 추적
- 기본 모드에서도 지역별 좌표 현황 출력
- 전체 수집 완료 후 누적 통계 표시 (성공률 포함)
- GitHub Actions 환경에서는 자동으로 quiet 모드
- 진행률에 [LIMIT:xx] 표시 추가
- 지번 주소 (jibun_address) 추출 및 저장
"""
import os
import sys
import time
import sqlite3
import argparse
from typing import List, Dict, Tuple, Optional
from datetime import timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from core.database import get_db_connection, get_db_reader
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from core.filters import AddressFilter
from core.kst_time import now_kst
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS, REGION_NAMES
from constants.paths import MASTER_DB_PATH as MASTER_DB, MASTER_DIR
from collectors.geo_collector import GeoCollector

BASE_DIR = str(MASTER_DIR)
GLOBAL_VOCAB_PATH = str(MASTER_DIR.parent / "active" / "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']

# ✅ ANSI 색상 코드
GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"

if __name__ == "__main__":
    # ✅ GitHub Actions 환경 감지
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'

    parser = argparse.ArgumentParser(description="학교 기본정보 수집기")
    parser.add_argument("--regions", default="ALL", help="수집할 지역 (ALL 또는 쉼표 구분, 예: B10,C10)")
    parser.add_argument("--debug", action="store_true", help="상세 출력 모드")
    parser.add_argument("--quiet", action="store_true", help="출력 최소화 (GitHub Actions 등)")
    parser.add_argument("--limit", type=int, default=None, help="수집할 학교 수 제한 (테스트용)")
    args = parser.parse_args()

    # ✅ GitHub Actions 환경에서는 자동으로 quiet 모드 활성화
    if is_github_actions and not args.quiet:
        args.quiet = True

    collector = NeisInfoCollector(
        shard="none",
        debug_mode=args.debug,
        quiet_mode=args.quiet
    )

    if args.regions == "ALL":
        regions = ALL_REGIONS
    else:
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    if not args.quiet:
        print(f"\n🚀 학교 정보 수집 시작 (지역: {len(regions)}개, limit: {args.limit or '전체'})")
        print("=" * 70)

    for region in regions:
        collector.fetch_region(region, limit=args.limit)
        if args.limit:
            break

    collector.close()

    # ✅ 전체 통계 출력 (quiet 모드가 아니면)
    if not args.quiet:
        total = collector.total_new + collector.total_failed + collector.total_skipped
        success_rate = (collector.total_new / total * 100) if total > 0 else 0

        print("=" * 70)
        print(f"📊 전체 통계")
        print(f"   신규 성공: {collector.total_new}개 ({success_rate:.1f}%)")
        print(f"   실패:      {collector.total_failed}개")
        print(f"   스킵:      {collector.total_skipped}개")
        print(f"   총 처리:   {total}개")
        print("=" * 70)
        print("✅ 수집 완료")
    else:
        collector.logger.info("수집 완료")
        