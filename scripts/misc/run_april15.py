#!/usr/bin/env python3
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.util.manage_log import build_domain_logger
from core.data.backup import cleanup_files_older_than
from scripts.misc.run_feb22 import update_archive_merged
from constants.paths import LOG_DIR, ARCHIVE_DIR   # 추가

logger = build_domain_logger("april15", "april15", __file__)

def main():
    logger.info("="*60)
    logger.info("🏁 4월 15일 archive 정리 시작")
    # 1년 지난 파일 재삭제 (안전장치)
    cleanup_files_older_than(str(ARCHIVE_DIR), days=365, exclude_pattern="_merged")
    # 통합본 다시 갱신 (혹시 누락된 데이터 방지)
    update_archive_merged()
    logger.info("✅ 4월 15일 완료")

if __name__ == "__main__":
    main()
    