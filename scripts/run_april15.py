#!/usr/bin/env python3
import os
from core.logger import build_logger
from core.backup import cleanup_files_older_than
from run_feb22 import update_archive_merged
from constants.paths import LOG_DIR   # 추가

logger = build_logger("april15", str(LOG_DIR / "april15.log"))   # 수정

def main():
    logger.info("="*60)
    logger.info("🏁 4월 15일 archive 정리 시작")
    # 1년 지난 파일 재삭제 (안전장치)
    cleanup_files_older_than("../data/archive", days=365, exclude_pattern="_merged")
    # 통합본 다시 갱신 (혹시 누락된 데이터 방지)
    update_archive_merged()
    logger.info("✅ 4월 15일 완료")

if __name__ == "__main__":
    main()
    