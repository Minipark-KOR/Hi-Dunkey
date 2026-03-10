#!/usr/bin/env python3
"""
백업 파일 관리 전용 스크립트
- backup 디렉토리에서 3년 이상 된 파일을 archive로 이동
"""
import os
import sys
import shutil
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.kst_time import now_kst
from core.logger import build_logger
from constants.paths import BACKUP_DIR, ARCHIVE_DIR, LOG_DIR

logger = build_logger("move_feb22", str(LOG_DIR / "move_feb22.log"))

def move_old_backups(cutoff_years: int = 3):
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    if not os.path.exists(BACKUP_DIR):
        logger.warning(f"⚠️ 백업 디렉토리 없음: {BACKUP_DIR}")
        return

    cutoff = now_kst().date() - timedelta(days=365 * cutoff_years)
    moved  = 0

    for fname in os.listdir(BACKUP_DIR):
        if not fname.endswith(".db"):
            continue
        try:
            fdate = datetime.strptime(fname.split("_")[0], "%Y%m%d").date()
        except (ValueError, IndexError):
            continue

        if fdate >= cutoff:
            continue

        src = os.path.join(BACKUP_DIR, fname)
        dst = os.path.join(ARCHIVE_DIR, fname)

        if os.path.exists(dst):
            base, ext = os.path.splitext(fname)
            cnt = 1
            while os.path.exists(os.path.join(ARCHIVE_DIR, f"{base}_{cnt}{ext}")):
                cnt += 1
            dst = os.path.join(ARCHIVE_DIR, f"{base}_{cnt}{ext}")

        shutil.move(src, dst)
        logger.info(f"  📦 이동: {fname} → {os.path.basename(dst)}")
        moved += 1

    logger.info(f"✅ {moved}개 파일 archive 이동 완료 (기준일: {cutoff})")


def main():
    logger.info("=" * 60)
    logger.info("📦 백업 파일 아카이빙 프로세스 시작")
    move_old_backups()
    logger.info("✅ 관리 프로세스 종료")


if __name__ == "__main__":
    main()
    