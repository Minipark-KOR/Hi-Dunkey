#!/usr/bin/env python3
"""
프로젝트 공통 경로 상수
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ACTIVE_DIR = PROJECT_ROOT / "data" / "active"
MASTER_DIR = PROJECT_ROOT / "data" / "master"
METRICS_DIR = PROJECT_ROOT / "data" / "metrics"
BACKUP_DIR = PROJECT_ROOT / "data" / "backup"
ARCHIVE_DIR = PROJECT_ROOT / "data" / "archive"

GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
UNKNOWN_DB_PATH = str(ACTIVE_DIR / "unknown_patterns.db")
MASTER_DB_PATH = str(MASTER_DIR / "school_info.db")   # ✅ 변경
