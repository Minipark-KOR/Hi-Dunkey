#!/usr/bin/env python3
# core/util/manage_log.py
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

from constants.paths import LOG_DIR


def resolve_domain_log_path(domain: str, source_file: str | None = None) -> Path:
    """도메인 로그 경로를 반환합니다.

    기본 형식: data/logs/domain.log
    source_file가 있으면 충돌 방지를 위해 앞폴더를 붙입니다.
    예) scripts/run/retry_worker.py + retry_worker -> data/logs/run.retry_worker.log
    """
    safe_domain = str(domain).strip().replace(" ", "_")
    if not source_file:
        return LOG_DIR / f"{safe_domain}.log"

    parent = Path(source_file).resolve().parent.name
    if parent in {"core", "scripts", ""}:
        return LOG_DIR / f"{safe_domain}.log"
    return LOG_DIR / f"{parent}.{safe_domain}.log"

def build_logger(name: str, log_file: str, level=logging.INFO):
    """RotatingFileHandler 적용 로거 (5MB, 5개 백업, 중복 방지)"""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if os.environ.get('LOG_CONSOLE', 'false').lower() == 'true':
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger


def build_domain_logger(name: str, domain: str, source_file: str | None = None, level=logging.INFO):
    """도메인 규칙 기반 로거를 생성합니다.

    - 로그 루트는 항상 data/logs
    - 파일명은 <parent>.<domain>.log 또는 <domain>.log
    """
    log_path = resolve_domain_log_path(domain, source_file)
    return build_logger(name, str(log_path), level=level)
    