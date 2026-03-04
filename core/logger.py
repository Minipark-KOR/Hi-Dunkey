#!/usr/bin/env python3
# core/logger.py
import logging
from logging.handlers import RotatingFileHandler
import os

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
    