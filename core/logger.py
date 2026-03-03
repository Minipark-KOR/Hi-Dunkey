#!/usr/bin/env python3
# core/logger.py
import logging
from logging.handlers import RotatingFileHandler
import os

def build_logger(name: str, log_file: str, level=logging.INFO):
    """
    RotatingFileHandler 를 적용한 로거 생성 (5MB, 5 개 백업)
    - 이미 핸들러가 있으면 중복 추가하지 않음
    - LOG_CONSOLE 환경변수가 'true'이면 콘솔에도 출력
    """
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
    