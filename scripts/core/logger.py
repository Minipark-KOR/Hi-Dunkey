#!/usr/bin/env python3
"""
로깅 설정
"""
import os
import logging
from typing import Optional

def build_logger(name: str, log_path: str, level: int = logging.INFO) -> logging.Logger:
    """
    로거 생성 (파일 + 콘솔)
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # 파일 핸들러
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # 콘솔 핸들러
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    return logger
    