#!/usr/bin/env python3
"""
백업/아카이브 공통 함수
"""
import os
import shutil
import glob
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional

def vacuum_into(src_path: str, dst_path: str) -> None:
    """
    VACUUM INTO로 압축된 백업 파일 생성
    """
    if not os.path.exists(src_path):
        return
    with sqlite3.connect(src_path) as conn:
        conn.execute(f"VACUUM INTO '{dst_path}'")

def move_files_by_age(src_dir: str, dst_dir: str, cutoff_year: int,
                      pattern: str = "*.db") -> List[str]:
    """
    src_dir에서 파일명의 첫 번째 토큰(년도)이 cutoff_year 이하인 파일을 dst_dir로 이동
    반환: 이동된 파일명 리스트
    """
    os.makedirs(dst_dir, exist_ok=True)
    moved = []
    for file_path in glob.glob(os.path.join(src_dir, pattern)):
        fname = os.path.basename(file_path)
        try:
            year = int(fname.split('_')[0])
            if year <= cutoff_year:
                shutil.move(file_path, os.path.join(dst_dir, fname))
                moved.append(fname)
        except (ValueError, IndexError):
            continue
    return moved

def cleanup_files_older_than(dir_path: str, days: int = 365,
                             pattern: str = "*.db",
                             exclude_pattern: Optional[str] = None) -> List[str]:
    """
    디렉토리 내에서 파일명 마지막 부분에 YYYYMMDD 형식의 날짜가 포함된 파일 중
    days 일보다 오래된 파일 삭제
    반환: 삭제된 파일 경로 리스트
    """
    now = datetime.now()
    deleted = []
    for file_path in glob.glob(os.path.join(dir_path, pattern)):
        fname = os.path.basename(file_path)
        if exclude_pattern and exclude_pattern in fname:
            continue
        # 파일명에서 날짜 추출 (마지막 _YYYYMMDD.db)
        parts = fname.split('_')
        if len(parts) < 2:
            continue
        date_str = parts[-1].replace('.db', '')
        try:
            file_date = datetime.strptime(date_str, '%Y%m%d')
            if (now - file_date) > timedelta(days=days):
                os.remove(file_path)
                deleted.append(file_path)
        except (ValueError, IndexError):
            continue
    return deleted

def get_block_range(year: int, base_year: int = 2026) -> tuple:
    """
    year가 속한 10년 블록의 (시작, 끝) 반환
    base_year는 첫 해 (예: 2026)
    """
    offset = (year - base_year) // 10
    start = base_year + offset * 10
    end = start + 9
    return start, end
    