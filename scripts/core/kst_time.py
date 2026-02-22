#!/usr/bin/env python3
"""
한국시간(KST) 처리 유틸리티
"""
from datetime import datetime, timedelta
import pytz

KST = pytz.timezone('Asia/Seoul')
UTC = pytz.UTC

def now_kst() -> datetime:
    """현재 한국시간 반환"""
    return datetime.now(KST)

def now_utc() -> datetime:
    """현재 UTC 시간 반환"""
    return datetime.now(UTC)

def kst_to_utc(dt: datetime) -> datetime:
    """한국시간 → UTC 변환"""
    if dt.tzinfo is None:
        dt = KST.localize(dt)
    return dt.astimezone(UTC)

def utc_to_kst(dt: datetime) -> datetime:
    """UTC → 한국시간 변환"""
    if dt.tzinfo is None:
        dt = UTC.localize(dt)
    return dt.astimezone(KST)

def get_kst_time() -> str:
    """한국시간 문자열 (로깅용)"""
    return now_kst().strftime("%Y-%m-%d %H:%M:%S KST")

def get_utc_time() -> str:
    """UTC 시간 문자열 (저장용)"""
    return now_utc().strftime("%Y-%m-%d %H:%M:%S UTC")
    