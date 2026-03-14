#!/usr/bin/env python3
"""
데이터 급감 감지 및 보호 (DataGuard)
"""
import logging
from datetime import datetime
from core.kst_time import now_kst

logger = logging.getLogger("data_guard")

class DataGuard:
    """데이터 급감 세이프가드"""

    # 정상적으로 데이터가 줄어드는 케이스
    SKIP_GUARD_CONDITIONS = [
        "학년도 전환 직후 (3월 1~7일)",   # 새 학년도라 데이터 적음
        "최초 수집 (old_count == 0)",       # 비교 대상 없음
    ]

    def __init__(self, conn=None):
        self.conn = conn

    def check_data_drop(
        self,
        table: str,
        school_id: int,
        new_count: int,
        old_count: int,
        threshold: float = 0.9,
        conn=None,
    ) -> bool:
        """
        True: 정상 (계속 진행)
        False: 급감 감지 (rollback 필요)
        """
        # 예외: 최초 수집
        if old_count == 0:
            return True

        # 예외: 학년도 전환 기간 (3/1 ~ 3/7)
        today = now_kst()
        if today.month == 3 and today.day <= 7:
            logger.info("학년도 전환기간: 데이터 급감 검사 건너뜀")
            return True

        if new_count < old_count * threshold:
            logger.error(
                f"🚨 데이터 급감 감지 [{table}] school={school_id} "
                f"{old_count} → {new_count} "
                f"({new_count/old_count:.0%}) — 롤백"
            )
            return False
        return True

class DataDropException(Exception):
    """데이터 급감 시 발생시키는 예외"""
    pass
    