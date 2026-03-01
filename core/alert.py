#!/usr/bin/env python3
"""
알림 모듈 (Slack, 이메일 등 연동)
"""
import logging

logger = logging.getLogger("alert")

def send_alert(
    message: str,
    level: str = "error",
    channel: str = None,
    *,
    exc_info: Exception = None
):
    """
    운영자에게 알림을 보냅니다.
    - level: 'critical', 'error', 'warning', 'info'
    - channel: 'slack', 'email', 'pagerduty' (미구현시 로그만)
    """
    log_func = getattr(logger, level, logger.error)
    log_func(f"[ALERT] {message}", exc_info=exc_info)
    # TODO: 실제 슬랙/webhook 연동 시 구현
    