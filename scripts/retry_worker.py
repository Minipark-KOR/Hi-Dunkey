#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
from datetime import datetime, time
from typing import Dict, Any, Callable, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from core.kst_time import now_kst

logger = build_logger("retry_worker", "logs/retry_worker.log")

HandlerResult = Tuple[bool, bool]  # (success, is_permanent_failure)
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}


def get_today_3pm_kst_naive() -> datetime:
    n = now_kst()
    if getattr(n, "tzinfo", None) is not None:
        n = n.replace(tzinfo=None)
    return datetime.combine(n.date(), time(15, 0))


def register_handler(domain: str, task_type: str, handler: Callable[[Dict[str, Any]], HandlerResult]):
    TASK_HANDLERS[(domain, task_type)] = handler


def main():
    deadline = get_today_3pm_kst_naive()
    now = now_kst()
    if getattr(now, "tzinfo", None) is not None:
        now = now.replace(tzinfo=None)

    if now >= deadline:
        logger.info(f"데드라인 이후({now} >= {deadline})이므로 retry_worker를 실행하지 않습니다.")
        return

    rm = RetryManager(max_retries=None, base_delay=60, backoff_factor=2, deadline_buffer_seconds=70)
    logger.info(f"오늘의 재시도 데드라인(KST): {deadline}")

    failures = rm.get_pending_retries(limit=50, deadline=deadline)
    if not failures:
        logger.info("재시도할 작업 없음")
        return

    logger.info(f"총 {len(failures)}개 작업 재시도 (데드라인: {deadline})")

    for f in failures:
        domain = f["domain"]
        task_type = f["task_type"]
        failure_id = f["id"]

        handler = TASK_HANDLERS.get((domain, task_type))
        if not handler:
            msg = f"handler not found: {domain}/{task_type}"
            logger.warning(f"{msg} id={failure_id}")
            rm.mark_orphan(failure_id, error=msg)
            continue

        try:
            success, is_permanent = handler(f)
        except Exception as e:
            logger.error(f"핸들러 예외: {e}", exc_info=True)
            success, is_permanent = False, False

        if (not success) and is_permanent:
            rm.mark_orphan(failure_id, error=f"permanent failure: {domain}/{task_type}")
            logger.warning(f"영구 실패(orphan) id={failure_id}")
            continue

        if success:
            rm.mark_resolved(failure_id, status="SUCCESS")
            logger.info(f"성공 id={failure_id}")
            continue

        still_alive = rm.schedule_retry_by_id(
            failure_id=failure_id,
            error="재시도 실패",
            deadline=deadline,
        )
        if still_alive:
            logger.info(f"실패, 다음 재시도 예약됨 id={failure_id}")
        else:
            logger.warning(f"데드라인 도달 또는 최대 재시도 초과로 포기 id={failure_id}")

    logger.info("재시도 워커 종료")


if __name__ == "__main__":
    main()