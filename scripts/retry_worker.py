#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
import sqlite3
import argparse
from datetime import datetime, time
from typing import Dict, Any, Callable, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from core.kst_time import now_kst

logger = build_logger("retry_worker", "logs/retry_worker.log")

HandlerResult = Tuple[bool, bool]  # (success: bool, is_permanent_failure: bool)
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}


def kst_naive(dt: datetime) -> datetime:
    if getattr(dt, "tzinfo", None) is not None:
        return dt.replace(tzinfo=None)
    return dt


def get_today_3pm_kst_naive(now: datetime) -> datetime:
    n = kst_naive(now)
    return datetime.combine(n.date(), time(15, 0))


def register_handler(domain: str, task_type: str, handler: Callable[[Dict[str, Any]], HandlerResult]):
    TASK_HANDLERS[(domain, task_type)] = handler


def count_due_before_deadline(rm: RetryManager, deadline: datetime) -> int:
    # RetryManager에 public method가 없으므로(현재 구조 기준) 연결을 직접 사용합니다.
    with rm._get_connection() as conn:  # noqa: SLF001 (internal 사용)
        cur = conn.execute(
            """
            SELECT COUNT(*)
            FROM failures
            WHERE status = 'FAILED'
              AND resolved_at IS NULL
              AND next_attempt IS NOT NULL
              AND next_attempt <= ?
            """,
            (deadline,),
        )
        return int(cur.fetchone()[0] or 0)


def main():
    parser = argparse.ArgumentParser(description="retry worker")
    parser.add_argument("--ignore-deadline", action="store_true", help="데드라인 무시하고 재시도 실행")
    args = parser.parse_args()

    now = kst_naive(now_kst())
    deadline = get_today_3pm_kst_naive(now)

    rm = RetryManager(max_retries=None, base_delay=60, backoff_factor=2, deadline_buffer_seconds=70)
    logger.info(f"retry_worker 시작. now(KST)={now}, deadline(KST)={deadline}, ignore_deadline={args.ignore_deadline}")

    # ✅ 정책: 15시 이후에는 자동 재시도(핸들러 실행) 금지
    if (now >= deadline) and (not args.ignore_deadline):
        due = count_due_before_deadline(rm, deadline)
        logger.warning(
            f"데드라인 이후이므로 자동 재시도를 수행하지 않습니다. "
            f"(deadline={deadline}, 남아있는 대상(<=deadline)={due}건) "
            f"알림/만료 처리는 deadline_notifier가 담당합니다."
        )
        return

    # 15시 전에는 데드라인 이내 작업만(정책 안전장치)
    failures = rm.get_pending_retries(limit=50, deadline=None if args.ignore_deadline else deadline)

    if not failures:
        logger.info("재시도할 작업 없음")
        return

    logger.info(f"총 {len(failures)}개 작업 재시도")

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

        # 일시 실패: 데드라인 기준으로 스케줄/만료 결정
        still_alive = rm.schedule_retry_by_id(
            failure_id=failure_id,
            error="재시도 실패",
            deadline=None if args.ignore_deadline else deadline,
        )
        if still_alive:
            logger.info(f"실패, 다음 재시도 예약됨 id={failure_id}")
        else:
            logger.warning(f"데드라인 도달 또는 최대 재시도 초과로 포기/만료 id={failure_id}")

    logger.info("재시도 워커 종료")


if __name__ == "__main__":
    main()
