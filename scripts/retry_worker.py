#!/usr/bin/env python3
"""
재시도 워커 - 실패한 작업을 지수 백오프로 재시도
크론탭 1분 간격 실행 권장
"""
import os
import sys
import sqlite3
from typing import Dict, Any, Callable

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger

logger = build_logger("retry_worker", "logs/retry_worker.log")
rm = RetryManager()

TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], bool]] = {}


def register_handler(domain: str, task_type: str, handler: Callable):
    TASK_HANDLERS[(domain, task_type)] = handler


def handle_geocode_retry(failure: Dict[str, Any]) -> bool:
    try:
        from core.geo import VWorldGeocoder

        sc_code = failure.get('sc_code', '')
        address = failure.get('address', '')
        shard   = failure.get('shard', '')

        if not sc_code or not address or not shard:
            logger.error(f"지오코딩 재시도: 필수 정보 부족 id={failure['id']}")
            rm.mark_orphan(failure['id'], "필수 정보 부족")
            return True

        geo = VWorldGeocoder(calls_per_second=3.0)
        coords = geo.geocode(address)
        if not coords:
            return False

        lon, lat = coords
        db_path = f"data/master/school_{shard}.db"

        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            updated = conn.execute(
                "UPDATE schools SET longitude=?, latitude=? WHERE sc_code=?",
                (lon, lat, sc_code)
            ).rowcount
            conn.commit()

        if updated == 0:
            rm.mark_orphan(failure['id'], f"sc_code={sc_code} not found in {db_path}")
            return True

        rm.mark_resolved(failure['id'], status='SUCCESS')
        return True

    except Exception as e:
        logger.error(f"지오코딩 재시도 예외: {e}", exc_info=True)
        return False


register_handler('geo', 'geocode', handle_geocode_retry)


def main():
    logger.info("🚀 재시도 워커 시작")
    failures = rm.get_pending_retries(limit=50)

    if not failures:
        logger.info("재시도할 작업 없음")
        return

    logger.info(f"총 {len(failures)}개 작업 재시도")

    for f in failures:
        domain, task_type, failure_id = f['domain'], f['task_type'], f['id']
        handler = TASK_HANDLERS.get((domain, task_type))

        if not handler:
            logger.warning(f"처리기 없음: {domain}/{task_type} id={failure_id}")
            continue

        logger.info(f"재시도: {domain}/{task_type} id={failure_id} (retries={f['retries']})")
        success = handler(f)

        if not success:
            still_alive = rm.record_failure(
                domain=domain, task_type=task_type,
                shard=f.get('shard'), sc_code=f.get('sc_code'), region=f.get('region'),
                year=f.get('year'), month=f.get('month'), day=f.get('day'),
                semester=f.get('semester'), address=f.get('address'),
                sub_key=f.get('sub_key'), error="재시도 실패"
            )
            if still_alive:
                logger.info(f"❌ 실패, 다음 재시도 예약됨 id={failure_id}")
            else:
                logger.warning(f"🚫 최대 재시도 초과, 포기 id={failure_id}")

    logger.info("✅ 재시도 워커 종료")


if __name__ == "__main__":
    main()
    