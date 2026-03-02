#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
import argparse
import sqlite3
from datetime import datetime
from typing import Dict, Any, Callable, Tuple, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from core.kst_time import now_kst
from core.filters import AddressFilter
from collectors.geo_collector import GeoCollector

logger = build_logger("retry_worker", "logs/retry_worker.log")

HandlerResult = Tuple[bool, bool]  # (success, is_permanent_failure)
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}

_GEO_COLLECTOR: Optional[GeoCollector] = None


def kst_naive(dt: datetime) -> datetime:
    if getattr(dt, "tzinfo", None) is not None:
        return dt.replace(tzinfo=None)
    return dt


def register_handler(domain: str, task_type: str, handler: Callable[[Dict[str, Any]], HandlerResult]):
    TASK_HANDLERS[(domain, task_type)] = handler


def get_geo_collector() -> GeoCollector:
    global _GEO_COLLECTOR
    if _GEO_COLLECTOR is None:
        _GEO_COLLECTOR = GeoCollector(
            global_db_path="data/global_vocab.db",
            school_db_path="data/master/school_info.db",
            debug_mode=False,
        )
    return _GEO_COLLECTOR


def update_school_coords(
    school_db_path: str,
    sc_code: str,
    lon: float,
    lat: float,
    cleaned: str,
    addr_components: Dict[str, Any],
):
    with sqlite3.connect(school_db_path, timeout=30) as conn:
        try:
            conn.execute(
                """
                UPDATE schools
                SET longitude = ?, latitude = ?,
                    cleaned_address = ?,
                    geocode_attempts = 0,
                    last_error = NULL,
                    city_id = ?, district_id = ?, street_id = ?,
                    number_type = ?, number_value = ?, number_start = ?, number_end = ?, number_bit = ?
                WHERE sc_code = ?
                """,
                (
                    lon, lat, cleaned,
                    addr_components.get("city_id", 0),
                    addr_components.get("district_id", 0),
                    addr_components.get("street_id", 0),
                    addr_components.get("number_type"),
                    addr_components.get("number"),
                    addr_components.get("number_start"),
                    addr_components.get("number_end"),
                    addr_components.get("number_bit"),
                    sc_code,
                ),
            )
        except sqlite3.OperationalError:
            conn.execute(
                """
                UPDATE schools
                SET longitude = ?, latitude = ?, cleaned_address = ?
                WHERE sc_code = ?
                """,
                (lon, lat, cleaned, sc_code),
            )


def handle_school_geocode(failure: dict) -> HandlerResult:
    sc_code = failure.get("sc_code")
    address = failure.get("address")
    retries = failure.get("retries") or 0

    if not sc_code or not address:
        return (False, True)

    level = min(max(int(retries), 1), 3)
    cleaned = AddressFilter.clean(address, level=level)

    gc = get_geo_collector()
    try:
        coords = gc._geocode(cleaned)
    except Exception as e:
        logger.error(f"geocode error: {e}", exc_info=True)
        return (False, False)

    if not coords:
        return (False, False)

    lon, lat = coords
    try:
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(
            school_db_path="data/master/school_info.db",
            sc_code=sc_code,
            lon=lon,
            lat=lat,
            cleaned=cleaned,
            addr_components=addr_components,
        )
        return (True, False)
    except Exception as e:
        logger.error(f"update error: {e}", exc_info=True)
        return (False, True)


register_handler("school", "geocode", handle_school_geocode)


def main():
    parser = argparse.ArgumentParser(description="retry worker")
    parser.add_argument("--limit", type=int, default=50, help="한 번에 처리할 작업 수")
    args = parser.parse_args()

    rm = RetryManager(max_retries=None, base_delay=60, backoff_factor=2, deadline_buffer_seconds=70)
    logger.info(f"retry_worker start. now(KST)={kst_naive(now_kst())}")

    failures = rm.get_pending_retries(limit=args.limit, deadline=None)  # 모든 pending 작업
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

        handler_exc = None
        try:
            success, is_permanent = handler(f)
        except Exception as e:
            handler_exc = str(e)[:200]
            logger.error(f"handler exception: {e}", exc_info=True)
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
            error=handler_exc or "재시도 실패",
            deadline=None,  # 데드라인 제한 없음
        )
        if still_alive:
            logger.info(f"실패, 다음 재시도 예약됨 id={failure_id}")
        else:
            logger.warning(f"최대 재시도 초과로 포기/만료 id={failure_id}")

    logger.info("재시도 워커 종료")


if __name__ == "__main__":
    main()
    