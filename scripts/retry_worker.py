#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
import argparse
import sqlite3
import re
from datetime import datetime, time
from typing import Dict, Any, Callable, Tuple, Optional
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from core.kst_time import now_kst
from core.filters import AddressFilter
from core.error_classifier import classify_error
from collectors.geo_collector import GeoCollector

logger = build_logger("retry_worker", "logs/retry_worker.log")

HandlerResult = Tuple[bool, bool]  # (success, is_permanent_failure)
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}

_GEO_COLLECTOR: Optional[GeoCollector] = None
_SCHOOL_DB = "data/master/school_info.db"
_FAILURES_DB = "data/failures.db"


def kst_naive(dt: datetime) -> datetime:
    if getattr(dt, "tzinfo", None) is not None:
        return dt.replace(tzinfo=None)
    return dt


def get_today_3pm_kst_naive(now: datetime) -> datetime:
    n = kst_naive(now)
    return datetime.combine(n.date(), time(15, 0))


def register_handler(domain: str, task_type: str, handler: Callable[[Dict[str, Any]], HandlerResult]):
    TASK_HANDLERS[(domain, task_type)] = handler


def get_geo_collector() -> GeoCollector:
    global _GEO_COLLECTOR
    if _GEO_COLLECTOR is None:
        _GEO_COLLECTOR = GeoCollector(
            global_db_path="data/global_vocab.db",
            school_db_path=_SCHOOL_DB,
            failures_db_path=_FAILURES_DB,
            debug_mode=False,
        )
    return _GEO_COLLECTOR


def _geocode_vworld(gc: GeoCollector, address: str, addr_type: str = "ROAD") -> Tuple[Optional[Tuple[float, float]], int]:
    """
    VWorld API 호출, (좌표, 상태코드) 반환
    - 성공: (좌표, 200)
    - 실패: (None, 상태코드)
    """
    if not gc.vworld_key:
        logger.error("VWORLD_API_KEY not set")
        return None, 0

    try:
        coords = gc._geocode_with_type(address, addr_type)
        if coords:
            return coords, 200
        # 좌표 없음은 404 로 간주
        return None, 404
    except requests.exceptions.HTTPError as e:
        if hasattr(e, 'response') and e.response is not None:
            return None, e.response.status_code
        return None, 500
    except requests.exceptions.RequestException as e:
        logger.error(f"VWorld {addr_type} request error: {e}")
        return None, 503
    except Exception as e:
        logger.error(f"VWorld {addr_type} unexpected error: {e}")
        return None, 500


def geocode_kakao(address: str) -> Tuple[Optional[Tuple[float, float]], int]:
    KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")
    if not KAKAO_API_KEY:
        return None, 0

    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address, "analyze_type": "exact"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('documents'):
                x = float(data['documents'][0]['x'])
                y = float(data['documents'][0]['y'])
                return (x, y), 200
        return None, resp.status_code
    except Exception as e:
        logger.error(f"Kakao geocode error: {e}")
        return None, 500


def update_school_coords(sc_code: str, lon: float, lat: float, cleaned: str, addr_components: Dict[str, Any]):
    with sqlite3.connect(_SCHOOL_DB, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
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


def get_consecutive_404(sc_code: str) -> int:
    with sqlite3.connect(_FAILURES_DB) as conn:
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM failures 
            WHERE sc_code=? AND error_msg LIKE '%404%' 
            AND status='FAILED' AND resolved_at IS NULL
            """,
            (sc_code,)
        )
        return cur.fetchone()[0] or 0


def handle_school_geocode(failure: dict) -> HandlerResult:
    sc_code = failure.get("sc_code")
    address = failure.get("address")
    retries = failure.get("retries") or 0

    if not sc_code or not address:
        return (False, True)

    level = min(max(int(retries), 1), 4)
    cleaned = AddressFilter.clean(address, level=level)
    gc = get_geo_collector()

    final_status = 0
    error_messages = []

    # 1 차: VWorld road
    coords, status = _geocode_vworld(gc, cleaned, "ROAD")
    if coords:
        lon, lat = coords
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components)
        return (True, False)
    if status > 0:
        final_status = status
        error_messages.append(f"ROAD:{status}")

    # 2 차: VWorld parcel
    coords, status = _geocode_vworld(gc, cleaned, "PARCEL")
    if coords:
        lon, lat = coords
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components)
        return (True, False)
    if status > 0:
        final_status = status
        error_messages.append(f"PARCEL:{status}")

    # simplified 는 항상 정의
    simplified = re.sub(r'\s*[-,.+()].*$', '', cleaned).strip()

    # 3 차: simplified road
    if simplified != cleaned:
        coords, status = _geocode_vworld(gc, simplified, "ROAD")
        if coords:
            lon, lat = coords
            addr_components = gc.meta_vocab.save_address(simplified)
            update_school_coords(sc_code, lon, lat, simplified, addr_components)
            return (True, False)
        if status > 0:
            final_status = status
            error_messages.append(f"SIMPLIFIED_ROAD:{status}")

    # 4 차: simplified parcel
    if simplified != cleaned:
        coords, status = _geocode_vworld(gc, simplified, "PARCEL")
        if coords:
            lon, lat = coords
            addr_components = gc.meta_vocab.save_address(simplified)
            update_school_coords(sc_code, lon, lat, simplified, addr_components)
            return (True, False)
        if status > 0:
            final_status = status
            error_messages.append(f"SIMPLIFIED_PARCEL:{status}")

    # 5 차: Kakao API
    coords, status = geocode_kakao(cleaned)
    if coords:
        logger.info(f"Kakao API success for {cleaned[:30]}")
        lon, lat = coords
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components)
        return (True, False)
    if status > 0:
        final_status = status
        error_messages.append(f"KAKAO:{status}")

    # 실패 처리: 에러 분류
    consec_404 = get_consecutive_404(sc_code)
    err_info = classify_error(final_status, consec_404)

    # 에러 메시지 저장
    full_error = f"[{','.join(error_messages)}] {err_info['message']}"

    if err_info['action'] == 'stop':
        logger.critical(f"Fatal auth error for {sc_code}: {err_info['message']}")
        return (False, True)
    elif err_info['action'] == 'orphan':
        logger.warning(f"Orphan detected for {sc_code}: {full_error}")
        return (False, True)
    else:
        logger.info(f"Transient failure for {sc_code}: {full_error}")
        return (False, False)


register_handler("school", "geocode", handle_school_geocode)


def main():
    parser = argparse.ArgumentParser(description="retry worker")
    parser.add_argument("--limit", type=int, default=50, help="한 번에 처리할 작업 수")
    args = parser.parse_args()

    now = kst_naive(now_kst())
    deadline = get_today_3pm_kst_naive(now)

    rm = RetryManager(
        db_path=_FAILURES_DB,
        max_retries=None,
        base_delay=60,
        backoff_factor=2,
        deadline_buffer_seconds=70
    )

    logger.info(f"retry_worker start. now(KST)={now}, deadline={deadline}")
    print(f"🚀 retry_worker 시작 - {now}")

    failures = rm.get_pending_retries(limit=args.limit, deadline=deadline)
    if not failures:
        logger.info("재시도할 작업 없음")
        print("ℹ️  재시도할 작업 없음")
        return

    logger.info(f"총 {len(failures)}개 작업 재시도")
    print(f"📋 총 {len(failures)}개 작업 재시도")

    success_count = 0
    orphan_count = 0
    retry_count = 0

    for f in failures:
        domain = f["domain"]
        task_type = f["task_type"]
        failure_id = f["id"]

        handler = TASK_HANDLERS.get((domain, task_type))
        if not handler:
            msg = f"handler not found: {domain}/{task_type}"
            logger.warning(f"{msg} id={failure_id}")
            rm.mark_orphan(failure_id, error=msg)
            orphan_count += 1
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
            logger.warning(f"영구 실패 (orphan) id={failure_id}")
            print(f"❌ 포기 id={failure_id}")
            orphan_count += 1
            continue

        if success:
            rm.mark_resolved(failure_id, status="SUCCESS")
            logger.info(f"성공 id={failure_id}")
            print(f"✅ 성공 id={failure_id}")
            success_count += 1
            continue

        still_alive = rm.schedule_retry_by_id(
            failure_id=failure_id,
            error=handler_exc or "재시도 실패",
            deadline=deadline,
        )
        if still_alive:
            logger.info(f"실패, 다음 재시도 예약됨 id={failure_id}")
            print(f"⏳ 실패, 다음 재시도 예약됨 id={failure_id}")
            retry_count += 1
        else:
            logger.warning(f"데드라인 도달 또는 최대 재시도 초과로 포기 id={failure_id}")
            print(f"❌ 포기 id={failure_id}")
            orphan_count += 1

    logger.info(f"재시도 워커 종료 - 성공:{success_count}, 재시도:{retry_count}, 포기:{orphan_count}")
    print(f"🏁 재시도 워커 종료 - 성공:{success_count}, 재시도:{retry_count}, 포기:{orphan_count}")


if __name__ == "__main__":
    try:
        main()
    finally:
        if _GEO_COLLECTOR is not None:
            _GEO_COLLECTOR.flush()
            _GEO_COLLECTOR.meta_vocab.flush()
            