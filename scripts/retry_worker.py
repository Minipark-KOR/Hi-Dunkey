#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
import argparse
import sqlite3
import re
import json
import time
from datetime import datetime, time as dt_time
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
address_mapping_logger = build_logger("address_mapping", "logs/address_mapping.log")

HandlerResult = Tuple[bool, bool]
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}

_GEO_COLLECTOR: Optional[GeoCollector] = None
_SCHOOL_DB = "data/master/school_info.db"
_FAILURES_DB = "data/failures.db"
_LAST_ERROR_MSG: Optional[str] = None

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"


def kst_naive(dt: datetime) -> datetime:
    if getattr(dt, "tzinfo", None) is not None:
        return dt.replace(tzinfo=None)
    return dt


def get_today_3pm_kst_naive(now: datetime) -> datetime:
    n = kst_naive(now)
    return datetime.combine(n.date(), dt_time(15, 0))


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
    if not gc.vworld_key:
        logger.error("VWORLD_API_KEY not set")
        return None, 0
    try:
        coords = gc._geocode_with_type(address, addr_type)
        if coords:
            return coords, 200
        return None, 404
    except requests.exceptions.HTTPError as e:
        if hasattr(e, 'response') and e.response is not None:
            return None, e.response.status_code
        return None, 500
    except requests.exceptions.RequestException as e:
        logger.error(f"VWorld {addr_type} request error: {e}")
        return None, 500
    except Exception as e:
        logger.error(f"VWorld {addr_type} unexpected error: {e}")
        return None, 500


def geocode_kakao(address: str) -> Tuple[Optional[Tuple[float, float]], Optional[int], Optional[str]]:
    KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")
    if not KAKAO_API_KEY:
        return None, None, None
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address, "analyze_type": "exact"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('documents'):
                doc = data['documents'][0]
                x = float(doc['x'])
                y = float(doc['y'])
                official = doc.get('road_address', {}).get('address_name') or doc.get('address', {}).get('address_name')
                return (x, y), 200, official
        return None, resp.status_code, None
    except Exception as e:
        logger.error(f"Kakao geocode error: {e}")
        return None, None, None


def update_school_coords(
    sc_code: str,
    lon: float,
    lat: float,
    cleaned: str,
    addr_components: Dict[str, Any],
    kakao_address: Optional[str] = None
):
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
                    number_type = ?, number_value = ?, number_start = ?, number_end = ?, number_bit = ?,
                    kakao_address = COALESCE(?, kakao_address)
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
                    kakao_address,
                    sc_code,
                ),
            )
        except sqlite3.OperationalError as e:
            logger.warning(f"kakao_address 컬럼 없음, 기본 업데이트: {e}")
            conn.execute(
                """
                UPDATE schools
                SET longitude = ?, latitude = ?, cleaned_address = ?
                WHERE sc_code = ?
                """,
                (lon, lat, cleaned, sc_code),
            )


def log_address_mapping(original: str, mapped: str, sc_code: str, source: str):
    if original == mapped:
        return
    mapping_data = {
        "sc_code": sc_code,
        "original": original,
        "mapped": mapped,
        "source": source,
        "timestamp": now_kst().isoformat()
    }
    address_mapping_logger.info(json.dumps(mapping_data, ensure_ascii=False))


def get_consecutive_404(sc_code: str) -> int:
    try:
        with sqlite3.connect(_FAILURES_DB) as conn:
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM failures 
                WHERE sc_code=? 
                AND (error_msg LIKE '%404%' OR error_msg LIKE '%NOT_FOUND%')
                AND status='FAILED' AND resolved_at IS NULL
                """,
                (sc_code,)
            )
            return cur.fetchone()[0] or 0
    except Exception:
        return 0


def handle_school_geocode(failure: dict) -> HandlerResult:
    sc_code = failure.get("sc_code")
    original_address = failure.get("address")
    retries = failure.get("retries") or 0

    if not sc_code or not original_address:
        return (False, True)

    level = min(max(int(retries), 1), 3)
    cleaned = AddressFilter.clean(original_address, level=level)

    gc = get_geo_collector()

    # jibun_address 조회
    jibun = None
    with sqlite3.connect(_SCHOOL_DB) as conn:
        cur = conn.execute("SELECT jibun_address FROM schools WHERE sc_code=?", (sc_code,))
        row = cur.fetchone()
        if row:
            jibun = row[0]

    final_status = 0
    error_messages = []

    # 0차: 지번 주소로 PARCEL 우선 시도 (jibun이 있을 경우)
    if jibun:
        coords, status = _geocode_vworld(gc, jibun, "PARCEL")
        if coords:
            lon, lat = coords
            addr_components = gc.meta_vocab.save_address(jibun)
            update_school_coords(sc_code, lon, lat, jibun, addr_components)
            log_address_mapping(original_address, jibun, sc_code, "vworld_jibun")
            logger.info(f"✅ 지번 우선 성공: {sc_code} - {jibun[:50]}...")
            return (True, False)
        if status:
            final_status = status
            error_messages.append(f"JIBUN_PARCEL:{status}")

    # 1차: VWorld road
    coords, status = _geocode_vworld(gc, cleaned, "ROAD")
    if coords:
        lon, lat = coords
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components)
        log_address_mapping(original_address, cleaned, sc_code, "vworld")
        return (True, False)
    if status:
        final_status = status
        error_messages.append(f"ROAD:{status}")

    # 2차: VWorld parcel
    coords, status = _geocode_vworld(gc, cleaned, "PARCEL")
    if coords:
        lon, lat = coords
        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components)
        log_address_mapping(original_address, cleaned, sc_code, "vworld")
        return (True, False)
    if status:
        final_status = status
        error_messages.append(f"PARCEL:{status}")

    simplified = re.sub(r'\s*[-,.+()].*$', '', cleaned).strip()

    # 3차: simplified road
    if simplified != cleaned:
        coords, status = _geocode_vworld(gc, simplified, "ROAD")
        if coords:
            lon, lat = coords
            addr_components = gc.meta_vocab.save_address(simplified)
            update_school_coords(sc_code, lon, lat, simplified, addr_components)
            log_address_mapping(original_address, simplified, sc_code, "vworld")
            return (True, False)
        if status:
            final_status = status
            error_messages.append(f"SIMPLIFIED_ROAD:{status}")

    # 4차: simplified parcel
    if simplified != cleaned:
        coords, status = _geocode_vworld(gc, simplified, "PARCEL")
        if coords:
            lon, lat = coords
            addr_components = gc.meta_vocab.save_address(simplified)
            update_school_coords(sc_code, lon, lat, simplified, addr_components)
            log_address_mapping(original_address, simplified, sc_code, "vworld")
            return (True, False)
        if status:
            final_status = status
            error_messages.append(f"SIMPLIFIED_PARCEL:{status}")

    # 5차: Kakao
    coords, status, official_address = geocode_kakao(cleaned)
    if coords:
        lon, lat = coords
        logger.info(f"Kakao API success for {cleaned[:30]}")

        if official_address and official_address != cleaned:
            log_address_mapping(original_address, official_address, sc_code, "kakao")
            cleaned = official_address

        addr_hash = gc._hash_address(cleaned)
        gc.cache[addr_hash] = (lon, lat)
        gc._save_to_cache(cleaned, lon, lat, "KAKAO")

        addr_components = gc.meta_vocab.save_address(cleaned)
        update_school_coords(sc_code, lon, lat, cleaned, addr_components, kakao_address=official_address)
        return (True, False)

    if status:
        final_status = status
        error_messages.append(f"KAKAO:{status}")

    consec_404 = get_consecutive_404(sc_code)
    err_info = classify_error(final_status, consec_404)
    full_error = f"[{','.join(error_messages)}] {err_info['message']}" if error_messages else err_info['message']

    if err_info['action'] == 'stop':
        logger.critical(f"Fatal auth error for {sc_code}: {err_info['message']}")
        global _LAST_ERROR_MSG
        _LAST_ERROR_MSG = full_error
        return (False, True)
    elif err_info['action'] == 'orphan':
        logger.warning(f"Orphan detected for {sc_code}: {full_error}")
        _LAST_ERROR_MSG = full_error
        return (False, True)
    else:
        logger.info(f"Transient failure for {sc_code}: {full_error}")
        _LAST_ERROR_MSG = full_error
        return (False, False)


register_handler("school", "geocode", handle_school_geocode)


def print_progress(current, total, success, retry, orphan, start_time):
    elapsed = time.time() - start_time
    avg = current / elapsed if elapsed > 0 else 0
    bar = f"[{'=' * (current * 50 // total):<50}] {current}/{total}"
    status = f"{GREEN}✅{RESET}{success:3d}  {YELLOW}⏳{RESET}{retry:3d}  {RED}❌{RESET}{orphan:3d}"
    print(f"\r{bar}  {status}  {avg:.1f}개/초", end="", flush=True)


def print_summary(success, retry, orphan, start_time, remaining):
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print("📊 재시도 워커 실행 결과")
    print("=" * 70)
    print(f"✅ 성공: {success:,}개")
    print(f"⏳ 재시도 예약: {retry:,}개")
    print(f"❌ 포기 (ORPHAN): {orphan:,}개")
    print("-" * 70)
    print(f"⏱️  처리 시간: {elapsed:.2f}초")
    if elapsed > 0:
        print(f"⚡ 평균 속도: {success/elapsed:.1f}개/초")
    print(f"📌 남은 작업: {remaining:,}개 (status='FAILED')")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="재시도 워커")
    parser.add_argument("--limit", type=int, default=50, help="한 번에 처리할 작업 수")
    parser.add_argument("--force", action="store_true", help="next_attempt 무시")
    parser.add_argument("--dry-run", action="store_true", help="실제 API 호출 없이 로그만 출력")
    parser.add_argument("--menu", action="store_true", help="실행 후 메뉴 표시")
    args = parser.parse_args()

    now = kst_naive(now_kst())
    deadline = get_today_3pm_kst_naive(now)

    rm = RetryManager(
        db_path=_FAILURES_DB,
        max_retries=3,
        base_delay=60,
        backoff_factor=2,
        deadline_buffer_seconds=70
    )

    print(f"\n🚀 retry_worker 시작 (한국시간: {now})")
    print(f"   ├─ force={args.force}, limit={args.limit}, dry-run={args.dry_run}")
    print(f"   ├─ 데드라인: {deadline}")
    print("=" * 70)

    if args.force:
        failures = rm.get_all_pending_retries(limit=args.limit)
    else:
        failures = rm.get_pending_retries(limit=args.limit, deadline=deadline)

    if not failures:
        print("ℹ️  재시도할 작업 없음")
        return

    if args.dry_run:
        logger.info("🚧 DRY RUN MODE - API 호출 없음")
        for i, f in enumerate(failures, 1):
            sc_code = f["sc_code"]
            jibun = None
            with sqlite3.connect(_SCHOOL_DB) as conn:
                cur = conn.execute("SELECT jibun_address FROM schools WHERE sc_code=?", (sc_code,))
                row = cur.fetchone()
                if row:
                    jibun = row[0]
            if jibun:
                logger.info(f"[DRY RUN] {sc_code} has jibun: {jibun[:50]}...")
            else:
                logger.info(f"[DRY RUN] {sc_code} has no jibun")
        print_summary(0, 0, len(failures), time.time(), 0)
        return

    print(f"📋 총 {len(failures)}개 작업 재시도\n")

    success_count = 0
    orphan_count = 0
    retry_count = 0
    start_time = time.time()
    last_update = start_time

    for i, f in enumerate(failures, 1):
        domain = f["domain"]
        task_type = f["task_type"]
        failure_id = f["id"]

        global _LAST_ERROR_MSG
        _LAST_ERROR_MSG = None

        handler = TASK_HANDLERS.get((domain, task_type))
        if not handler:
            msg = f"handler not found: {domain}/{task_type}"
            logger.warning(f"{msg} id={failure_id}")
            rm.mark_orphan(failure_id, error=msg)
            orphan_count += 1
            if time.time() - last_update >= 0.2:
                print_progress(i, len(failures), success_count, retry_count, orphan_count, start_time)
                last_update = time.time()
            continue

        try:
            success, is_permanent = handler(f)
        except Exception as e:
            logger.error(f"handler exception: {e}", exc_info=True)
            success, is_permanent = False, False

        if (not success) and is_permanent:
            rm.mark_orphan(failure_id, error=f"permanent failure: {domain}/{task_type}")
            orphan_count += 1
        elif success:
            rm.mark_resolved(failure_id, status="SUCCESS")
            success_count += 1
        else:
            still_alive = rm.schedule_retry_by_id(
                failure_id=failure_id,
                error=_LAST_ERROR_MSG or "재시도 실패",
                deadline=deadline,
            )
            if still_alive:
                retry_count += 1
            else:
                orphan_count += 1

        if time.time() - last_update >= 0.2:
            print_progress(i, len(failures), success_count, retry_count, orphan_count, start_time)
            last_update = time.time()

    print_progress(len(failures), len(failures), success_count, retry_count, orphan_count, start_time)
    print()

    remaining = len(rm.get_all_pending_retries(limit=10000)) if args.force else len(rm.get_pending_retries(limit=10000, deadline=deadline))
    print_summary(success_count, retry_count, orphan_count, start_time, remaining)

    if args.menu:
        # 간단한 메뉴 구현 생략
        pass


if __name__ == "__main__":
    try:
        main()
    finally:
        if _GEO_COLLECTOR is not None:
            _GEO_COLLECTOR.flush()
            _GEO_COLLECTOR.meta_vocab.flush()
            