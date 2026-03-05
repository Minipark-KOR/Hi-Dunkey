#!/usr/bin/env python3
# scripts/retry_worker.py
import os
import sys
import argparse
import sqlite3
import re
import json
import time
import subprocess
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

# 에러 메시지 저장용 전역 변수
_LAST_ERROR_MSG: Optional[str] = None

GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


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
    """Kakao API 지오코딩 (폴백용)"""
    kakao_key = os.getenv("KAKAO_API_KEY", "").strip()
    if not kakao_key:
        try:
            from constants.codes import KAKAO_API_KEY
            kakao_key = (KAKAO_API_KEY or "").strip()
        except:
            pass
    if not kakao_key:
        return None, None, None

    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {kakao_key}"}
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
            else:
                # documents가 없으면 404로 처리 (좌표 없음)
                return None, 404, None
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
    global _LAST_ERROR_MSG
    _LAST_ERROR_MSG = None

    sc_code = failure.get("sc_code")
    original_address = failure.get("address")
    retries = failure.get("retries") or 0

    if not sc_code or not original_address:
        return (False, True)

    level = min(max(int(retries), 1), 3)
    cleaned = AddressFilter.clean(original_address, level=level)

    gc = get_geo_collector()
    final_status = 0
    error_messages = []

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

    # 5차: Kakao API (최종 폴백)
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
    else:
        error_messages.append("KAKAO:NOT_CALLED")

    consec_404 = get_consecutive_404(sc_code)
    err_info = classify_error(final_status, consec_404)
    full_error = f"[{','.join(error_messages)}] {err_info['message']}" if error_messages else err_info['message']

    if err_info['action'] == 'stop':
        logger.critical(f"Fatal auth error for {sc_code}: {err_info['message']}")
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


# ========================================================
# 메뉴 기능
# ========================================================

def check_missing_count(school_db: str):
    if not os.path.exists(school_db):
        print("❌ DB 파일 없음")
        return
    with sqlite3.connect(school_db) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM schools WHERE latitude IS NULL OR longitude IS NULL")
        missing = cur.fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
        print(f"\n📍 지오코딩 상태: {missing}/{total} ({missing/total*100:.1f}% 누락)")


def check_failures_queue(failures_db: str):
    if not os.path.exists(failures_db):
        print("❌ DB 파일 없음")
        return
    with sqlite3.connect(failures_db) as conn:
        cur = conn.execute("SELECT status, COUNT(*) FROM failures GROUP BY status")
        rows = cur.fetchall()
        print("\n📊 failures 큐 상태")
        for s, c in rows:
            print(f"  - {s}: {c}개")


def check_db_size(school_db: str, failures_db: str):
    print("\n💾 DB 파일 크기:")
    for label, path in [("학교 DB", school_db), ("Failures DB", failures_db)]:
        if os.path.exists(path):
            size = os.path.getsize(path) / (1024*1024)
            print(f"  {label}: {size:.2f} MB")
        else:
            print(f"  {label}: 없음")


def run_retry_worker(force: bool = False, limit: int = 100):
    """retry_worker를 다시 실행 (force 옵션 및 limit 지정 가능)"""
    mode = "force" if force else "normal"
    print(f"\n🚀 retry_worker 다시 실행 중... (mode: {mode}, limit: {limit})")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cmd = [sys.executable, __file__, "--limit", str(limit), "--no-menu"]  # 메뉴 없이 실행
    if force:
        cmd.append("--force")
    subprocess.run(
        cmd,
        cwd=script_dir,
        env={**os.environ, "PYTHONPATH": os.path.dirname(script_dir)}
    )


def list_failed_schools(school_db: str, failures_db: str):
    """실패한 학교 목록 출력 (학교명, 도로명 주소, 지번 주소)"""
    print("\n📋 실패한 학교 목록 (status='FAILED')")
    print("=" * 90)
    try:
        with sqlite3.connect(school_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("""
                SELECT s.sc_code, s.sc_name, s.address, s.jibun_address,
                       f.error_msg, f.retries
                FROM schools s
                JOIN failures f ON s.sc_code = f.sc_code
                WHERE f.status = 'FAILED'
                ORDER BY s.sc_name
                LIMIT 50
            """)
            rows = cur.fetchall()
            if not rows:
                print("ℹ️  현재 FAILED 상태인 학교가 없습니다.")
                return

            print(f"총 {len(rows)}개 (최대 50개 표시)")
            print("-" * 90)
            for row in rows:
                print(f"학교명: {row['sc_name']}")
                print(f"주소: {row['address']}")
                print(f"지번: {row['jibun_address'] or '없음'}")
                print(f"에러: {row['error_msg'] or '없음'}")
                print(f"재시도: {row['retries']}")
                print("-" * 90)
    except Exception as e:
        print(f"❌ 조회 실패: {e}")


def reset_orphan_and_cleanse(failures_db: str, school_db: str):
    """ORPHAN 상태를 FAILED로 초기화하고 cleanse_failures 실행"""
    print("\n🔄 ORPHAN → FAILED 초기화 중...")
    try:
        with sqlite3.connect(failures_db) as conn:
            cur = conn.execute("UPDATE failures SET status='FAILED', retries=0, resolved_at=NULL WHERE status='ORPHAN';")
            conn.commit()
            print(f"✅ {cur.rowcount}개 레코드가 FAILED로 변경되었습니다.")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    print("\n🚀 cleanse_failures.py 실행 중...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        [sys.executable, os.path.join(script_dir, "cleanse_failures.py")],
        cwd=os.path.dirname(script_dir),
        env={**os.environ, "PYTHONPATH": os.path.dirname(script_dir)}
    )
    if result.returncode == 0:
        print("✅ cleanse_failures 완료")
    else:
        print(f"⚠️ cleanse_failures 종료 코드: {result.returncode}")


def show_menu(rm: RetryManager, school_db: str, failures_db: str):
    while True:
        print("\n" + "=" * 70)
        print("📋 추가 작업 메뉴")
        print("=" * 70)
        print("  1. 누락 학교 개수 확인")
        print("  2. failures 큐 상태 확인")
        print("  3. DB 파일 크기 확인")
        print("  4. 실패한 학교 목록 확인")
        print("  5. ORPHAN 초기화 및 지번 정제 실행")
        print("  6. retry_worker limit mode (작업 수 입력, force 실행)")
        print("  0. 종료")
        print("=" * 70)

        choice = input("번호를 선택하세요 (0-6): ").strip()

        if choice == '1':
            check_missing_count(school_db)
        elif choice == '2':
            check_failures_queue(failures_db)
        elif choice == '3':
            check_db_size(school_db, failures_db)
        elif choice == '4':
            list_failed_schools(school_db, failures_db)
        elif choice == '5':
            reset_orphan_and_cleanse(failures_db, school_db)
        elif choice == '6':
            # limit 입력 받기
            limit_input = input("처리할 작업 수를 입력하세요 (기본: 100): ").strip()
            if limit_input == "":
                limit = 100
            else:
                try:
                    limit = int(limit_input)
                    if limit <= 0:
                        print("⚠️  1 이상의 숫자를 입력하세요. 기본값 100을 사용합니다.")
                        limit = 100
                except ValueError:
                    print("⚠️  숫자를 입력하세요. 기본값 100을 사용합니다.")
                    limit = 100
            run_retry_worker(force=True, limit=limit)
        elif choice == '0':
            print("👋 종료합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다.")

# ========================================================
# 메인
# ========================================================

def main():
    global _LAST_ERROR_MSG

    parser = argparse.ArgumentParser(description="재시도 워커")
    parser.add_argument("--limit", type=int, default=50, help="한 번에 처리할 작업 수")
    parser.add_argument("--force", action="store_true", help="next_attempt 무시")
    parser.add_argument("--menu", action="store_true", default=True, help="실행 후 메뉴 표시 (기본값)")
    parser.add_argument("--no-menu", action="store_false", dest="menu", help="메뉴 없이 종료")
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
    print(f"   ├─ force={args.force}, limit={args.limit}")
    print(f"   ├─ 데드라인: {deadline}")
    print("=" * 70)

    if args.force:
        failures = rm.get_all_pending_retries(limit=args.limit)
    else:
        failures = rm.get_pending_retries(limit=args.limit, deadline=deadline)

    if not failures:
        print("ℹ️  재시도할 작업 없음")
        if args.menu:
            show_menu(rm, _SCHOOL_DB, _FAILURES_DB)
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
        show_menu(rm, _SCHOOL_DB, _FAILURES_DB)


if __name__ == "__main__":
    try:
        main()
    finally:
        if _GEO_COLLECTOR is not None:
            _GEO_COLLECTOR.flush()
            _GEO_COLLECTOR.meta_vocab.flush()
