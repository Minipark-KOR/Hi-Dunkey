#!/usr/bin/env python3
# scripts/retry_worker.py
# 개발 가이드: docs/developer_guide.md 참조
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
from core.geo import VWorldGeocoder
from collectors.geo_collector import GeoCollector
from constants.paths import GLOBAL_VOCAB_DB_PATH, NEIS_INFO_DB_PATH, SCHOOL_INFO_DB_PATH, FAILURES_DB_PATH, LOG_DIR

logger = build_logger("retry_worker", str(LOG_DIR / "retry_worker.log"))
address_mapping_logger = build_logger("address_mapping", str(LOG_DIR / "address_mapping.log"))

HandlerResult = Tuple[bool, bool]
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}

_GEO_COLLECTOR: Optional[GeoCollector] = None
_VWORLD_GEOCODER: Optional[VWorldGeocoder] = None
global_db_path=str(GLOBAL_VOCAB_DB_PATH),
_NEIS_INFO_DB = str(NEIS_INFO_DB_PATH)
_SCHOOLINFO_DB = str(SCHOOL_INFO_DB_PATH)
_FAILURES_DB = str(FAILURES_DB_PATH)

# 에러 메시지 저장용
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
            neis_info_db_path=_NEIS_INFO_DB,
            failures_db_path=_FAILURES_DB,
            debug_mode=False,
        )
    return _GEO_COLLECTOR


def get_vworld_geocoder() -> VWorldGeocoder:
    global _VWORLD_GEOCODER
    if _VWORLD_GEOCODER is None:
        _VWORLD_GEOCODER = VWorldGeocoder(calls_per_second=3.0)
    return _VWORLD_GEOCODER


def geocode_vworld(address: str, addr_type: str = "road") -> Optional[Tuple[float, float]]:
    """VWorld 지오코딩"""
    gc = get_vworld_geocoder()
    return gc.geocode(address, addr_type)


def reverse_geocode(lat: float, lon: float) -> Optional[str]:
    """VWorld 역지오코딩"""
    gc = get_vworld_geocoder()
    return gc.reverse_geocode(lat, lon)


def get_neis_school_info(sc_code: str) -> Optional[Dict]:
    """나이스 DB에서 학교 정보 조회 (도로명/지번 주소, 좌표)"""
    if not os.path.exists(_NEIS_INFO_DB):
        return None
    try:
        conn = sqlite3.connect(_NEIS_INFO_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT address, cleaned_address, jibun_address, latitude, longitude
            FROM schools
            WHERE sc_code = ? AND status = '운영'
        """, (sc_code,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                "road_addr": row["address"] or row["cleaned_address"],
                "jibun_addr": row["jibun_address"],
                "lat": row["latitude"],
                "lon": row["longitude"],
            }
    except Exception as e:
        logger.warning(f"나이스 DB 조회 실패: {e}")
    return None


def get_schoolinfo_coords(sc_code: str) -> Optional[Tuple[float, float]]:
    """학교알리미 DB에서 좌표 조회"""
    if not os.path.exists(_SCHOOLINFO_DB):
        return None
    try:
        conn = sqlite3.connect(_SCHOOLINFO_DB)
        cur = conn.execute(
            "SELECT latitude, longitude FROM schools WHERE school_code = ?",
            (sc_code,)
        )
        row = cur.fetchone()
        conn.close()
        if row and row[0] is not None and row[1] is not None:
            return (row[0], row[1])
    except Exception as e:
        logger.warning(f"학교알리미 좌표 조회 실패: {e}")
    return None


def update_school_coords(sc_code: str, coords: Tuple[float, float], address: str, source: str):
    """neis_info.db에 좌표 및 주소 업데이트"""
    lon, lat = coords  # VWorld는 (lon, lat) 순서
    with sqlite3.connect(_NEIS_INFO_DB) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            conn.execute("""
                UPDATE schools
                SET longitude = ?, latitude = ?, cleaned_address = ?,
                    geocode_attempts = 0, last_error = NULL
                WHERE sc_code = ?
            """, (lon, lat, address, sc_code))
            conn.commit()
            logger.info(f"✅ [{source}] {sc_code} 좌표 업데이트 완료")
        except sqlite3.Error as e:
            logger.error(f"좌표 업데이트 실패 {sc_code}: {e}")


def record_step_log(sc_code: str, step: str, address: str, status: str, reason: str = ""):
    """각 단계의 성공/실패를 로그 테이블에 기록"""
    conn = sqlite3.connect(_FAILURES_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geo_step_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sc_code TEXT,
            step TEXT,
            attempted_at TEXT,
            address TEXT,
            status TEXT,
            reason TEXT
        )
    """)
    conn.execute("""
        INSERT INTO geo_step_log (sc_code, step, attempted_at, address, status, reason)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (sc_code, step, now_kst().isoformat(), address, status, reason))
    conn.commit()
    conn.close()


def handle_school_geocode(failure: dict) -> HandlerResult:
    global _LAST_ERROR_MSG
    _LAST_ERROR_MSG = None

    sc_code = failure.get("sc_code")
    original_address = failure.get("address")  # 학교알리미 주소
    if not sc_code:
        return (False, True)

    # 1. 나이스 정보 조회
    neis_info = get_neis_school_info(sc_code)

    # 2. 학교알리미 좌표 조회
    schoolinfo_coords = get_schoolinfo_coords(sc_code)

    # 3. 단계별 지오코딩 시도
    # 단계 1: 나이스 도로명 → VWorld
    if neis_info and neis_info.get("road_addr"):
        coords = geocode_vworld(neis_info["road_addr"], "road")
        if coords:
            update_school_coords(sc_code, coords, neis_info["road_addr"], "neis_road")
            record_step_log(sc_code, "neis_road", neis_info["road_addr"], "success")
            return (True, False)
        else:
            record_step_log(sc_code, "neis_road", neis_info["road_addr"], "failure", "VWorld geocode failed")

    # 단계 2: 나이스 지번 → VWorld
    if neis_info and neis_info.get("jibun_addr"):
        coords = geocode_vworld(neis_info["jibun_addr"], "parcel")
        if coords:
            update_school_coords(sc_code, coords, neis_info["jibun_addr"], "neis_jibun")
            record_step_log(sc_code, "neis_jibun", neis_info["jibun_addr"], "success")
            return (True, False)
        else:
            record_step_log(sc_code, "neis_jibun", neis_info["jibun_addr"], "failure", "VWorld geocode failed")

    # 단계 3: 학교알리미 도로명 → VWorld
    if original_address:
        coords = geocode_vworld(original_address, "road")
        if coords:
            update_school_coords(sc_code, coords, original_address, "schoolinfo_road")
            record_step_log(sc_code, "schoolinfo_road", original_address, "success")
            return (True, False)
        else:
            record_step_log(sc_code, "schoolinfo_road", original_address, "failure", "VWorld geocode failed")

    # 단계 4: 학교알리미 지번 → VWorld
    if original_address:
        jibun = AddressFilter.extract_jibun(original_address)
        if jibun:
            coords = geocode_vworld(jibun, "parcel")
            if coords:
                update_school_coords(sc_code, coords, jibun, "schoolinfo_jibun")
                record_step_log(sc_code, "schoolinfo_jibun", jibun, "success")
                return (True, False)
            else:
                record_step_log(sc_code, "schoolinfo_jibun", jibun, "failure", "VWorld geocode failed")

    # 단계 5: 학교알리미 좌표 → 역지오코딩 → VWorld 재검증
    if schoolinfo_coords:
        rev_addr = reverse_geocode(schoolinfo_coords[0], schoolinfo_coords[1])
        if rev_addr:
            coords2 = geocode_vworld(rev_addr, "road")
            if coords2:
                update_school_coords(sc_code, coords2, rev_addr, "schoolinfo_reverse")
                record_step_log(sc_code, "schoolinfo_reverse", rev_addr, "success")
                return (True, False)
            else:
                record_step_log(sc_code, "schoolinfo_reverse", rev_addr, "failure", "forward geocode failed after reverse")
        else:
            record_step_log(sc_code, "schoolinfo_reverse", f"{schoolinfo_coords[0]},{schoolinfo_coords[1]}", "failure", "reverse geocode failed")

    # 모든 단계 실패 → ORPHAN 처리
    record_step_log(sc_code, "final", original_address or "", "failure", "all attempts failed")
    _LAST_ERROR_MSG = "all geocoding attempts failed"
    return (False, True)  # is_permanent = True (ORPHAN)


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

    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/last_result.txt", "w", encoding="utf-8") as f:
            f.write(f"성공: {success}\n")
            f.write(f"재시도: {retry}\n")
            f.write(f"포기: {orphan}\n")
            f.write(f"처리 시간: {elapsed:.2f}초\n")
            f.write(f"평균 속도: {success/elapsed:.1f}개/초\n")
            f.write(f"남은 작업: {remaining}\n")
    except Exception as e:
        logger.error(f"결과 저장 실패: {e}")


# 메뉴 함수들 (기존 코드와 동일, 생략 가능)
def check_missing_count(neis_info_db: str):
    if not os.path.exists(neis_info_db):
        print("❌ DB 파일 없음")
        return
    with sqlite3.connect(neis_info_db) as conn:
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
        print("\n📊 failures 큐 상태 (전체 누적)")
        for s, c in rows:
            print(f"  - {s}: {c}개")


def check_db_size(neis_info_db: str, failures_db: str):
    print("\n💾 DB 파일 크기:")
    for label, path in [("학교 DB", neis_info_db), ("Failures DB", failures_db)]:
        if os.path.exists(path):
            size = os.path.getsize(path) / (1024*1024)
            print(f"  {label}: {size:.2f} MB")
        else:
            print(f"  {label}: 없음")


def list_failed_schools(neis_info_db: str, failures_db: str):
    print("\n📋 실패한 학교 목록 (status='FAILED')")
    print("=" * 90)
    try:
        with sqlite3.connect(failures_db) as conn_f:
            conn_f.row_factory = sqlite3.Row
            cur_f = conn_f.execute("SELECT sc_code, error_msg, retries FROM failures WHERE status='FAILED'")
            failed_rows = {row['sc_code']: {'error_msg': row['error_msg'], 'retries': row['retries']} for row in cur_f.fetchall()}

        if not failed_rows:
            print("ℹ️  현재 FAILED 상태인 학교가 없습니다.")
            return

        with sqlite3.connect(neis_info_db) as conn_s:
            conn_s.row_factory = sqlite3.Row
            placeholders = ','.join(['?'] * len(failed_rows))
            cur_s = conn_s.execute(f"""
                SELECT sc_code, sc_name, address, jibun_address
                FROM schools
                WHERE sc_code IN ({placeholders})
                ORDER BY sc_name
                LIMIT 50
            """, list(failed_rows.keys()))
            rows = cur_s.fetchall()

        print(f"총 {len(rows)}개 (최대 50개 표시)")
        print("-" * 90)
        for row in rows:
            sc_code = row['sc_code']
            info = failed_rows.get(sc_code, {})
            print(f"학교명: {row['sc_name']} (코드: {sc_code})")
            print(f"도로명: {row['address']}")
            print(f"지번: {row['jibun_address'] or '없음'}")
            print(f"에러: {info.get('error_msg', '없음')}")
            print(f"재시도: {info.get('retries', 0)}")
            print("-" * 90)
    except Exception as e:
        print(f"❌ 조회 실패: {e}")


def reset_orphan_only(failures_db: str):
    print("\n🔄 ORPHAN → FAILED 초기화 중...")
    try:
        with sqlite3.connect(failures_db) as conn:
            cur = conn.execute("UPDATE failures SET status='FAILED', retries=0, resolved_at=NULL WHERE status='ORPHAN';")
            conn.commit()
            print(f"✅ {cur.rowcount}개 레코드가 FAILED로 변경되었습니다.")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")


def reset_expired_and_orphan(failures_db: str):
    print("\n🔄 EXPIRED, ORPHAN → FAILED 초기화 중...")
    try:
        with sqlite3.connect(failures_db) as conn:
            cur = conn.execute("""
                UPDATE failures 
                SET status='FAILED', retries=0, resolved_at=NULL 
                WHERE status IN ('EXPIRED', 'ORPHAN')
            """)
            conn.commit()
            print(f"✅ {cur.rowcount}개 레코드가 FAILED로 변경되었습니다.")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")


def run_retry_worker(force: bool = False, limit: int = 100):
    mode = "force" if force else "normal"
    print(f"\n🚀 retry_worker 다시 실행 중... (mode: {mode}, limit: {limit})")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cmd = [sys.executable, __file__, "--limit", str(limit), "--no-menu"]
    if force:
        cmd.append("--force")
    subprocess.run(
        cmd,
        cwd=script_dir,
        env={**os.environ, "PYTHONPATH": os.path.dirname(script_dir)}
    )


def run_cleanse_failures():
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


def run_seed_failures():
    print("\n🚀 seed_failures.py 실행 중...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        [sys.executable, os.path.join(script_dir, "seed_failures.py"), "--menu"],
        cwd=os.path.dirname(script_dir),
        env={**os.environ, "PYTHONPATH": os.path.dirname(script_dir)}
    )
    if result.returncode == 0:
        print("✅ seed_failures 완료")
    else:
        print(f"⚠️ seed_failures 종료 코드: {result.returncode}")


def show_last_result():
    result_file = LOG_DIR / "last_result.txt"
    if not os.path.exists(result_file):
        print("ℹ️  이전 실행 결과가 없습니다.")
        return
    with open(result_file, "r", encoding="utf-8") as f:
        print("\n📊 [이번 실행 결과]")
        print(f.read())


def clear_logs():
    confirm = input("정말 모든 로그 파일을 삭제하시겠습니까? (y/N): ").strip().lower()
    if confirm == 'y':
        log_dir = "logs"
        deleted = 0
        for f in os.listdir(log_dir):
            if f.endswith(".log") or f == "last_result.txt":
                os.remove(os.path.join(log_dir, f))
                deleted += 1
        print(f"✅ {deleted}개 로그 파일이 삭제되었습니다.")
    else:
        print("취소되었습니다.")


def show_menu(rm: RetryManager, neis_info_db: str, failures_db: str):
    while True:
        print("\n" + "=" * 70)
        print("📋 추가 작업 메뉴")
        print("=" * 70)
        print("  1. 누락된 학교 수 확인")
        print("  2. failures 전체 상태 확인 (누적)")
        print("  3. 이번 실행 결과 보기")
        print("  4. DB 파일 크기 확인")
        print("  5. 실패한 학교 목록 확인 (학교명, 도로명, 지번)")
        print("  6. ORPHAN 초기화 (ORPHAN → FAILED)")
        print("  7. 주소 정제 실행 (cleanse_failures.py)")
        print("  8. 누락 학교 등록 (seed_failures.py)")
        print("  9. 데이터 수집 재시도 (limit 입력, force 실행)")
        print(" 10. 로그 초기화 (모든 로그 삭제)")
        print("  0. 종료")
        print("=" * 70)

        choice = input("번호를 선택하세요 (0-10): ").strip()

        if choice == '1':
            check_missing_count(neis_info_db)
        elif choice == '2':
            check_failures_queue(failures_db)
        elif choice == '3':
            show_last_result()
        elif choice == '4':
            check_db_size(neis_info_db, failures_db)
        elif choice == '5':
            list_failed_schools(neis_info_db, failures_db)
        elif choice == '6':
            reset_orphan_only(failures_db)
        elif choice == '7':
            run_cleanse_failures()
        elif choice == '8':
            run_seed_failures()
        elif choice == '9':
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
        elif choice == '10':
            clear_logs()
        elif choice == '0':
            print("👋 종료합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다.")


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
            show_menu(rm, _NEIS_INFO_DB, _FAILURES_DB)
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
        show_menu(rm, _NEIS_INFO_DB, _FAILURES_DB)


if __name__ == "__main__":
    try:
        main()
    finally:
        if _GEO_COLLECTOR is not None:
            _GEO_COLLECTOR.flush()
            _GEO_COLLECTOR.meta_vocab.flush()
            