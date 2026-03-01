#!/usr/bin/env python3
"""
재시도 워커 - 실패한 작업을 지수 백오프로 재시도
크론탭 1분 간격 실행 권장
"""
import os
import sys
import sqlite3
from typing import Dict, Any, Callable, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from collectors.school_info_collector import SchoolInfoCollector

logger = build_logger("retry_worker", "logs/retry_worker.log")
rm = RetryManager()

HandlerResult = Tuple[bool, bool]
TASK_HANDLERS: Dict[tuple, Callable[[Dict[str, Any]], HandlerResult]] = {}


def register_handler(domain: str, task_type: str, handler: Callable):
    TASK_HANDLERS[(domain, task_type)] = handler


# ===== 지오코딩 재시도 핸들러 =====
def handle_geocode_retry(failure: Dict[str, Any]) -> HandlerResult:
    try:
        from core.geo import VWorldGeocoder

        sc_code = failure.get('sc_code', '')
        address = failure.get('address', '')
        shard   = failure.get('shard', '')

        if not sc_code or not address or not shard:
            logger.error(f"지오코딩 재시도: 필수 정보 부족 id={failure['id']}")
            return (True, True)

        geo = VWorldGeocoder(calls_per_second=3.0)
        coords = geo.geocode(address)
        if not coords:
            return (False, False)

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
            logger.error(f"데이터 정합성 오류: sc_code={sc_code}가 {db_path}에 없음")
            return (True, True)

        return (True, False)

    except Exception as e:
        logger.error(f"지오코딩 재시도 예외: {e}", exc_info=True)
        return (False, False)


# ===== 수집 재시도 핸들러 (신규) =====
def handle_collect_retry(failure: Dict[str, Any]) -> HandlerResult:
    """
    NEIS API 수집 실패 재시도
    - region(교육청 코드) 단위로 재수집
    """
    region = failure.get('region', '')
    year = failure.get('year', 2025)

    if not region:
        logger.error(f"수집 재시도: region 정보 없음 id={failure['id']}")
        return (True, True)

    logger.info(f"🔄 수집 재시도: region={region}, year={year}")

    try:
        # odd/even 샤드 모두 수집
        for shard in ['odd', 'even']:
            collector = SchoolInfoCollector(shard=shard)
            collector.fetch_region(region)
            collector.close()
        return (True, False)
    except Exception as e:
        logger.error(f"수집 재시도 실패: {e}", exc_info=True)
        return (False, False)


# 핸들러 등록
register_handler('geo', 'geocode', handle_geocode_retry)
register_handler('collect', 'fetch_region', handle_collect_retry)


def main():
    logger.info("🚀 재시도 워커 시작")
    failures = rm.get_pending_retries(limit=50)

    if not failures:
        logger.info("재시도할 작업 없음")
        return

    logger.info(f"총 {len(failures)}개 작업 재시도")

    for f in failures:
        domain     = f['domain']
        task_type  = f['task_type']
        failure_id = f['id']
        handler    = TASK_HANDLERS.get((domain, task_type))

        if not handler:
            logger.warning(f"처리기 없음: {domain}/{task_type} id={failure_id}")
            continue

        logger.info(f"재시도: {domain}/{task_type} id={failure_id} (retries={f['retries']})")
        success, is_permanent = handler(f)

        if success and is_permanent:
            rm.mark_orphan(failure_id, error=f"permanent fail: {domain}/{task_type}")
            logger.warning(f"🚫 영구 실패(orphan) id={failure_id}")

        elif success:
            rm.mark_resolved(failure_id, status='SUCCESS')
            logger.info(f"✅ 성공 id={failure_id}")

        else:
            still_alive = rm.record_failure(
                domain=domain,
                task_type=task_type,
                shard=f.get('shard'),
                sc_code=f.get('sc_code'),
                region=f.get('region'),
                year=f.get('year'),
                month=f.get('month'),
                day=f.get('day'),
                semester=f.get('semester'),
                address=f.get('address'),
                sub_key=f.get('sub_key'),
                error="재시도 실패"
            )
            if still_alive:
                logger.info(f"❌ 실패, 다음 재시도 예약됨 id={failure_id}")
            else:
                logger.warning(f"🚫 최대 재시도 초과, 포기 id={failure_id}")

    logger.info("✅ 재시도 워커 종료")


if __name__ == "__main__":
    main()
    