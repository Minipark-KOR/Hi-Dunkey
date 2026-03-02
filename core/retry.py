#!/usr/bin/env python3
"""
실패한 작업 재시도 관리 (지수 백오프)
"""
import sqlite3
from datetime import timedelta
from typing import Optional, List, Dict, Any

from .database import get_db_connection
from .logger import build_logger
from .kst_time import now_kst
from .alert import send_alert

logger = build_logger("retry", "logs/retry.log")


class RetryManager:

    def __init__(self, db_path: str = "data/active/failures.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with get_db_connection(self.db_path, timeout=30) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS failures (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain            TEXT NOT NULL,
                    task_type         TEXT NOT NULL,
                    shard             TEXT NOT NULL DEFAULT '',
                    sc_code           TEXT NOT NULL DEFAULT '',
                    region            TEXT NOT NULL DEFAULT '',
                    year              INTEGER NOT NULL DEFAULT 0,
                    month             INTEGER NOT NULL DEFAULT 0,
                    day               INTEGER NOT NULL DEFAULT 0,
                    semester          INTEGER NOT NULL DEFAULT 0,
                    address           TEXT NOT NULL DEFAULT '',
                    sub_key           TEXT NOT NULL DEFAULT '',
                    failed_at         TEXT NOT NULL,
                    retries           INTEGER DEFAULT 0,
                    max_retries       INTEGER DEFAULT 5,
                    next_retry_at     TEXT,
                    resolved          INTEGER DEFAULT 0,
                    resolution_status TEXT DEFAULT NULL,
                    last_error        TEXT,
                    UNIQUE(domain, task_type, shard, sc_code, region,
                           year, month, day, semester, sub_key)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_next_retry "
                "ON failures(next_retry_at) WHERE resolved=0"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_domain ON failures(domain)"
            )

    def record_failure(self, domain: str, task_type: str, **kwargs) -> bool:
        """
        실패 기록 저장 또는 갱신.
        반환값: True = 재시도 예약됨, False = 최대 재시도 초과로 포기.
        resolved=1인 기존 레코드는 초기화하여 새 실패로 재처리.
        """
        shard       = kwargs.get('shard') or ''
        sc_code     = kwargs.get('sc_code') or ''
        region      = kwargs.get('region') or ''
        year        = int(kwargs.get('year') or 0)
        month       = int(kwargs.get('month') or 0)
        day         = int(kwargs.get('day') or 0)
        semester    = int(kwargs.get('semester') or 0)
        sub_key     = kwargs.get('sub_key') or ''
        address     = kwargs.get('address') or ''
        error       = kwargs.get('error', '')
        max_retries = int(kwargs.get('max_retries', 5))

        with get_db_connection(self.db_path, timeout=30) as conn:
            try:
                cur = conn.execute("""
                    SELECT id, retries, max_retries, resolved FROM failures
                    WHERE domain=? AND task_type=? AND shard=? AND sc_code=?
                      AND region=? AND year=? AND month=? AND day=?
                      AND semester=? AND sub_key=?
                """, (domain, task_type, shard, sc_code, region,
                      year, month, day, semester, sub_key))
                row = cur.fetchone()

                if row:
                    failure_id, retries, stored_max, resolved = row

                    # 포기된 작업이 다시 실패 → 초기화 후 새 시도로 처리
                    if resolved == 1:
                        next_time_str = (
                            now_kst() + timedelta(minutes=1)
                        ).isoformat()
                        conn.execute("""
                            UPDATE failures SET
                                retries=0, resolved=0, resolution_status=NULL,
                                failed_at=?, next_retry_at=?,
                                last_error=?, max_retries=?
                            WHERE id=?
                        """, (now_kst().isoformat(), next_time_str,
                              error, max_retries, failure_id))
                        logger.info(
                            f"포기된 작업 재활성화: {domain}/{task_type} id={failure_id}"
                        )
                        return True

                    retries += 1
                    if retries >= stored_max:
                        conn.execute("""
                            UPDATE failures
                            SET resolved=1, resolution_status='GIVE_UP',
                                last_error=?, retries=?
                            WHERE id=?
                        """, (error, retries, failure_id))
                        logger.warning(
                            f"최대 재시도 초과 → 포기: {domain}/{task_type} "
                            f"sc_code={sc_code}"
                        )
                        return False

                    minutes = min(2 ** retries, 24 * 60)
                    next_time_str = (
                        now_kst() + timedelta(minutes=minutes)
                    ).isoformat()
                    conn.execute("""
                        UPDATE failures
                        SET retries=?, next_retry_at=?, last_error=?, resolved=0
                        WHERE id=?
                    """, (retries, next_time_str, error, failure_id))

                else:
                    # 신규 실패 기록
                    next_time_str = (
                        now_kst() + timedelta(minutes=1)
                    ).isoformat()
                    conn.execute("""
                        INSERT INTO failures
                        (domain, task_type, shard, sc_code, region,
                         year, month, day, semester, address, sub_key,
                         failed_at, retries, max_retries, next_retry_at, last_error)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        domain, task_type, shard,
                        sc_code, region, year, month, day, semester,
                        address, sub_key,
                        now_kst().isoformat(),
                        0, max_retries, next_time_str, error
                    ))

                return True

            except Exception as e:
                logger.error(f"실패 기록 저장 오류: {e}", exc_info=True)
                return False

    def get_pending_retries(
        self,
        domain: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """재시도 시간이 지난 미해결 실패 목록 반환"""
        now_str = now_kst().isoformat()
        with get_db_connection(
            self.db_path, timeout=30, row_factory=sqlite3.Row
        ) as conn:
            query = """
                SELECT * FROM failures
                WHERE resolved=0 AND next_retry_at <= ?
            """
            params: list = [now_str]
            if domain:
                query += " AND domain=?"
                params.append(domain)
            query += " ORDER BY next_retry_at ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def mark_resolved(self, failure_id: int, status: str = 'SUCCESS'):
        """성공 처리"""
        with get_db_connection(self.db_path, timeout=30) as conn:
            conn.execute(
                "UPDATE failures SET resolved=1, resolution_status=? WHERE id=?",
                (status, failure_id)
            )

    def mark_orphan(self, failure_id: int, error: str = ""):
        """영구 실패 처리 (데이터 정합성 오류 등)"""
        with get_db_connection(self.db_path, timeout=30) as conn:
            conn.execute("""
                UPDATE failures
                SET resolved=1, resolution_status='ORPHAN', last_error=?
                WHERE id=?
            """, (error, failure_id))
        send_alert(
            f"데이터 정합성 오류 (failure_id={failure_id}): {error}",
            level="critical"
        )
