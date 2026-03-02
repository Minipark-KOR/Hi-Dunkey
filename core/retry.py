# core/retry.py
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
from pathlib import Path

from core.kst_time import now_kst

logger = logging.getLogger(__name__)


class RetryManager:
    def __init__(
        self,
        db_path: str = "data/failures.db",
        max_retries: Optional[int] = None,
        base_delay: int = 60,
        backoff_factor: int = 2,
        deadline_buffer_seconds: int = 70,  # ✅ cron 매분 대응 (60초 이상)
    ):
        self.db_path = db_path
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.deadline_buffer = timedelta(seconds=deadline_buffer_seconds)

        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _now(self) -> datetime:
        """KST now. SQLite 비교를 위해 naive로 통일."""
        dt = now_kst()
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    shard TEXT,
                    sc_code TEXT,
                    region TEXT,
                    year INTEGER,
                    month INTEGER,
                    day INTEGER,
                    semester INTEGER,
                    address TEXT,
                    sub_key TEXT,
                    error_msg TEXT,
                    retries INTEGER DEFAULT 0,
                    next_attempt TIMESTAMP,
                    status TEXT DEFAULT 'FAILED',
                    resolved_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_next_attempt_pending
                ON failures(next_attempt)
                WHERE status='FAILED' AND resolved_at IS NULL
                """
            )

    def get_pending_retries(
        self,
        limit: int = 50,
        deadline: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        now = self._now()
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            query = """
                SELECT * FROM failures
                WHERE status = 'FAILED'
                  AND resolved_at IS NULL
                  AND next_attempt IS NOT NULL
                  AND next_attempt <= ?
            """
            params: list[Any] = [now]
            if deadline is not None:
                query += " AND next_attempt <= ?"
                params.append(deadline)
            query += " ORDER BY next_attempt ASC LIMIT ?"
            params.append(limit)
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def mark_resolved(self, failure_id: int, status: str = "SUCCESS", error_msg: Optional[str] = None):
        resolved_at = self._now()
        with self._get_connection() as conn:
            if error_msg is not None:
                conn.execute(
                    """
                    UPDATE failures
                    SET status = ?, resolved_at = ?, error_msg = ?
                    WHERE id = ?
                    """,
                    (status, resolved_at, error_msg, failure_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE failures
                    SET status = ?, resolved_at = ?
                    WHERE id = ?
                    """,
                    (status, resolved_at, failure_id),
                )

    def mark_orphan(self, failure_id: int, error: str):
        self.mark_resolved(failure_id, status="ORPHAN", error_msg=error)

    def mark_expired(self, failure_id: int, reason: str = "데드라인 초과"):
        resolved_at = self._now()
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE failures
                SET status = 'EXPIRED', resolved_at = ?, error_msg = ?
                WHERE id = ? AND status = 'FAILED' AND resolved_at IS NULL
                """,
                (resolved_at, reason, failure_id),
            )

    def _mark_expired_on_conn(self, conn: sqlite3.Connection, failure_id: int, reason: str):
        """✅ 동일 커넥션에서 만료 처리"""
        resolved_at = self._now()
        conn.execute(
            """
            UPDATE failures
            SET status = 'EXPIRED', resolved_at = ?, error_msg = ?
            WHERE id = ? AND status = 'FAILED' AND resolved_at IS NULL
            """,
            (resolved_at, reason, failure_id),
        )

    def _compute_next_attempt(
        self,
        now: datetime,
        retries: int,
        deadline: Optional[datetime],
    ) -> Optional[datetime]:
        if deadline is not None and now >= deadline:
            return None
        if self.max_retries is not None and retries > self.max_retries:
            return None

        delay_seconds = self.base_delay * (self.backoff_factor ** (retries - 1))
        next_attempt = now + timedelta(seconds=delay_seconds)

        if deadline is not None:
            # ✅ cron 매분 대비: 최소 1분 전 + 분 단위 정렬
            buffer = max(self.deadline_buffer, timedelta(minutes=1))
            last_chance = (deadline - buffer).replace(second=0, microsecond=0)

            if next_attempt > deadline:
                if now < last_chance:
                    next_attempt = last_chance
                else:
                    return None

        return next_attempt

    def schedule_retry_by_id(
        self,
        failure_id: int,
        error: str,
        deadline: Optional[datetime] = None,
    ) -> bool:
        now = self._now()
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, retries, status, resolved_at FROM failures WHERE id = ?",
                (failure_id,),
            ).fetchone()

            if not row:
                logger.warning("schedule_retry_by_id: id=%s not found", failure_id)
                return False
            if row["status"] != "FAILED" or row["resolved_at"] is not None:
                return False

            current_retries = int(row["retries"] or 0)
            new_retries = current_retries + 1

            next_attempt = self._compute_next_attempt(now=now, retries=new_retries, deadline=deadline)
            if next_attempt is None:
                self._mark_expired_on_conn(conn, failure_id, reason="데드라인 도달 또는 최대 재시도 초과")
                return False

            cur = conn.execute(
                """
                UPDATE failures
                SET retries = ?, next_attempt = ?, error_msg = ?, status = 'FAILED', resolved_at = NULL
                WHERE id = ? AND status = 'FAILED' AND resolved_at IS NULL
                """,
                (new_retries, next_attempt, error, failure_id),
            )
            return cur.rowcount == 1

    def record_failure(
        self,
        domain: str,
        task_type: str,
        deadline: Optional[datetime] = None,
        shard=None,
        sc_code=None,
        region=None,
        year=None,
        month=None,
        day=None,
        semester=None,
        address=None,
        sub_key=None,
        error: str = "",
    ) -> bool:
        now = self._now()
        next_attempt = self._compute_next_attempt(now=now, retries=1, deadline=deadline)
        if next_attempt is None:
            logger.warning("record_failure: 데드라인 이후이거나 정책상 예약 불가")
            return False

        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT id, retries FROM failures
                WHERE domain = ? AND task_type = ?
                  AND shard IS ? AND sc_code IS ? AND region IS ?
                  AND year IS ? AND month IS ? AND day IS ? AND semester IS ?
                  AND address IS ? AND sub_key IS ?
                  AND status = 'FAILED' AND resolved_at IS NULL
                ORDER BY id DESC LIMIT 1
                """,
                (domain, task_type, shard, sc_code, region, year, month, day, semester, address, sub_key),
            ).fetchone()

            if row:
                failure_id = row["id"]
                current_retries = int(row["retries"] or 0)
                new_retries = current_retries + 1

                next_attempt2 = self._compute_next_attempt(now=now, retries=new_retries, deadline=deadline)
                if next_attempt2 is None:
                    self._mark_expired_on_conn(conn, failure_id, reason="데드라인 도달 또는 최대 재시도 초과")
                    return False

                conn.execute(
                    """
                    UPDATE failures
                    SET retries = ?, next_attempt = ?, error_msg = ?, status = 'FAILED', resolved_at = NULL
                    WHERE id = ? AND status = 'FAILED' AND resolved_at IS NULL
                    """,
                    (new_retries, next_attempt2, error, failure_id),
                )
                return True

            conn.execute(
                """
                INSERT INTO failures
                (domain, task_type, shard, sc_code, region, year, month, day, semester, address, sub_key,
                 retries, next_attempt, error_msg, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'FAILED')
                """,
                (
                    domain, task_type, shard, sc_code, region, year, month, day, semester, address, sub_key,
                    1, next_attempt, error,
                ),
            )
            return True
            