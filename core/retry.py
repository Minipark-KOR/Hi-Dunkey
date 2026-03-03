# core/retry.py
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging
from pathlib import Path
from contextlib import contextmanager
from core.kst_time import now_kst
from core.logger import build_logger

logger = build_logger("retry", "logs/retry.log")

class RetryManager:
    def __init__(
        self,
        db_path: str = "data/failures.db",
        max_retries: Optional[int] = None,
        base_delay: int = 60,
        backoff_factor: int = 2,
        deadline_buffer_seconds: int = 70,
    ):
        self.db_path = db_path
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.deadline_buffer = timedelta(seconds=deadline_buffer_seconds)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA journal_mode=WAL")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _now(self) -> datetime:
        dt = now_kst()
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    def _init_db(self) -> None:
        with self.get_connection() as conn:
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
        self, limit: int = 50, deadline: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        now = self._now()
        cutoff = now  # deadline 은 스케줄링 시에만 사용

        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT *
                FROM failures
                WHERE status='FAILED'
                AND resolved_at IS NULL
                AND next_attempt IS NOT NULL
                AND next_attempt <= ?
                ORDER BY next_attempt ASC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_resolved(
        self, failure_id: int, status: str = "SUCCESS", error_msg: Optional[str] = None
    ) -> None:
        resolved_at = self._now()
        with self.get_connection() as conn:
            if error_msg is not None:
                conn.execute(
                    "UPDATE failures SET status=?, resolved_at=?, error_msg=? WHERE id=?",
                    (status, resolved_at, error_msg, failure_id),
                )
            else:
                conn.execute(
                    "UPDATE failures SET status=?, resolved_at=? WHERE id=?",
                    (status, resolved_at, failure_id),
                )

    def mark_orphan(self, failure_id: int, error: str) -> None:
        self.mark_resolved(failure_id, status="ORPHAN", error_msg=error)

    def mark_expired(self, failure_id: int, reason: str) -> None:
        resolved_at = self._now()
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE failures
                SET status='EXPIRED', resolved_at=?, error_msg=?
                WHERE id=? AND status='FAILED' AND resolved_at IS NULL
                """,
                (resolved_at, reason, failure_id),
            )

    def _mark_expired_on_conn(
        self, conn: sqlite3.Connection, failure_id: int, reason: str
    ) -> None:
        resolved_at = self._now()
        conn.execute(
            """
            UPDATE failures
            SET status='EXPIRED', resolved_at=?, error_msg=?
            WHERE id=? AND status='FAILED' AND resolved_at IS NULL
            """,
            (resolved_at, reason, failure_id),
        )

    def _compute_next_attempt(
        self, now: datetime, retries: int, deadline: Optional[datetime]
    ) -> Optional[datetime]:
        if deadline is not None and now >= deadline:
            return None
        # ✅ 수정: retries > self.max_retries (3 회 재시도 허용)
        if self.max_retries is not None and retries > self.max_retries:
            return None

        delay_seconds = self.base_delay * (self.backoff_factor ** (retries - 1))
        next_attempt = now + timedelta(seconds=delay_seconds)

        if deadline is not None:
            buffer = max(self.deadline_buffer, timedelta(minutes=1))
            last_chance = (deadline - buffer).replace(second=0, microsecond=0)

            if next_attempt > deadline:
                if now < last_chance:
                    next_attempt = last_chance
                else:
                    return None

        return next_attempt

    def schedule_retry_by_id(
        self, failure_id: int, error: str, deadline: Optional[datetime] = None
    ) -> bool:
        now = self._now()
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT id, retries, status, resolved_at FROM failures WHERE id=?",
                (failure_id,),
            ).fetchone()

            if not row:
                logger.warning("schedule_retry_by_id id=%s not found", failure_id)
                return False

            if row["status"] != "FAILED" or row["resolved_at"] is not None:
                return False

            current_retries = int(row["retries"] or 0)
            new_retries = current_retries + 1
            next_attempt = self._compute_next_attempt(
                now=now, retries=new_retries, deadline=deadline
            )

            if next_attempt is None:
                self._mark_expired_on_conn(conn, failure_id, reason=error)
                return False

            cur = conn.execute(
                """
                UPDATE failures
                SET retries=?, next_attempt=?, error_msg=?, status='FAILED', resolved_at=NULL
                WHERE id=? AND status='FAILED' AND resolved_at IS NULL
                """,
                (new_retries, next_attempt, error, failure_id),
            )
            return cur.rowcount == 1

    # core/retry.py 에 새 메서드 추가 (record_failure 메서드 근처)

    def get_all_pending_retries(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        next_attempt 시간과 무관하게 모든 FAILED 레코드 조회
        수동 테스트용 (--force 옵션 사용 시)
        """
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT * FROM failures
                WHERE status='FAILED' AND resolved_at IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

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

        # 데드라인 이후 실패는 즉시 EXPIRED 로 기록
        if deadline is not None and now >= deadline:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO failures (
                        domain, task_type, shard, sc_code, region, year, month, day, semester,
                        address, sub_key, retries, next_attempt, error_msg, status, resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'EXPIRED', ?)
                    """,
                    (
                        domain, task_type, shard, sc_code, region, year, month, day, semester,
                        address, sub_key, 1, None, error, now,
                    ),
                )
            return True

        next_attempt = self._compute_next_attempt(now=now, retries=1, deadline=deadline)
        if next_attempt is None:
            logger.warning("record_failure next_attempt None (deadline too tight?)")
            return False

        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row

            # ✅ 수정: NULL 비교 패턴 (컬럼 IS NULL OR 컬럼=?)
            row = conn.execute(
                """
                SELECT id, retries
                FROM failures
                WHERE domain=? AND task_type=?
                AND (shard IS NULL OR shard=?) AND (sc_code IS NULL OR sc_code=?)
                AND (region IS NULL OR region=?) AND (year IS NULL OR year=?)
                AND (month IS NULL OR month=?) AND (day IS NULL OR day=?)
                AND (semester IS NULL OR semester=?) AND (address IS NULL OR address=?)
                AND (sub_key IS NULL OR sub_key=?)
                AND status='FAILED' AND resolved_at IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    domain, task_type,
                    shard, sc_code, region, year, month, day, semester, address, sub_key,
                ),
            ).fetchone()

            if row:
                failure_id = row["id"]
                current_retries = int(row["retries"] or 0)
                new_retries = current_retries + 1
                next_attempt2 = self._compute_next_attempt(
                    now=now, retries=new_retries, deadline=deadline
                )
                if next_attempt2 is None:
                    self._mark_expired_on_conn(conn, failure_id, reason=error)
                    return False
                conn.execute(
                    """
                    UPDATE failures
                    SET retries=?, next_attempt=?, error_msg=?, status='FAILED', resolved_at=NULL
                    WHERE id=? AND status='FAILED' AND resolved_at IS NULL
                    """,
                    (new_retries, next_attempt2, error, failure_id),
                )
                return True

            conn.execute(
                """
                INSERT INTO failures (
                    domain, task_type, shard, sc_code, region, year, month, day, semester,
                    address, sub_key, retries, next_attempt, error_msg, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'FAILED')
                """,
                (
                    domain, task_type, shard, sc_code, region, year, month, day, semester,
                    address, sub_key, 1, next_attempt, error,
                ),
            )
            return True
            