#!/usr/bin/env python3
"""
수집 실행 메타 로그 테이블 관리
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from core.kst_time import now_kst
from constants.paths import ACTIVE_DIR

LOG_DB_PATH = ACTIVE_DIR / "collect_log.db"

class CollectLog:
    """수집 실행 로그 기록 및 조회"""

    def __init__(self, db_path: Path = LOG_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS collect_run (
                    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    step        TEXT NOT NULL,        -- 'meal', 'schedule', 'timetable', 'school'
                    region      TEXT NOT NULL,
                    shard       TEXT,
                    started_at  TEXT NOT NULL,
                    finished_at TEXT,
                    status      TEXT,                 -- 'running', 'success', 'failed', 'skipped'
                    rows_saved  INTEGER DEFAULT 0,
                    rows_before INTEGER DEFAULT 0,    -- DataGuard용
                    error_msg   TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_collect_run_step_region 
                ON collect_run(step, region)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_collect_run_status 
                ON collect_run(status)
            """)

    def start_run(self, step: str, region: str, shard: Optional[str] = None) -> int:
        """실행 시작 기록, run_id 반환"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                INSERT INTO collect_run (step, region, shard, started_at, status)
                VALUES (?, ?, ?, ?, 'running')
            """, (step, region, shard, now_kst().isoformat()))
            return cur.lastrowid

    def finish_run(self, run_id: int, status: str, rows_saved: int = 0,
                   rows_before: int = 0, error_msg: str = ""):
        """실행 완료 기록"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE collect_run
                SET finished_at = ?, status = ?, rows_saved = ?, rows_before = ?, error_msg = ?
                WHERE run_id = ?
            """, (now_kst().isoformat(), status, rows_saved, rows_before, error_msg, run_id))

    def get_failed_runs(self, step: Optional[str] = None) -> List[Dict]:
        """실패한 실행 목록 조회 (재시도용)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            query = "SELECT * FROM collect_run WHERE status = 'failed'"
            params = []
            if step:
                query += " AND step = ?"
                params.append(step)
            cur = conn.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def get_stats(self, step: str, date: str) -> Dict:
        """특정 날짜 통계 (예: 2026-03-01)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("""
                SELECT 
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(rows_saved) as total_rows
                FROM collect_run
                WHERE step = ? AND date(started_at) = ?
            """, (step, date))
            return dict(cur.fetchone())
            