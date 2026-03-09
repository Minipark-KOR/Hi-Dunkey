#!/usr/bin/env python3
"""
데이터베이스 연결 공통 모듈 (PRAGMA 설정 포함)
"""
import sqlite3
from contextlib import contextmanager
from typing import Optional, Generator
from constants.paths import MASTER_DIR

@contextmanager
def get_db_connection(
    db_path: str,
    timeout: float = 30.0,
    row_factory: Optional[type] = None
) -> Generator[sqlite3.Connection, None, None]:
    """
    SQLite 읽기/쓰기 연결 컨텍스트 매니저.
    WAL 모드, 외래키 ON, 동기화 NORMAL, 캐시 64MB 적용.
    row_factory 전달 시 반영됨.
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA busy_timeout = 5000")
        if row_factory is not None:
            conn.row_factory = row_factory
        yield conn
        conn.commit()
        # 디버그 출력 제거 (logger 사용 권장)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_db_reader(
    db_path: str,
    timeout: float = 10.0,
    row_factory: Optional[type] = None
) -> Generator[sqlite3.Connection, None, None]:
    """
    읽기 전용 SQLite 연결.
    commit/rollback 없음 → write lock 유발하지 않음.
    SELECT 전용으로 사용.
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA query_only = ON")
        if row_factory is not None:
            conn.row_factory = row_factory
        yield conn
    finally:
        conn.close()


def attach_master_db(
    conn: sqlite3.Connection,
    master_path: str = str(MASTER_DIR / "school_master.db")
) -> None:
    """학교 마스터 DB 연결"""
    conn.execute(f"ATTACH DATABASE '{master_path}' AS master_db")


def init_checkpoint_table(conn: sqlite3.Connection) -> None:
    """체크포인트 테이블 생성 (공통)"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collection_checkpoint (
            collector_type TEXT,
            target_key     TEXT,
            region_code    TEXT,
            school_code    TEXT,
            sub_key        TEXT,
            last_page      INTEGER,
            total_items    INTEGER,
            completed_at   TEXT,
            PRIMARY KEY (collector_type, target_key, region_code, school_code, sub_key)
        )
    """)
    