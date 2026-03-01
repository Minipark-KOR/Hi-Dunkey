#!/usr/bin/env python3
"""
데이터베이스 연결 공통 모듈 (PRAGMA 설정 포함)
"""
import sqlite3
from contextlib import contextmanager
from typing import Generator

@contextmanager
def get_db_connection(db_path: str, timeout: float = 30.0) -> Generator[sqlite3.Connection, None, None]:
    """
    SQLite 연결 컨텍스트 매니저
    - WAL 모드, 외래키 ON, 동기화 NORMAL, 캐시 설정 적용
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    conn.row_factory = sqlite3.Row
    try:
        # 연결마다 반드시 설정 (SQLite 기본값 OFF)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")   # 64MB 캐시
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def attach_master_db(conn: sqlite3.Connection, master_path: str = "../data/master/school_master.db") -> None:
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
    