# core/neis_validator.py
"""
NEIS 학교 코드 검증기
- neis_info.db에서 학교 코드 목록을 로드하여 메모리에 유지
- 다른 수집기에서 NEIS 등록 여부를 빠르게 확인할 수 있도록 함
"""
import os
import sqlite3
import threading
from typing import Set, Optional
from constants.paths import MASTER_DIR

class NeisValidator:
    _instance = None
    _lock = threading.Lock()
    _codes: Optional[Set[str]] = None
    _db_path = str(MASTER_DIR / "neis_info.db")

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._load()
        return cls._instance

    def _load(self):
        """neis_info.db에서 학교 코드를 로드"""
        if not os.path.exists(self._db_path):
            self._codes = set()
            return
        try:
            with sqlite3.connect(self._db_path) as conn:
                cur = conn.execute("SELECT sc_code FROM schools")
                self._codes = {row[0] for row in cur}
        except Exception as e:
            # 실제 환경에서는 logger 사용 권장
            print(f"⚠️ NEIS 코드 로드 실패: {e}")
            self._codes = set()

    def contains(self, school_code: str) -> bool:
        """학교 코드가 NEIS DB에 존재하는지 확인"""
        if self._codes is None:
            self._load()
        return school_code in self._codes

    def refresh(self):
        """수동으로 코드 목록 갱신 (예: 주기적 리로드)"""
        with self._lock:
            self._load()

    def get_all(self) -> Set[str]:
        """전체 코드 목록 반환 (복사본)"""
        if self._codes is None:
            self._load()
        return self._codes.copy()

# 싱글톤 인스턴스
neis_validator = NeisValidator()
