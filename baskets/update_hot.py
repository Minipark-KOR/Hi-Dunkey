# baskets/update_hot.py
from __future__ import annotations
from typing import List

def get_hot_schools(limit: int = 50) -> List[str]:
    """
    인기 학교 코드 목록 반환.
    TODO: 실제 검색 빈도 기반 통계 로직으로 교체 필요.
    현재는 전체 학교 중 일부를 반환하여 API 부하를 줄입니다.
    """
    try:
        from core.database import get_db_connection
        from constants.paths import MASTER_DB
        with get_db_connection(MASTER_DB) as conn:
            cur = conn.execute("SELECT sc_code FROM schools LIMIT ?", (limit,))
            return [row[0] for row in cur]
    except Exception:
        # DB 접근 실패 시 빈 리스트 반환 (fallback)
        return []
        