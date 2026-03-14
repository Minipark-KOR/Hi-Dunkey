#!/usr/bin/env python3
"""Hot 지역 목록 로더.

현재 프로젝트에서는 GA4 집계 파일이 있을 때만 Hot 목록을 사용하고,
없으면 빈 목록을 반환하여 호출부에서 안전 fallback(ALL) 처리합니다.
"""

import json
from pathlib import Path
from typing import List

from constants.codes import ALL_REGIONS
from constants.paths import GA4_DIR


def _normalize_region_list(values: List[str]) -> List[str]:
    normalized = []
    seen = set()
    for item in values:
        code = str(item).strip().upper()
        if code in ALL_REGIONS and code not in seen:
            normalized.append(code)
            seen.add(code)
    return normalized


def get_hot_schools(limit: int = 50) -> List[str]:
    """Hot 지역 코드 목록을 반환합니다.

    우선순위:
    1) data/ga4/hot_regions.json (리스트 또는 {regions:[...]})
    2) 없거나 파싱 실패 시 []
    """
    path = Path(GA4_DIR) / "hot_regions.json"
    if not path.exists():
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            regions = raw.get("regions", [])
        elif isinstance(raw, list):
            regions = raw
        else:
            return []

        return _normalize_region_list(regions)[: max(0, int(limit))]
    except Exception:
        return []
