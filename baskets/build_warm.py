#!/usr/bin/env python3
"""
Warm 클러스터 생성 (월 1회 실행)
- GA4 등 검색 로그 기반으로 동시 검색된 학교 쌍을 분석
- 결과는 data/baskets/warm/ 디렉토리에 학교별 JSON 파일로 저장
"""
import json
import os
from collections import defaultdict
from typing import Dict, List

# 프로젝트 루트를 sys.path에 추가 (필요시)
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from constants.paths import WARM_DIR
from core.logger import build_logger
from core.kst_time import now_kst

logger = build_logger(__name__, str(Path(__file__).parent.parent / "logs" / "build_warm.log"))


def fetch_search_logs() -> List[Dict]:
    """
    각 세션별 검색한 학교 코드 리스트 반환
    TODO: 실제 GA4 API 또는 로그 데이터베이스에서 가져오도록 구현
    현재는 예시 데이터 반환
    """
    # 예시 데이터 (실제로는 외부에서 수집)
    return [
        {"session_id": "1", "schools": ["7012345", "7012346"]},
        {"session_id": "2", "schools": ["7012345", "7034567"]},
        {"session_id": "3", "schools": ["7012346", "7034567"]},
    ]


def build_warm_clusters():
    logs = fetch_search_logs()
    co_occur = defaultdict(lambda: defaultdict(int))
    schools = set()

    for log in logs:
        sch_list = log['schools']
        for i, s1 in enumerate(sch_list):
            schools.add(s1)
            for s2 in sch_list[i+1:]:
                co_occur[s1][s2] += 1
                co_occur[s2][s1] += 1

    os.makedirs(WARM_DIR, exist_ok=True)

    for school in schools:
        related = sorted(co_occur[school].items(), key=lambda x: -x[1])
        top = [s for s, _ in related[:10]]
        with open(os.path.join(WARM_DIR, f"{school}.json"), "w") as f:
            json.dump(top, f)

    logger.info(f"✅ warm 클러스터 {len(schools)}개 생성 완료 (저장 위치: {WARM_DIR})")


if __name__ == "__main__":
    build_warm_clusters()
    