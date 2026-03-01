#!/usr/bin/env python3
"""
Warm 클러스터 생성 (월 1회 실행)
"""
import json
import os
from collections import defaultdict
from typing import Dict, List

# 가상의 로그 데이터 (실제로는 GA4 등에서 수집)
def fetch_search_logs() -> List[Dict]:
    """
    각 세션별 검색한 학교 코드 리스트 반환
    [{"session_id": "1", "schools": ["7012345", "7012346"]}, ...]
    """
    # 예시 데이터
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
    
    warm_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "../data/baskets/warm")
    os.makedirs(warm_dir, exist_ok=True)
    
    for school in schools:
        related = sorted(co_occur[school].items(), key=lambda x: -x[1])
        top = [s for s, _ in related[:10]]
        with open(os.path.join(warm_dir, f"{school}.json"), "w") as f:
            json.dump(top, f)
    print(f"✅ warm 클러스터 {len(schools)}개 생성 완료")

if __name__ == "__main__":
    build_warm_clusters()
    