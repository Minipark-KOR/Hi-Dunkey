#!/usr/bin/env python3
"""
Hot50 갱신 (GA4 데이터 기반) – 매일 새벽 실행
"""
import json
import os
from datetime import datetime
from typing import List

# 가상의 GA4 클라이언트 (실제로는 google.analytics.data_v1beta 사용)
def fetch_ga4_top_schools(period: str) -> List[str]:
    """
    period: 'daily', 'monthly', 'semester', 'yearly'
    """
    # 실제 구현에서는 GA4 API를 호출하여 인기 학교 코드 리스트 반환
    # 여기서는 예시 데이터 반환
    if period == 'daily':
        return ["7012345", "7012346", "7012347"]  # 예시
    elif period == 'monthly':
        return ["7012345", "7016789"]
    elif period == 'semester':
        return ["7012345", "7034567"]
    elif period == 'yearly':
        return ["7012345", "7056789"]
    return []

def update_hot_basket():
    baskets_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "../data/baskets")
    os.makedirs(baskets_dir, exist_ok=True)
    
    hot = {
        "daily": fetch_ga4_top_schools('daily'),
        "monthly": fetch_ga4_top_schools('monthly'),
        "semester": fetch_ga4_top_schools('semester'),
        "yearly": fetch_ga4_top_schools('yearly'),
        "updated_at": datetime.now().isoformat()
    }
    with open(os.path.join(baskets_dir, "hot.json"), "w") as f:
        json.dump(hot, f, indent=2)
    print("✅ hot.json 갱신 완료")

if __name__ == "__main__":
    update_hot_basket()
    