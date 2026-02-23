#!/usr/bin/env python3
"""
Hot50 갱신 (GA4 데이터 기반) – 매일 새벽 실행
"""
import json
import os
from typing import List

# 프로젝트 루트를 path에 추가 (core 모듈 임포트용)
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kst_time import now_kst

# 실제 GA4 연동 시 사용할 임포트 (주석 해제)
# from google.analytics.data_v1beta import BetaAnalyticsDataClient
# from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

# =====================[ GA4 데이터 가져오기 (예시) ]=====================
def fetch_ga4_top_schools(period: str) -> List[str]:
    """
    period: 'daily', 'monthly', 'semester', 'yearly'
    실제 구현 시 GA4 API를 호출하여 인기 학교 코드 리스트 반환
    """
    # TODO: GA4 API 연동 구현
    # 예시 데이터 반환
    if period == 'daily':
        return ["7012345", "7012346", "7012347"]
    elif period == 'monthly':
        return ["7012345", "7016789"]
    elif period == 'semester':
        return ["7012345", "7034567"]
    elif period == 'yearly':
        return ["7012345", "7056789"]
    return []

def get_hot_schools(period: str = 'daily') -> List[str]:
    """run_daily.py에서 호출할 함수"""
    return fetch_ga4_top_schools(period)

def update_hot_basket():
    baskets_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "../data/baskets")
    os.makedirs(baskets_dir, exist_ok=True)
    
    hot = {
        "daily": fetch_ga4_top_schools('daily'),
        "monthly": fetch_ga4_top_schools('monthly'),
        "semester": fetch_ga4_top_schools('semester'),
        "yearly": fetch_ga4_top_schools('yearly'),
        "updated_at": now_kst().isoformat()   # ✅ 한국시간 사용
    }
    with open(os.path.join(baskets_dir, "hot.json"), "w") as f:
        json.dump(hot, f, indent=2)
    print("✅ hot.json 갱신 완료")

if __name__ == "__main__":
    update_hot_basket()
    