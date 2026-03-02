#!/usr/bin/env python3
"""
NEIS API를 통해 모든 교육청의 학교 수를 확인하는 스크립트
"""
import requests
import json
import time

# 교육청 코드 목록 (ALL_REGIONS 기준)
REGIONS = [
    "B10", "C10", "D10", "E10", "F10", "G10", "H10", "I10",
    "J10", "K10", "M10", "N10", "P10", "Q10", "R10", "S10", "T10"
]

def get_school_count(region_code: str, api_key: str) -> int:
    """
    특정 교육청의 전체 학교 수를 반환
    """
    url = "https://open.neis.go.kr/hub/schoolInfo"
    params = {
        "ATPT_OFCDC_SC_CODE": region_code,
        "KEY": api_key,
        "Type": "json",
        "pSize": 1  # 1개만 요청 (전체 개수만 필요)
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 응답 구조에서 전체 개수 추출
        total_count = data.get('schoolInfo', [{}])[0].get('head', [{}])[0].get('list_total_count', 0)
        return int(total_count)
    except Exception as e:
        print(f"⚠️ {region_code} 조회 실패: {e}")
        return 0

def check_all_regions(api_key: str):
    """
    모든 교육청의 학교 수를 조회하고 합계 출력
    """
    total = 0
    results = {}
    
    print("📊 교육청별 학교 수 조회 중...\n")
    
    for region in REGIONS:
        count = get_school_count(region, api_key)
        results[region] = count
        total += count
        print(f"  {region}: {count:>5}개")
        time.sleep(0.5)  # API 부하 방지
    
    print("\n" + "=" * 30)
    print(f"📌 전체 학교 수: {total}개")
    
    return results, total

if __name__ == "__main__":
    # 여기에 실제 API 키 입력
    API_KEY = "917818905d7b46e4b0eb71d2a15d9187"  # 예시 키
    
    check_all_regions(API_KEY)
    