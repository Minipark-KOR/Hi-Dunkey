#!/usr/bin/env python3
"""
NEIS API를 통해 모든 교육청의 학교 수를 확인하는 스크립트
"""
import requests
import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from constants.codes import ALL_REGIONS
from core.config import config

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
    
    for region in ALL_REGIONS:
        count = get_school_count(region, api_key)
        results[region] = count
        total += count
        print(f"  {region}: {count:>5}개")
        time.sleep(0.5)  # API 부하 방지
    
    print("\n" + "=" * 30)
    print(f"📌 전체 학교 수: {total}개")
    
    return results, total

if __name__ == "__main__":
    # API 키는 환경변수 또는 config에서 가져옴
    api_key = config.get_api_key('neis')
    if not api_key:
        print("❌ NEIS_API_KEY가 설정되지 않았습니다.")
        sys.exit(1)
    
    check_all_regions(api_key)
    