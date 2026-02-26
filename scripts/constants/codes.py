#!/usr/bin/env python3
"""
공통 상수 (API 키, 엔드포인트, 지역 코드, 배치 설정 등)
"""
import os
from pathlib import Path  # 명시적 경로 설정을 위해 추가
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

# =====================[ 환경 설정 ]=====================
# 명시적 경로로 .env 파일 로드 (실행 위치와 상관없이 항상 올바른 파일 참조)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# =====================[ 버전 정보 ]=====================
__version__ = '2.0.0'
__last_updated__ = '2024-03-27'

# =====================[ API Keys ]=====================
NEIS_API_KEY = os.environ.get("NEIS_API_KEY", "")
SCHOOL_INFO_API_KEY = os.environ.get("SCHOOL_INFO_API_KEY", "")
KASI_API_KEY = os.environ.get("KASI_API_KEY", "")
VWORLD_API_KEY = os.environ.get("VWORLD_API_KEY", "")

# =====================[ API 키 검증 ]=====================
def check_api_keys() -> Dict[str, bool]:
    """
    필수 API 키 존재 여부 확인
    Returns: 각 키의 존재 여부 딕셔너리
    """
    required_keys = {
        'NEIS': NEIS_API_KEY,
        'SCHOOL_INFO': SCHOOL_INFO_API_KEY,
    }
    
    status = {}
    for name, key in required_keys.items():
        status[name] = bool(key)
    
    # 누락된 키가 있으면 경고
    if not all(status.values()):
        missing = [k for k, v in status.items() if not v]
        print(f"⚠️ 누락된 API 키: {', '.join(missing)}")
        print(f"   .env 파일 위치: {env_path}")
    
    return status

# =====================[ NEIS API Endpoints ]=====================
NEIS_ENDPOINTS = {
    'school': 'https://open.neis.go.kr/hub/schoolInfo',
    'meal': 'https://open.neis.go.kr/hub/mealServiceDietInfo',
    'schedule': 'https://open.neis.go.kr/hub/schoolSchedule',
    'timetable': 'https://open.neis.go.kr/hub/classTimeTable',
}

# =====================[ 지역 코드 ]=====================
ALL_REGIONS: List[str] = [
    "B10","C10","D10","E10","F10","G10","H10","I10",
    "J10","K10","L10","M10","N10","P10","Q10","R10","S10","T10"
]

REGION_NAMES: Dict[str, str] = {
    "B10": "서울", "C10": "부산", "D10": "대구", "E10": "인천",
    "F10": "광주", "G10": "대전", "H10": "울산", "I10": "세종",
    "J10": "경기", "K10": "강원", "L10": "충북", "M10": "충남",
    "N10": "전북", "P10": "전남", "Q10": "경북", "R10": "경남",
    "S10": "제주", "T10": "교육부",
}

# =====================[ 학교 코드 범위 (샤딩용) ]=====================
SCHOOL_RANGES = {
    'A': (1, 4),   # 1~4 (기존 호환성)
    'B': (5, 9),   # 5~9 (기존 호환성)
    'A1': (1, 3),  # 세분화 (경기 4개 봇에 사용 중)
    'B1': (4, 6),
    'C1': (7, 9),
    'Z': (0, 0),   # 0으로 시작하는 특수 케이스
}

RANGE_NAMES = {
    'A': '1-4 (저범위)',
    'B': '5-9 (고범위)',
    'A1': '1-3',
    'B1': '4-6',
    'C1': '7-9',
    'Z': '0번대',
}

# =====================[ 학년 코드 ]=====================
GRADE_CODES = {
    'ONE': 11, 'TWO': 12, 'THREE': 13, 'FOUR': 14, 'FIVE': 15, 'SIX': 16,
    'MWO': 21, 'MWT': 22, 'MWR': 23,
    'HWO': 31, 'HWT': 32, 'HWR': 33,
    'TK': 40, 'JC': 50,
}
E_KEYS = ['ONE','TWO','THREE','FOUR','FIVE','SIX']
M_KEYS = ['MWO','MWT','MWR']
H_KEYS = ['HWO','HWT','HWR']

# =====================[ 급식 타입 ]=====================
MEAL_TYPES = {1: '조식', 2: '중식', 3: '석식'}

# =====================[ 요일 매핑 ]=====================
DAY_OF_WEEK = {'월':1, '화':2, '수':3, '목':4, '금':5, '토':6, '일':7}

# =====================[ 배치 설정 ]=====================
BATCH_CONFIG = {
    'meal': {'initial':500, 'min':100, 'max':2000, 'memory_mb':50},
    'schedule': {'initial':500, 'min':100, 'max':1500, 'memory_mb':40},
    'timetable': {'initial':300, 'min':50, 'max':1000, 'memory_mb':30},
    'master': {'initial':500, 'min':100, 'max':1500, 'memory_mb':30},
}

# =====================[ API 설정 ]=====================
API_CONFIG = {
    'timeout': 20,
    'max_retries': 3,
    'backoff': 1.0,
    'retry_status': [429, 500, 502, 503, 504],
}

# =====================[ 라이프사이클 날짜 ]=====================
LIFECYCLE_DATE = "0222"   # 매년 2월 22일 (학사일정 기준일)


# =====================[ 실행 시 간단한 검증 ]=====================
if __name__ == "__main__":
    print(f"📋 NEIS Collector v{__version__}")
    print(f"📁 .env 파일 위치: {env_path}")
    print("\n🔑 API 키 상태:")
    for name, present in check_api_keys().items():
        status = "✅" if present else "❌"
        print(f"  {status} {name}")
    
    print(f"\n🌍 전체 지역: {len(ALL_REGIONS)}개")
    print(f"📊 학교 범위: {list(SCHOOL_RANGES.keys())}")