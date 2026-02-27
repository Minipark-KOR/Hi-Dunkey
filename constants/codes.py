#!/usr/bin/env python3
"""
공통 상수 (API 키, 엔드포인트, 지역 코드, 배치 설정 등)
"""
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# =====================[ 버전 정보 ]=====================
__version__ = '2.0.0'
__last_updated__ = '2026-02-27'

# =====================[ API Keys ]=====================
NEIS_API_KEY = os.environ.get("NEIS_API_KEY", "")
SCHOOL_INFO_API_KEY = os.environ.get("SCHOOL_INFO_API_KEY", "")
KASI_API_KEY = os.environ.get("KASI_API_KEY", "")
VWORLD_API_KEY = os.environ.get("VWORLD_API_KEY", "")

# =====================[ NEIS API Endpoints ]=====================
NEIS_ENDPOINTS = {
    'school': 'https://open.neis.go.kr/hub/schoolInfo',
    'meal': 'https://open.neis.go.kr/hub/mealServiceDietInfo',
    'schedule': 'https://open.neis.go.kr/hub/schoolSchedule',
    'timetable': 'https://open.neis.go.kr/hub/misTimetable',
}

# ✅ 학교급별 시간표 엔드포인트 (약어/정식명칭 동시 지원)
TIMETABLE_ENDPOINTS = {
    # 정식 명칭
    '초등학교': 'https://open.neis.go.kr/hub/elsTimetable',
    '중학교': 'https://open.neis.go.kr/hub/misTimetable',
    '고등학교': 'https://open.neis.go.kr/hub/hisTimetable',
    '특수학교': 'https://open.neis.go.kr/hub/spsTimetable',
    # 약어 (DB 데이터 대응)
    '초': 'https://open.neis.go.kr/hub/elsTimetable',
    '중': 'https://open.neis.go.kr/hub/misTimetable',
    '고': 'https://open.neis.go.kr/hub/hisTimetable',
    '특': 'https://open.neis.go.kr/hub/spsTimetable',
}

# ✅ 시간표 응답 키 매핑 (안전한 파싱을 위해)
TIMETABLE_RESPONSE_KEYS = {
    'elsTimetable': 'elsTimetable',
    'misTimetable': 'misTimetable',
    'hisTimetable': 'hisTimetable',
    'spsTimetable': 'spsTimetable',
}

# ✅ 학교급별 학년 범위 (전공과 13-15 확장)
GRADE_RANGES = {
    '초등학교': range(1, 7),
    '중학교': range(1, 4),
    '고등학교': range(1, 4),
    '특수학교': range(0, 16),  # 0(유치원) ~ 15(전공과 3년차)
    # 약어 대응
    '초': range(1, 7),
    '중': range(1, 4),
    '고': range(1, 4),
    '특': range(0, 16),
}

# ✅ 특수학교 상세 학년 설명
SPECIAL_GRADE_DESC = {
    0: "유치원 과정",
    1: "초등 1", 2: "초등 2", 3: "초등 3", 4: "초등 4", 5: "초등 5", 6: "초등 6",
    7: "중등 1 (중1)", 8: "중등 2 (중2)", 9: "중등 3 (중3)",
    10: "고등 1 (고1)", 11: "고등 2 (고2)", 12: "고등 3 (고3)",
    13: "전공과 1년차", 14: "전공과 2년차", 15: "전공과 3년차"
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

# =====================[ 학교 코드 범위 ]=====================
SCHOOL_RANGES = {
    'A': (1, 4),
    'B': (5, 9),
    'A1': (1, 3),
    'B1': (4, 6),
    'C1': (7, 9),
    'Z': (0, 0),
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
BOT_COUNT = max(1, int(os.environ.get("BOT_COUNT", "10")))

API_CONFIG = {
    'timeout': 20,
    'max_retries': 3,
    'backoff': 1.0,
    'retry_status': [429, 500, 502, 503, 504],
    'rate_limit': {
        'per_second': 50,
        'per_bot': 50 // BOT_COUNT,
        'sleep_time': round(BOT_COUNT / 50, 3),
    }
}

# =====================[ 라이프사이클 날짜 ]=====================
LIFECYCLE_DATE = "0222"


# =====================[ API 키 검증 ]=====================
def check_api_keys() -> Dict[str, bool]:
    required_keys = {
        'NEIS': NEIS_API_KEY,
        'SCHOOL_INFO': SCHOOL_INFO_API_KEY,
    }
    
    status = {}
    for name, key in required_keys.items():
        status[name] = bool(key)
    
    if not all(status.values()):
        missing = [k for k, v in status.items() if not v]
        print(f"⚠️ 누락된 API 키: {', '.join(missing)}")
        print(f"   .env 파일 위치: {env_path}")
    
    return status


if __name__ == "__main__":
    print(f"📋 NEIS Collector v{__version__}")
    print(f"📁 .env 파일 위치: {env_path}")
    print("\n🔑 API 키 상태:")
    for name, present in check_api_keys().items():
        status = "✅" if present else "❌"
        print(f"  {status} {name}")
    
    print(f"\n🌍 전체 지역: {len(ALL_REGIONS)}개")
    print(f"📊 학교 범위: {list(SCHOOL_RANGES.keys())}")