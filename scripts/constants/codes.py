#!/usr/bin/env python3
"""
공통 상수 (API 키, 엔드포인트, 지역 코드, 배치 설정 등)
"""
import os
from typing import Dict, List

# =====================[ API Keys ]=====================
NEIS_API_KEY = os.environ.get("NEIS_API_KEY", "")
SCHOOL_ALRIMI_API_KEY = os.environ.get("SCHOOL_ALRIMI_API_KEY", "")
KASI_API_KEY = os.environ.get("KASI_API_KEY", "")
VWORLD_API_KEY = os.environ.get("VWORLD_API_KEY", "")

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
LIFECYCLE_DATE = "0222"   # 2월 22일
