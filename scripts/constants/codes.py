import os
# 깃허브 시크릿에서 API 키 로드
NEIS_API_KEY = os.getenv("NEIS_API_KEY", "")
NEIS_ENDPOINTS = {
    'school': 'https://open.neis.go.kr/hub/schoolInfo',
    'meal': 'https://open.neis.go.kr/hub/mealServiceDietInfo',
    'schedule': 'https://open.neis.go.kr/hub/SchoolSchedule',
    'timetable': 'https://open.neis.go.kr/hub/hisTimetable'
}
ALL_REGIONS = ["B10", "C10", "D10", "E10", "F10", "G10", "H10", "I10", "J10", "K10", "M10", "N10", "P10", "Q10", "R10", "S10", "T10"]