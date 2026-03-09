# constants/paths.py
from pathlib import Path
from core.config import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# data/ 아래 모든 디렉토리 정의
DATA_DIR = PROJECT_ROOT / "data"

# 설정 파일에서 경로를 읽되, 없으면 기본값 사용
ACTIVE_DIR = DATA_DIR / config.get('paths', 'active_dir', default='active')
MASTER_DIR = DATA_DIR / config.get('paths', 'master_dir', default='master')
LOG_DIR = DATA_DIR / config.get('paths', 'logs_dir', default='logs')
METRICS_DIR = DATA_DIR / config.get('paths', 'metrics_dir', default='metrics')
TEMP_DIR = DATA_DIR / config.get('paths', 'temp_dir', default='temp')
CACHE_DIR = DATA_DIR / config.get('paths', 'cache_dir', default='cache')
EXPORT_DIR = DATA_DIR / config.get('paths', 'export_dir', default='export')
QUERIES_DIR = DATA_DIR / config.get('paths', 'queries_dir', default='queries')
GA4_DIR = DATA_DIR / config.get('paths', 'ga4_dir', default='ga4')

# 각 collector별 DB 파일 경로 (Path 객체) - 통일!
NEIS_INFO_DB_PATH = MASTER_DIR / "neis_info.db"
SCHOOL_INFO_DB_PATH = MASTER_DIR / "school_info.db"
MEAL_DB_PATH = ACTIVE_DIR / "meal.db"
SCHEDULE_DB_PATH = ACTIVE_DIR / "schedule.db"
TIMETABLE_DB_PATH = ACTIVE_DIR / "timetable.db"

# 공통 DB (Path 객체)
FAILURES_DB_PATH = DATA_DIR / "failures.db"
GLOBAL_VOCAB_DB_PATH = ACTIVE_DIR / "global_vocab.db"
UNKNOWN_DB_PATH = ACTIVE_DIR / "unknown_patterns.db"

# NEIS 학교 기본정보 DB
NEIS_INFO_ODD_DB_PATH = MASTER_DIR / "neis_info_odd.db"
NEIS_INFO_EVEN_DB_PATH = MASTER_DIR / "neis_info_even.db"
NEIS_INFO_TOTAL_DB_PATH = MASTER_DIR / "neis_info_total.db"
# NEIS 급식 병합 DB
MEAL_ODD_DB_PATH = ACTIVE_DIR / "meal_odd.db"
MEAL_EVEN_DB_PATH = ACTIVE_DIR / "meal_even.db"
MEAL_TOTAL_DB_PATH = ACTIVE_DIR / "meal_total.db"

# NEIS 학사일정 병합 DB
SCHEDULE_ODD_DB_PATH = ACTIVE_DIR / "schedule_odd.db"
SCHEDULE_EVEN_DB_PATH = ACTIVE_DIR / "schedule_even.db"
SCHEDULE_TOTAL_DB_PATH = ACTIVE_DIR / "schedule_total.db"

# NEIS 시간표 병합 DB
TIMETABLE_ODD_DB_PATH = ACTIVE_DIR / "timetable_odd.db"
TIMETABLE_EVEN_DB_PATH = ACTIVE_DIR / "timetable_even.db"
TIMETABLE_TOTAL_DB_PATH = ACTIVE_DIR / "timetable_total.db"

# 학교알리미 병합 DB
SCHOOL_INFO_ODD_DB_PATH = MASTER_DIR / "school_info_odd.db"
SCHOOL_INFO_EVEN_DB_PATH = MASTER_DIR / "school_info_even.db"

# (선택) 하위 호환성을 위해 문자열 버전이 필요하면 별도 정의
# MASTER_DB_PATH_STR = str(NEIS_INFO_DB_PATH)

# 모든 디렉토리 생성
for d in [ACTIVE_DIR, MASTER_DIR, LOG_DIR, METRICS_DIR, 
          TEMP_DIR, CACHE_DIR, EXPORT_DIR, QUERIES_DIR, GA4_DIR]:
    d.mkdir(parents=True, exist_ok=True)
