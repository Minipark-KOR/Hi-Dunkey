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

# (선택) 하위 호환성을 위해 문자열 버전이 필요하면 별도 정의
# MASTER_DB_PATH_STR = str(NEIS_INFO_DB_PATH)

# 모든 디렉토리 생성
for d in [ACTIVE_DIR, MASTER_DIR, LOG_DIR, METRICS_DIR, 
          TEMP_DIR, CACHE_DIR, EXPORT_DIR, QUERIES_DIR, GA4_DIR]:
    d.mkdir(parents=True, exist_ok=True)
