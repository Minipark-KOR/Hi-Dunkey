# constants/paths.py
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data"

# 기본 디렉토리 (하드코딩, config 없이도 동작)
ACTIVE_DIR = DATA_DIR / "active"
MASTER_DIR = DATA_DIR / "master"
LOG_DIR = DATA_DIR / "logs"
METRICS_DIR = DATA_DIR / "metrics"
TEMP_DIR = DATA_DIR / "temp"
CACHE_DIR = DATA_DIR / "cache"
EXPORT_DIR = DATA_DIR / "export"
QUERIES_DIR = DATA_DIR / "queries"
GA4_DIR = DATA_DIR / "ga4"
REPORTS_DIR = DATA_DIR / "reports"
STATS_DIR = REPORTS_DIR / "stats"
EXCEL_DIR = REPORTS_DIR / "excel"
ARCHIVE_DIR = DATA_DIR / "archive"
BASKETS_DIR = DATA_DIR / "baskets"
WARM_DIR = BASKETS_DIR / "warm"
BACKUP_DIR = WARM_DIR  # 최근 날짜별 백업 (data/baskets/warm/)

# DB 파일 경로
NEIS_INFO_DB_PATH = MASTER_DIR / "neis_info.db"
SCHOOL_INFO_DB_PATH = MASTER_DIR / "school_info.db"
MEAL_DB_PATH = ACTIVE_DIR / "meal.db"
SCHEDULE_DB_PATH = ACTIVE_DIR / "schedule.db"
TIMETABLE_DB_PATH = ACTIVE_DIR / "timetable.db"
FAILURES_DB_PATH = DATA_DIR / "failures.db"
GLOBAL_VOCAB_DB_PATH = ACTIVE_DIR / "global_vocab.db"
UNKNOWN_DB_PATH = ACTIVE_DIR / "unknown_patterns.db"
NEIS_INFO_ODD_DB_PATH = MASTER_DIR / "neis_info_odd.db"
NEIS_INFO_EVEN_DB_PATH = MASTER_DIR / "neis_info_even.db"
NEIS_INFO_TOTAL_DB_PATH = MASTER_DIR / "neis_info_total.db"
MEAL_ODD_DB_PATH = ACTIVE_DIR / "meal_odd.db"
MEAL_EVEN_DB_PATH = ACTIVE_DIR / "meal_even.db"
MEAL_TOTAL_DB_PATH = ACTIVE_DIR / "meal_total.db"
SCHEDULE_ODD_DB_PATH = ACTIVE_DIR / "schedule_odd.db"
SCHEDULE_EVEN_DB_PATH = ACTIVE_DIR / "schedule_even.db"
SCHEDULE_TOTAL_DB_PATH = ACTIVE_DIR / "schedule_total.db"
TIMETABLE_ODD_DB_PATH = ACTIVE_DIR / "timetable_odd.db"
TIMETABLE_EVEN_DB_PATH = ACTIVE_DIR / "timetable_even.db"
TIMETABLE_TOTAL_DB_PATH = ACTIVE_DIR / "timetable_total.db"
SCHOOL_INFO_ODD_DB_PATH = MASTER_DIR / "school_info_odd.db"
SCHOOL_INFO_EVEN_DB_PATH = MASTER_DIR / "school_info_even.db"
ADDITIONAL_SCHOOL_INFO_DB_PATH = MASTER_DIR / "additional_school_info.db"
NEIS_ENRICHED_DB_PATH = MASTER_DIR / "neis_enriched.db"
SCHOOL_ENRICHED_DB_PATH = MASTER_DIR / "school_enriched.db"
SCHOOL_MASTER_DB_PATH = MASTER_DIR / "school_master.db"

# 모든 디렉토리 생성
for d in [ACTIVE_DIR, MASTER_DIR, LOG_DIR, METRICS_DIR, TEMP_DIR, CACHE_DIR,
          EXPORT_DIR, QUERIES_DIR, GA4_DIR, REPORTS_DIR, STATS_DIR, EXCEL_DIR,
          BASKETS_DIR, WARM_DIR]:
    d.mkdir(parents=True, exist_ok=True)
