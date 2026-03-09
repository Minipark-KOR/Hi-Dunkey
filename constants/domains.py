"""
도메인 설정 상수 (백업/병합용)
"""
from constants.paths import NEIS_INFO_DB_PATH, MEAL_DB_PATH, TIMETABLE_DB_PATH, SCHEDULE_DB_PATH
from constants.paths import GLOBAL_VOCAB_DB_PATH, UNKNOWN_DB_PATH

DOMAIN_CONFIG = {
    "school": {
        "description":  "학교 기본정보",
        "db_path": str(NEIS_INFO_DB_PATH),
        "table":        "schools",
        "enabled":      True,
        "merge_script": "merge_neis_info_dbs.py",            # ✅ 변경
        "fetch_args":   lambda region, year: {"region": region},
    },
    "meal": {
        "description":  "급식 정보",
        "db_path": str(MEAL_DB_PATH),
        "table":        "meal",
        "enabled":      True,
        "merge_script": "merge_meal_dbs",
        "fetch_args":   lambda region, year: {"region": region},
    },
    "timetable": {
        "description":  "시간표 정보",
        "db_path": str(TIMETABLE_DB_PATH),
        "table":        "timetable",
        "enabled":      True,
        "merge_script": "merge_timetable_dbs",
        "fetch_args":   lambda region, year: [
            {"region": region, "year": year, "semester": 1},
            {"region": region, "year": year, "semester": 2},
        ],
    },
    "schedule": {
        "description":  "학사일정 정보",
        "db_path": str(SCHEDULE_DB_PATH),
        "table":        "schedule",
        "enabled":      True,
        "merge_script": "merge_schedule_dbs",
        "fetch_args":   lambda region, year: {"region": region, "year": year},
    },
}

GLOBAL_DBS = [
   {"name": "global_vocab.db", "path": str(GLOBAL_VOCAB_DB_PATH), "table": "meta_vocab"},
   {"name": "unknown_patterns.db", "path": str(UNKNOWN_DB_PATH), "table": "unknown_patterns"},
]
