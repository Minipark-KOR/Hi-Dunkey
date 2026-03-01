"""
도메인 설정 상수 (백업/병합용)
"""

DOMAIN_CONFIG = {
    "school": {
        "description":  "학교 기본정보",
        "db_path":      "data/master/school_info.db",          # ✅ 변경
        "table":        "schools",
        "enabled":      True,
        "merge_script": "merge_school_info_dbs.py",            # ✅ 변경
        "fetch_args":   lambda region, year: {"region": region},
    },
    "meal": {
        "description":  "급식 정보",
        "db_path":      "data/active/meal/meal.db",
        "table":        "meal",
        "enabled":      True,
        "merge_script": "merge_meal_dbs",
        "fetch_args":   lambda region, year: {"region": region},
    },
    "timetable": {
        "description":  "시간표 정보",
        "db_path":      "data/active/timetable/timetable.db",
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
        "db_path":      "data/active/schedule/schedule.db",
        "table":        "schedule",
        "enabled":      True,
        "merge_script": "merge_schedule_dbs",
        "fetch_args":   lambda region, year: {"region": region, "year": year},
    },
}

GLOBAL_DBS = [
    {"name":  "global_vocab.db", "path": "data/active/global_vocab.db", "table": "meta_vocab"},
    {"name":  "unknown_patterns.db", "path": "data/active/unknown_patterns.db", "table": "unknown_patterns"},
]
