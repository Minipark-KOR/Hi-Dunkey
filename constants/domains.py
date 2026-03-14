# constants/domains.py
"""
도메인 설정 상수 (백업/병합용)

목적:
- 도메인 정책의 단일 관리 지점
- 수집기 자동 등록 결과와 런타임 동기화
- 이름 변경 시 도메인 기준 일괄 변경 지원
"""
from copy import deepcopy
from typing import Dict, Any

from constants.paths import NEIS_INFO_DB_PATH, MEAL_DB_PATH, TIMETABLE_DB_PATH, SCHEDULE_DB_PATH
from constants.paths import GLOBAL_VOCAB_DB_PATH, UNKNOWN_DB_PATH

DOMAIN_CONFIG = {
    "school": {
        "description":  "학교 기본정보",
        # 도메인 이름과 실제 수집기 모듈명이 다를 수 있으므로 명시
        "collector_name": "neis_info",
        "db_path": str(NEIS_INFO_DB_PATH),
        "table":        "schools_neis",
        "enabled":      True,
        "merge_script": "merge_neis_info_dbs.py",            # 확장자 추가
        "fetch_args":   lambda region, year: {"region": region},
        "aliases":      ["neis", "neis_info"],
    },
    "school_info": {
        "description":  "학교 기본정보 (학교알리미)",
        "collector_name": "school_info",
        "db_path": str(NEIS_INFO_DB_PATH),
        "table":        "schools_info",
        "enabled":      True,
        "merge_script": "merge_school_info_dbs.py",
        "fetch_args":   lambda region, year: {"region": region, "year": year},
        "aliases":      ["schoolinfo"],
    },
    "meal": {
        "description":  "급식 정보",
        "collector_name": "meal",
        "db_path": str(MEAL_DB_PATH),
        "table":        "meal",
        "enabled":      True,
        "merge_script": "merge_meal_dbs.py",                 # 확장자 추가
        "fetch_args":   lambda region, year: {"region": region},
    },
    "timetable": {
        "description":  "시간표 정보",
        "collector_name": "timetable",
        "db_path": str(TIMETABLE_DB_PATH),
        "table":        "timetable",
        "enabled":      True,
        "merge_script": "merge_timetable_dbs.py",            # 확장자 추가
        "fetch_args":   lambda region, year: [
            {"region": region, "year": year, "semester": 1},
            {"region": region, "year": year, "semester": 2},
        ],
    },
    "schedule": {
        "description":  "학사일정 정보",
        "collector_name": "schedule",
        "db_path": str(SCHEDULE_DB_PATH),
        "table":        "schedule",
        "enabled":      True,
        "merge_script": "merge_schedule_dbs.py",             # 확장자 추가
        "fetch_args":   lambda region, year: {"region": region, "year": year},
    },
}

GLOBAL_DBS = [
   {"name": "global_vocab.db", "path": str(GLOBAL_VOCAB_DB_PATH), "table": "meta_vocab"},
   {"name": "unknown_patterns.db", "path": str(UNKNOWN_DB_PATH), "table": "unknown_patterns"},
]


def get_runtime_domain_config(collectors: Dict[str, type]) -> Dict[str, Dict[str, Any]]:
    """
    자동 등록된 수집기 정보로 DOMAIN_CONFIG를 런타임 동기화합니다.
    - 명시된 도메인은 collector_name 기준으로 메타데이터를 보강
    - DOMAIN_CONFIG에 없는 수집기는 자동 도메인 엔트리로 추가
    """
    runtime_cfg: Dict[str, Dict[str, Any]] = deepcopy(DOMAIN_CONFIG)

    mapped_collectors = set()
    for domain_name, meta in runtime_cfg.items():
        collector_name = meta.get("collector_name", domain_name)
        meta["collector_name"] = collector_name

        cls = collectors.get(collector_name)
        if cls is None:
            continue

        mapped_collectors.add(collector_name)
        meta.setdefault("schema_name", getattr(cls, "schema_name", None))
        meta["table"] = getattr(cls, "table_name", meta.get("table"))
        if not meta.get("description"):
            meta["description"] = getattr(cls, "description", collector_name)

    for collector_name, cls in collectors.items():
        if collector_name in mapped_collectors:
            continue

        runtime_cfg[collector_name] = {
            "description": getattr(cls, "description", collector_name),
            "collector_name": collector_name,
            "db_path": None,
            "table": getattr(cls, "table_name", None),
            "schema_name": getattr(cls, "schema_name", None),
            "enabled": True,
            "merge_script": getattr(cls, "merge_script", None),
            "fetch_args": lambda region, year: {"region": region, "year": year},
            "auto_registered": True,
            "aliases": [],
        }

    return runtime_cfg


def resolve_collector_name(name: str, collectors: Dict[str, type]) -> str:
    """
    입력 이름(수집기명/도메인명/alias)을 실제 수집기 모듈명으로 해석합니다.
    """
    # CLI/대시보드 입력은 대소문자, 하이픈 표기 차이가 날 수 있으므로 정규화
    normalized = name.strip().lower().replace("-", "_")

    normalized_collectors = {
        k.strip().lower().replace("-", "_"): k for k in collectors.keys()
    }

    if normalized in normalized_collectors:
        return normalized_collectors[normalized]

    if name in collectors:
        return name

    runtime_cfg = get_runtime_domain_config(collectors)

    normalized_domains = {
        k.strip().lower().replace("-", "_"): k for k in runtime_cfg.keys()
    }

    if normalized in normalized_domains:
        domain_key = normalized_domains[normalized]
        collector_name = runtime_cfg[domain_key].get("collector_name", domain_key)
        normalized_collector_name = collector_name.strip().lower().replace("-", "_")
        if normalized_collector_name in normalized_collectors:
            return normalized_collectors[normalized_collector_name]

    if name in runtime_cfg:
        collector_name = runtime_cfg[name].get("collector_name", name)
        if collector_name in collectors:
            return collector_name

    for _, meta in runtime_cfg.items():
        for alias in meta.get("aliases", []):
            normalized_alias = str(alias).strip().lower().replace("-", "_")
            if normalized_alias == normalized:
                collector_name = meta.get("collector_name")
                if not collector_name:
                    continue
                normalized_collector_name = collector_name.strip().lower().replace("-", "_")
                if normalized_collector_name in normalized_collectors:
                    return normalized_collectors[normalized_collector_name]
                if collector_name in collectors:
                    return collector_name

    return name
