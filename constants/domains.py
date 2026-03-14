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


def _normalize_name(name: str) -> str:
    return str(name).strip().lower().replace("-", "_")

DOMAIN_CONFIG = {
    "school": {
        "description":  "학교 기본정보",
        # 도메인 이름과 실제 수집기 모듈명이 다를 수 있으므로 명시
        "collector_name": "neis_info",
        "db_path": str(NEIS_INFO_DB_PATH),
        "table":        "schools_neis",
        "enabled":      True,
        "merge_script": "neis_info.py",
        "fetch_args":   lambda region, year: {"region": region},
        "aliases":      ["neis", "neis_info"],
    },
    "school_info": {
        "description":  "학교 기본정보 (학교알리미)",
        "collector_name": "school_info",
        "db_path": str(NEIS_INFO_DB_PATH),
        "table":        "schools_info",
        "enabled":      True,
        "merge_script": "school_info.py",
        "fetch_args":   lambda region, year: {"region": region, "year": year},
        "aliases":      ["schoolinfo"],
    },
    "meal": {
        "description":  "급식 정보",
        "collector_name": "meal",
        "db_path": str(MEAL_DB_PATH),
        "table":        "meal",
        "enabled":      True,
        "merge_script": "meal.py",
        "fetch_args":   lambda region, year: {"region": region},
    },
    "timetable": {
        "description":  "시간표 정보",
        "collector_name": "timetable",
        "db_path": str(TIMETABLE_DB_PATH),
        "table":        "timetable",
        "enabled":      True,
        "merge_script": None,  # TODO: timetable 병합 스크립트 미이식
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
        "merge_script": None,  # TODO: schedule 병합 스크립트 미이식
        "fetch_args":   lambda region, year: {"region": region, "year": year},
    },
}

GLOBAL_DBS = [
   {"name": "global_vocab.db", "path": str(GLOBAL_VOCAB_DB_PATH), "table": "meta_vocab"},
   {"name": "unknown_patterns.db", "path": str(UNKNOWN_DB_PATH), "table": "unknown_patterns"},
]

# 리빌딩 단계에서는 canonical/domain 이름만 허용하고 legacy alias 해석은 비활성화합니다.
ENABLE_LEGACY_ALIAS_RESOLUTION = False


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

    리빌딩 단계에서는 legacy alias 해석을 비활성화하여 이름 변경 충돌을 조기에 노출합니다.
    """
    # CLI/대시보드 입력은 대소문자, 하이픈 표기 차이가 날 수 있으므로 정규화
    normalized = _normalize_name(name)

    normalized_collectors = {
        _normalize_name(k): k for k in collectors.keys()
    }

    if normalized in normalized_collectors:
        return normalized_collectors[normalized]

    if name in collectors:
        return name

    runtime_cfg = get_runtime_domain_config(collectors)

    normalized_domains = {
        _normalize_name(k): k for k in runtime_cfg.keys()
    }

    if normalized in normalized_domains:
        domain_key = normalized_domains[normalized]
        collector_name = runtime_cfg[domain_key].get("collector_name", domain_key)
        normalized_collector_name = _normalize_name(collector_name)
        if normalized_collector_name in normalized_collectors:
            return normalized_collectors[normalized_collector_name]

    if name in runtime_cfg:
        collector_name = runtime_cfg[name].get("collector_name", name)
        if collector_name in collectors:
            return collector_name

    if ENABLE_LEGACY_ALIAS_RESOLUTION:
        for _, meta in runtime_cfg.items():
            for alias in meta.get("aliases", []):
                normalized_alias = _normalize_name(alias)
                if normalized_alias == normalized:
                    collector_name = meta.get("collector_name")
                    if not collector_name:
                        continue
                    normalized_collector_name = _normalize_name(collector_name)
                    if normalized_collector_name in normalized_collectors:
                        return normalized_collectors[normalized_collector_name]
                    if collector_name in collectors:
                        return collector_name

    return name


def validate_name_resolution_map(collectors: Dict[str, type]) -> None:
    """collector/domain/alias 이름 해석 충돌을 검증합니다.

    동일 입력 토큰이 서로 다른 수집기로 매핑되면 예외를 발생시켜
    실행 초기에 충돌을 탐지합니다.
    """
    runtime_cfg = get_runtime_domain_config(collectors)
    token_to_targets: Dict[str, set] = {}
    token_to_sources: Dict[str, set] = {}

    def add_mapping(raw_token: str, target_collector: str, source: str) -> None:
        token = _normalize_name(raw_token)
        if not token:
            return
        token_to_targets.setdefault(token, set()).add(target_collector)
        token_to_sources.setdefault(token, set()).add(source)

    # collector 이름 자체는 항상 자기 자신으로 매핑
    for collector_name in collectors.keys():
        add_mapping(collector_name, collector_name, f"collector:{collector_name}")

    # domain/alias는 domain이 가리키는 collector_name에 매핑
    for domain_name, meta in runtime_cfg.items():
        collector_name = meta.get("collector_name", domain_name)
        if collector_name not in collectors:
            continue
        add_mapping(domain_name, collector_name, f"domain:{domain_name}")
        if ENABLE_LEGACY_ALIAS_RESOLUTION:
            for alias in meta.get("aliases", []):
                add_mapping(str(alias), collector_name, f"alias:{domain_name}:{alias}")

    collisions = []
    for token, targets in sorted(token_to_targets.items()):
        if len(targets) > 1:
            sources = ", ".join(sorted(token_to_sources.get(token, set())))
            collisions.append(
                f"- '{token}' -> {sorted(targets)} (sources: {sources})"
            )

    if collisions:
        details = "\n".join(collisions)
        raise ValueError(
            "수집기 이름 해석 충돌 감지:\n"
            f"{details}\n"
            "constants/domains.py의 collector_name/aliases를 조정하세요."
        )
