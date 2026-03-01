#!/usr/bin/env python3
"""
API 응답 키와 내부 필드명 간 매핑 정의
"""
from typing import Dict

NEIS_COMMON_MAP: Dict[str, str] = {
    'SD_SCHUL_CODE': 'school_code',
    'ATPT_OFCDC_SC_CODE': 'region_code',
    'SCHUL_NM': 'school_name',
}

NEIS_SCHOOL_MAP: Dict[str, str] = {
    'ENG_SCHUL_NM': 'eng_name',
    'SCHUL_KND_SC_NM': 'school_kind',
    'ORG_RDNMA': 'address',
    'ORG_TELNO': 'phone',
    'HMPG_ADRES': 'homepage',
}

NEIS_MEAL_MAP: Dict[str, str] = {
    'MLSV_YMD': 'meal_date',
    'MMEAL_SC_CODE': 'meal_type',
    'DDISH_NM': 'menu',
    'CAL_INFO': 'calories',
    'NTR_INFO': 'nutrition',
    'LOAD_DTM': 'load_dt',
}

NEIS_TIMETABLE_MAP: Dict[str, str] = {
    'AY': 'year',
    'SEM': 'semester',
    'GRADE': 'grade',
    'CLASS_NM': 'class_name',
    'PERIO': 'period',
    'ITRT_CNTNT': 'subject',
    'SCHUL_KND_SC_NM': 'school_level',
}

NEIS_SCHEDULE_MAP: Dict[str, str] = {
    'AA_YMD': 'event_date',
    'EVENT_NM': 'event_name',
    'EVENT_CNTNT': 'event_content',
}

NEIS_FIELD_MAP_BY_CONTEXT: Dict[str, Dict[str, str]] = {
    'common': NEIS_COMMON_MAP,
    'school': {**NEIS_COMMON_MAP, **NEIS_SCHOOL_MAP},
    'meal': {**NEIS_COMMON_MAP, **NEIS_MEAL_MAP},
    'timetable': {**NEIS_COMMON_MAP, **NEIS_TIMETABLE_MAP},
    'schedule': {**NEIS_COMMON_MAP, **NEIS_SCHEDULE_MAP},
}

_REVERSE_MAPS: Dict[str, Dict[str, str]] = {
    ctx: {v: k for k, v in field_map.items()}
    for ctx, field_map in NEIS_FIELD_MAP_BY_CONTEXT.items()
}

def get_api_field(raw_item: dict, internal_field: str, context: str = 'common', default=None):
    reverse_map = _REVERSE_MAPS.get(context, _REVERSE_MAPS['common'])
    api_key = reverse_map.get(internal_field, internal_field)
    if __debug__ and internal_field not in reverse_map:
        import warnings
        warnings.warn(f"Unknown internal field '{internal_field}' in context '{context}'", stacklevel=2)
    return raw_item.get(api_key, default)

def get_api_key(internal_field: str, context: str = 'common') -> str:
    reverse_map = _REVERSE_MAPS.get(context, _REVERSE_MAPS['common'])
    return reverse_map.get(internal_field, internal_field)
    