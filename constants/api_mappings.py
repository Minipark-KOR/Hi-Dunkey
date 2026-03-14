#!/usr/bin/env python3
# constants/api_mappings.py
"""
API 응답 키와 내부 필드명 간 매핑 정의
"""
from typing import Dict

# NEIS 공통 매핑
NEIS_COMMON_MAP: Dict[str, str] = {
    'SD_SCHUL_CODE': 'school_code',
    'ATPT_OFCDC_SC_CODE': 'region_code',
    'SCHUL_NM': 'school_name',
}

# NEIS 학교정보 추가 매핑
NEIS_SCHOOL_MAP: Dict[str, str] = {
    'ENG_SCHUL_NM': 'eng_name',
    'SCHUL_KND_SC_NM': 'school_kind',
    'ORG_RDNMA': 'address',
    'ORG_TELNO': 'phone',
    'HMPG_ADRES': 'homepage',
    'ATPT_OFCDC_SC_NM': 'atpt_ofcdc_sc_nm',
    'LCTN_SC_NM': 'lctn_sc_nm',
    'JU_ORG_NM': 'ju_org_nm',
}

# NEIS 급식 추가 매핑
NEIS_MEAL_MAP: Dict[str, str] = {
    'MLSV_YMD': 'meal_date',
    'MMEAL_SC_CODE': 'meal_type',
    'DDISH_NM': 'menu',
    'CAL_INFO': 'calories',
    'NTR_INFO': 'nutrition',
    'LOAD_DTM': 'load_dt',
}

# NEIS 시간표 추가 매핑
NEIS_TIMETABLE_MAP: Dict[str, str] = {
    'AY': 'year',
    'SEM': 'semester',
    'GRADE': 'grade',
    'CLASS_NM': 'class_name',
    'PERIO': 'period',
    'ITRT_CNTNT': 'subject',
    'SCHUL_KND_SC_NM': 'school_level',
}

# NEIS 학사일정 추가 매핑
NEIS_SCHEDULE_MAP: Dict[str, str] = {
    'AA_YMD': 'event_date',
    'EVENT_NM': 'event_name',
    'EVENT_CNTNT': 'event_content',
}

# 학교알리미 API → 내부 필드명 매핑
SCHOOL_INFO_MAP: Dict[str, str] = {
    'SCHUL_CODE': 'school_code',               # 정보공시 학교코드
    'SCHUL_NM': 'school_name',                  # 학교명
    'ATPT_OFCDC_ORG_NM': 'atpt_ofcdc_org_nm',   # 시도교육청명
    'ATPT_OFCDC_ORG_CODE': 'atpt_ofcdc_org_code', # 시도교육청코드
    'JU_ORG_NM': 'ju_org_nm',                    # 교육지원청명
    'JU_ORG_CODE': 'ju_org_code',                # 교육지원청코드
    'ADRCD_NM': 'adrcd_nm',                      # 지역명
    'ADRCD_CD': 'adrcd_cd',                      # 지역코드
    'LCTN_SC_CODE': 'lctn_sc_code',              # 소재지구분코드
    'SCHUL_KND_SC_CODE': 'schul_knd_sc_code',    # 학교급코드
    'FOND_SC_CODE': 'fond_sc_code',              # 설립구분
    'HS_KND_SC_NM': 'hs_knd_sc_nm',              # 학교특성
    'BNHH_YN': 'bnhh_yn',                         # 분교여부
    'SCHUL_FOND_TYP_CODE': 'schul_fond_typ_code', # 설립유형
    'DGHT_SC_CODE': 'dght_sc_code',               # 주야구분
    'FOAS_MEMRD': 'foas_memrd',                   # 개교기념일
    'FOND_YMD': 'fond_ymd',                       # 설립일
    'ADRES_BRKDN': 'adres_brkdn',                 # 주소내역
    'DTLAD_BRKDN': 'dtlad_brkdn',                 # 상세주소내역
    'ZIP_CODE': 'zip_code',                       # 우편번호
    'SCHUL_RDNZC': 'schul_rdnzc',                 # 학교도로명 우편번호
    'SCHUL_RDNMA': 'schul_rdnma',                 # 학교도로명 주소
    'SCHUL_RDNDA': 'schul_rdnda',                 # 학교도로명 상세주소
    'LTTUD': 'lttud',                              # 위도
    'LGTUD': 'lgtud',                              # 경도
    'USER_TELNO': 'user_telno',                    # 전화번호
    'USER_TELNO_SW': 'user_telno_sw',              # 전화번호(교무실)
    'USER_TELNO_GA': 'user_telno_ga',              # 전화번호(행정실)
    'PERC_FAXNO': 'perc_faxno',                    # 팩스번호
    'HMPG_ADRES': 'hmpg_adres',                    # 홈페이지 주소
    'COEDU_SC_CODE': 'coedu_sc_code',              # 남녀공학 구분
    'ABSCH_YN': 'absch_yn',                        # 폐교여부
    'ABSCH_YMD': 'absch_ymd',                      # 폐교일자
    'CLOSE_YN': 'close_yn',                        # 휴교여부
    'SCHUL_CRSE_SC_VALUE': 'schul_crse_sc_value',  # 학교과정구분값
    'SCHUL_CRSE_SC_VALUE_NM': 'schul_crse_sc_value_nm', # 학교과정구분명
}

# 컨텍스트별 매핑 (school_info 포함)
NEIS_FIELD_MAP_BY_CONTEXT: Dict[str, Dict[str, str]] = {
    'common': NEIS_COMMON_MAP,
    'school': {**NEIS_COMMON_MAP, **NEIS_SCHOOL_MAP},
    'meal': {**NEIS_COMMON_MAP, **NEIS_MEAL_MAP},
    'timetable': {**NEIS_COMMON_MAP, **NEIS_TIMETABLE_MAP},
    'schedule': {**NEIS_COMMON_MAP, **NEIS_SCHEDULE_MAP},
    'school_info': SCHOOL_INFO_MAP,   # 학교알리미 컨텍스트 추가
}

# 역매핑 생성 (내부 필드명 → API 키)
_REVERSE_MAPS: Dict[str, Dict[str, str]] = {
    ctx: {v: k for k, v in field_map.items()}
    for ctx, field_map in NEIS_FIELD_MAP_BY_CONTEXT.items()
}

def get_api_field(raw_item: dict, internal_field: str, context: str = 'common', default=None):
    """
    raw_item에서 internal_field에 해당하는 값을 추출합니다.
    context에 따라 적절한 API 키로 변환하여 조회합니다.
    """
    reverse_map = _REVERSE_MAPS.get(context, _REVERSE_MAPS['common'])
    api_key = reverse_map.get(internal_field, internal_field)
    # 디버그 모드에서만 경고 (개발 시 유용)
    if __debug__ and internal_field not in reverse_map:
        import warnings
        warnings.warn(f"Unknown internal field '{internal_field}' in context '{context}'", stacklevel=2)
    return raw_item.get(api_key, default)

def get_api_key(internal_field: str, context: str = 'common') -> str:
    """
    내부 필드명에 해당하는 API 키를 반환합니다.
    """
    reverse_map = _REVERSE_MAPS.get(context, _REVERSE_MAPS['common'])
    return reverse_map.get(internal_field, internal_field)