# 📘 Hi-Dunkey 개발 가이드 (v2.0 - 자동 등록 시스템)

**최종 수정:** 2026-03-14  
**버전:** 2.0 (자동 등록 시스템 + 중앙 대시보드 통합)

이 문서는 Hi-Dunkey 프로젝트에서 새로운 수집기 (Collector) 및 데이터 파이프라인을 개발할 때 반드시 따라야 할 표준 패턴과 규칙을 설명합니다.

---

## 1. 프로젝트 구조 개요

### 📦 프로젝트 루트

```
├── config/                      # 설정 파일 디렉토리
│   └── config.yaml              # 환경별 설정 (경로, 타임아웃, 병렬 설정 등)
├── core/                        # 핵심 공통 모듈 (라이브러리 전용)
│   ├── collector_engine.py      # ✅ 모든 수집기의 베이스 클래스 (v2)
│   ├── config.py                # 설정 파일 로더
│   ├── database.py              # DB 연결 공통
│   ├── manage_schema.py         # 스키마 기반 테이블 생성
│   ├── collector_stats.py       # 배치 처리 통계
│   ├── data_validator.py        # 데이터 유효성 검증
│   ├── kst_time.py              # 시간 처리
│   ├── logger.py                # 로깅
│   ├── network.py               # 네트워크 요청
│   ├── retry.py                 # 실패 재시도 관리
│   ├── shard.py                 # 샤딩/범위 필터링
│   ├── address/                 # 주소/지역 관련 모듈
│   └── ...
├── scripts/                     # 실행 스크립트 (기능별 디렉토리 분리)
│   ├── collector/               # 원시 데이터 수집기
│   │   ├── __init__.py          # ✅ 자동 등록 시스템
│   │   ├── neis_info.py         # NEIS 원시 데이터 수집
│   │   └── school_info.py       # 학교알리미 원시 데이터 수집
│   ├── enrich/                  # 데이터 보강
│   ├── merge/                   # DB 병합
│   └── util/                    # 유틸리티
├── constants/                   # 상수 모듈 (중앙 통제)
│   ├── api_mappings.py          # API 응답 키 ↔ 내부 필드명 매핑
│   ├── codes.py                 # API 엔드포인트, 지역 코드
│   ├── paths.py                 # 모든 DB 경로 중앙 정의
│   ├── schema.py                # 테이블 스키마 중앙 정의
│   └── ...
├── collector_cli.py             # ✅ 공통 CLI 진입점 (자동 등록 시스템 통합)
├── master_collectors.py         # ✅ 메뉴 기반 마스터 제어 + 중앙 대시보드
├── migrate.py                   # ✅ 통합 DB 마이그레이션 (루트)
├── docs/                        # ✅ 정적 문서 (Git 포함)
│   └── developer_guide.md       # 이 문서
├── data/                        # ✅ 런타임 데이터만 (Git 제외)
│   ├── master/
│   ├── logs/
│   └── backups/
└── requirements.txt
```

### 📌 디렉토리 역할

| 디렉토리 | 용도 | Git 관리 |
|----------|------|----------|
| `core/` | 라이브러리 모듈 (클래스, 함수) | ✅ 포함 |
| `scripts/` | 도메인별 실행 스크립트 | ✅ 포함 |
| `constants/` | 상수 (스키마, 매핑, 경로) | ✅ 포함 |
| `docs/` | 정적 문서 | ✅ 포함 |
| `data/` | 런타임 데이터 (DB, 로그) | ❌ 제외 (.gitignore) |
| `config/` | 설정 파일 | ✅ 포함 (민감정보 제외) |

---

## 2. 데이터 파이프라인 개요

모든 데이터는 다음 3 단계로 처리됩니다.

| 단계 | 설명 | DB 예시 |
|------|------|--------|
| **원시 (Raw) DB** | 수집기가 API 응답을 최소 가공하여 저장 (보강 필드는 NULL) | `neis_info.db`, `school_info.db` |
| **보강 (Enriched) DB** | 원시 DB 를 읽어 주소 정제, ID 생성 등 보강 | `neis_enriched.db`, `school_enriched.db` |
| **마스터 (Master) DB** | 여러 보강 DB 를 통합한 최종 서비스용 DB | `school_master.db` |

모든 DB 경로는 `constants/paths.py`에서 중앙 정의합니다.

---

## 3. Collector 기본 구조 (CollectorEngine v2)

```python
#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.collector_engine import CollectorEngine
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.config import config
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR
from constants.api_mappings import get_api_field

API_URL = "https://api.example.com/endpoint"

class NewCollector(CollectorEngine):
    # ----- 메타데이터 (클래스 변수) -----
    description = "새로운 수집기 설명"
    table_name = "my_table"                 # 실제 테이블명
    schema_name = "new_collector"            # schema.py 의 키 (필수!)
    api_context = "my_context"               # api_mappings.py 에서 사용할 컨텍스트
    merge_script = "scripts/merge_my_dbs.py" # 병합 스크립트 (없으면 None)
    
    # 설정 파일에서 값을 가져오되, 없으면 기본값 사용
    _cfg = config.get_collector_config("new_collector")
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    # ------------------------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False, quiet_mode=False, **kwargs):
        super().__init__("new_collector", str(MASTER_DIR), shard, school_range, 
                         quiet_mode=quiet_mode, debug_mode=debug_mode)

    def fetch_region(self, region_code: str, year: int = None, **kwargs):
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)
        
        self.print(f"📡 [{region_name}] 수집 시작 (year={year})", level="debug")
        
        params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(API_URL, params, 'rootKey', 
                                      region=region_code, year=year)
        
        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return
        
        # 배치 구성 후 한 번에 enqueue (성능 최적화)
        batch = []
        for row in rows:
            school_code = get_api_field(row, "school_code", self.api_context)
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue
            batch.append(self._transform_row(row, region_code))
        
        if batch:
            self.enqueue(batch)

    def _transform_row(self, row: dict, region_code: str) -> dict:
        return {
            "school_code": get_api_field(row, "school_code", self.api_context),
            "some_data": get_api_field(row, "some_data", self.api_context),
            "collected_at": now_kst().isoformat(),
        }

if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)
    run_collector(NewCollector, _fetch, "새로운 수집기")
```

### 3.1. CollectorEngine v2 주요 변경 사항

| 항목 | 설명 |
|------|------|
| **`schema_name` 필수** | 각 수집기는 자신의 스키마 이름을 클래스 변수로 반드시 정의 |
| **`_init_db` 자동화** | `CollectorEngine.__init__`에서 자동 테이블 생성 |
| **`_process_item` 제거** | 모든 데이터 변환은 `fetch_region` 에서 완료 후 `enqueue` |
| **통계 자동 수집** | `CollectorStats` 가 배치 저장 시마다 처리 시간, 배치 크기 자동 기록 |
| **데이터 유효성 검증** | `validate_data: true` 설정 시 저장 전 검증 수행 |
| **배치 최적화** | `enqueue` 를 배치 단위로 한 번에 호출 (성능 향상) |

### 3.2. `__init__` 주요 인자

| 인자 | 설명 | 기본값 |
|------|------|--------|
| `shard` | `"none"`, `"odd"`, `"even"` 중 하나 | `"none"` |
| `school_range` | `"A"`, `"B"`, `"none"` 또는 `None` | `None` |
| `debug_mode` | 디버그 출력 여부 | `False` |
| `quiet_mode` | 출력 최소화 여부 (CI/CD 용) | `False` |
| `**kwargs` | 추가 옵션 | - |

### 3.3. DB 경로 자동 결정

`CollectorEngine` 가 `self.db_path` 를 다음과 같이 생성합니다.

| shard | 경로 |
|-------|------|
| `"none"` | `{base_dir}/{name}.db` |
| `"odd"` | `{base_dir}/{name}_odd.db` |
| `"even"` | `{base_dir}/{name}_even.db` |
| `school_range` 있음 | `{name}_{shard}_{school_range}.db` |

※ `base_dir` 은 일반적으로 `constants.paths.MASTER_DIR`을 사용합니다.

---

## 4. 자동 등록 시스템 (Auto-Registration)

### 4.1. 개요

**`scripts/collector/` 디렉토리에 수집기 파일만 추가하면 자동으로 인식됩니다.**  
수동으로 `COLLECTOR_MAP` 에 등록할 필요가 없습니다.

### 4.2. 구현 위치: `scripts/collector/__init__.py`

```python
#!/usr/bin/env python3
# scripts/collector/__init__.py
# 수집기 자동 등록 시스템

import importlib
import pkgutil
from pathlib import Path
from typing import Dict, Type

_COLLECTOR_MAP: Dict[str, Type] = None

def get_collector_map() -> Dict[str, Type]:
    """scripts/collector/ 디렉토리에서 CollectorEngine 상속 클래스 자동 탐색"""
    global _COLLECTOR_MAP
    if _COLLECTOR_MAP is not None:
        return _COLLECTOR_MAP
    
    collector_map = {}
    collector_dir = Path(__file__).parent
    
    for module_info in pkgutil.iter_modules([str(collector_dir)]):
        if module_info.name.startswith('_'):
            continue
        
        try:
            module = importlib.import_module(f"scripts.collector.{module_info.name}")
            for name in dir(module):
                obj = getattr(module, name)
                # CollectorEngine 상속 클래스만 필터링
                if (isinstance(obj, type) and 
                    hasattr(obj, 'schema_name') and 
                    hasattr(obj, 'table_name')):
                    collector_map[module_info.name] = obj
        except Exception as e:
            print(f"⚠️ 수집기 로드 실패: {module_info.name} - {e}")
    
    _COLLECTOR_MAP = collector_map
    return collector_map

def get_registered_collectors() -> Dict[str, Type]:
    """캐시된 수집기 맵 반환"""
    return get_collector_map()

COLLECTOR_MAP = get_registered_collectors()
```

### 4.3. 새 수집기 추가 절차

| 기존 방식 | 자동 등록 시스템 |
|-----------|-----------------|
| 1. 파일 생성<br>2. `collector_cli.py` 수정<br>3. `COLLECTOR_MAP` 등록 | **1. 파일 생성**<br>✅ **끝** (자동 인식) |

```bash
# 1. scripts/collector/new_collector.py 작성
# 2. 자동 인식 확인
python master_collectors.py --list

# 3. 수집기 실행
python collector_cli.py new_collector --regions B10
```

---

## 5. 중앙 대시보드 (Central Dashboard)

### 5.1. CLI 대시보드

```bash
# 전체 수집기 현황 표시
python master_collectors.py --dashboard

# 출력 예시:
================================================================================
📊 Hi-Dunkey 중앙 대시보드
   생성시간: 2026-03-14 15:30:45
================================================================================

수집기                   레코드      크기 (MB) 마지막수정              상태      
--------------------------------------------------------------------------------
neis_info               12,450        45.32 2026-03-14 15:20       ✅ 정상    
school_info              8,230        32.18 2026-03-14 14:50       ✅ 정상    
--------------------------------------------------------------------------------
총계                    20,680        77.50 MB
================================================================================
```

### 5.2. 주요 명령어

| 명령어 | 설명 |
|--------|------|
| `python master_collectors.py --list` | 모든 수집기 목록 조회 (자동 등록) |
| `python master_collectors.py --dashboard` | 중앙 대시보드 표시 |
| `python master_collectors.py --test` | 전체 수집기 Smoke 테스트 |
| `python master_collectors.py --stats <name>` | 특정 수집기 상세 통계 |

### 5.3. 웹 대시보드 (선택)

```bash
# Flask 기반 웹 대시보드 실행
python scripts/util/dashboard_server.py
# → http://localhost:5000 접속 (60 초 자동 새로고침)
```

---

## 6. 스크립트 네이밍 및 배치 규칙

| 기능 | 디렉토리 | 파일명 예시 | 실행 명령 |
|------|---------|------------|----------|
| 원시 수집 | `collector/` | `neis_info.py` | `python -m scripts.collector.neis_info` |
| 데이터 보강 | `enrich/` | `neis_info.py` | `python -m scripts.enrich.neis_info` |
| DB 병합 | `merge/` | `neis_info.py` | `python -m scripts.merge.neis_info` |
| 유틸리티 | `util/` | `vacuum.py` | `python -m scripts.util.vacuum` |

**중요:** 모든 스크립트는 모듈 실행 (`-m`) 을 권장하며, 최상단에 프로젝트 루트를 `sys.path` 에 추가해야 합니다.

---

## 7. 설정 파일 (`config.yaml`) 사용법

### 7.1. 설정 파일 예시

```yaml
# config/config.yaml
paths:
  master_dir: "data/master"
  logs_dir: "logs"

collectors:
  new_collector:
    timeout_seconds: 3600
    validate_data: true          # ✅ 데이터 유효성 검증 활성화
    metrics_config:
      enabled: true
      collect_geo: true
```

### 7.2. 설정 값 접근

```python
from core.config import config

# 특정 collector 설정 가져오기
cfg = config.get_collector_config("new_collector")
timeout = cfg.get("timeout_seconds", 3600)

# 전체 설정에서 경로 가져오기
master_dir = config.get('paths', 'master_dir', default='data/master')
```

### 7.3. 환경변수 오버라이드

환경변수 `CONFIG__PATHS__MASTER_DIR=/custom/path` 형식으로 설정을 덮어쓸 수 있습니다.

---

## 8. DB 초기화 – 중앙 스키마 활용 (자동화)

각 수집기는 더 이상 `_init_db` 메서드를 구현하지 않습니다. `CollectorEngine` 가 `__init__`에서 자동으로 다음을 수행합니다.

```python
def _init_db(self):
    if self.schema_name:
        with get_db_connection(self.db_path) as conn:
            create_table_from_schema(conn, self.schema_name)
            self._init_db_common(conn)
```

`create_table_from_schema` 는 `constants/schema.py`에 정의된 스키마를 읽어 테이블과 인덱스를 생성합니다.

---

## 9. 데이터 수집 메서드 구현

대부분의 수집기는 `fetch_region(region_code, year=None, **kwargs)` 형태의 메서드를 구현합니다.

```python
def fetch_region(self, region_code: str, year: int = None, **kwargs):
    if year is None:
        year = get_current_school_year(now_kst())
    region_name = REGION_NAMES.get(region_code, region_code)
    
    self.print(f"📡 [{region_name}] 수집 시작 (year={year})", level="debug")
    
    params = {"ATPT_OFCDC_SC_CODE": region_code}
    rows = self._fetch_paginated(API_URL, params, 'root_key', 
                                  region=region_code, year=year)
    
    if not rows:
        self.logger.warning(f"[{region_name}] 데이터 없음")
        return
    
    # 배치 구성 후 한 번에 enqueue
    batch = []
    for row in rows:
        school_code = get_api_field(row, "school_code", self.api_context)
        if not school_code or not should_include_school(self.shard, self.school_range, school_code):
            continue
        batch.append(self._transform_row(row, region_code))
    
    if batch:
        self.enqueue(batch)
```

### 9.1. `_fetch_paginated` 사용법

`CollectorEngine`에는 `_fetch_paginated` 메서드가 내장되어 있습니다.

```python
rows = self._fetch_paginated(
    url, base_params, response_key,
    page_size=100,
    region=region_code,
    year=year
)
```

- `response_key`: API 응답 JSON 에서 데이터 배열이 있는 키 (예: `'schoolInfo'`)
- 내부적으로 페이지네이션, 재시도, 지수 백오프 자동 처리

### 9.2. 샤드/범위 필터링

```python
from core.shard import should_include_school

if not should_include_school(self.shard, self.school_range, school_code):
    continue
```

---

## 10. 데이터 변환 (`_transform_row`)

API 응답 row 를 DB 레코드 딕셔너리로 변환할 때 반드시 `get_api_field` 를 사용하여 중앙 매핑 시스템을 활용합니다.

```python
def _transform_row(self, row: dict, region_code: str) -> dict:
    now = now_kst().isoformat()
    return {
        "school_code": get_api_field(row, "school_code", self.api_context),
        "some_data": get_api_field(row, "some_data", self.api_context),
        "collected_at": now,
    }
```

`api_context`는 클래스 변수로 정의하며, `api_mappings.py`에 해당 컨텍스트가 정의되어 있어야 합니다.

---

## 11. 중앙 API 매핑 시스템

### 11.1. 매핑 파일 예시

```python
# constants/api_mappings.py
NEIS_COMMON_MAP = {
    'SD_SCHUL_CODE': 'school_code',
    'ATPT_OFCDC_SC_CODE': 'region_code',
    'SCHUL_NM': 'school_name',
}

NEIS_SCHOOL_MAP = {**NEIS_COMMON_MAP, **{
    'ENG_SCHUL_NM': 'eng_name',
    'ORG_RDNMA': 'address',
}}

NEIS_FIELD_MAP_BY_CONTEXT = {
    'common': NEIS_COMMON_MAP,
    'school': NEIS_SCHOOL_MAP,
}

def get_api_field(row: dict, field: str, context: str, default=None):
    mapping = NEIS_FIELD_MAP_BY_CONTEXT.get(context, {})
    api_key = mapping.get(field, field)
    return row.get(api_key, default)
```

### 11.2. 사용 이점

| 변경 대상 | 수정 위치 | 자동 반영 범위 |
|-----------|----------|----------------|
| API 응답 키 | `constants/api_mappings.py` | 모든 수집기 (`get_api_field` 사용 시) |
| DB 컬럼 | `constants/schema.py` + `migrate.py` | 모든 샤드 DB (자동 마이그레이션) |
| 샤딩 규칙 | `core/shard.py` | 수집, 보강, 병합 모든 스크립트 |
| 수집 로직 | `core/collector_engine.py` | 모든 수집기 (상속받으므로 자동 적용) |
| DB 경로 | `constants/paths.py` | 전 프로젝트 경로 일괄 변경 |

---

## 12. 중앙 스키마 관리 (`constants/schema.py`)

### 12.1. 스키마 정의 (최신 형식)

PRIMARY KEY 정보는 별도의 `primary_key` 리스트로 명시합니다.

```python
# constants/schema.py
SCHEMAS = {
    "school_info": {
        "table_name": "schools_info",
        "primary_key": ["school_code"],                     # ✅ 명시적 PK
        "columns": [
            ("school_code", "TEXT", ""),
            ("school_name", "TEXT", "NOT NULL"),
            ("region_code", "TEXT", "NOT NULL"),
        ],
        "indexes": [
            ("idx_schools_info_region", "atpt_ofcdc_org_code"),
        ],
    },
    "neis_info": {
        "table_name": "schools_neis",
        "primary_key": ["sc_code"],
        "columns": [...],
        "indexes": [...],
    },
}
```

### 12.2. 통합 마이그레이션 스크립트 (`migrate.py`)

```bash
# 특정 스키마에 해당하는 모든 DB 파일 마이그레이션
python migrate.py school_info

# 특정 DB 파일만 마이그레이션
python migrate.py neis_info --db data/master/neis_info_odd.db

# 대화형 모드 (인자 없이 실행)
python migrate.py
```

---

## 13. 병렬 실행 (`run_pipeline.py`)

```bash
# 사용법
python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [--regions REGIONS] [--quiet]

# 예시
python scripts/run_pipeline.py neis_info --year 2026 --regions B10,J10 --quiet
```

- `--regions`로 여러 지역을 지정하면 각 지역별로 odd/even 을 병렬 실행합니다.
- `--year`를 생략하면 프롬프트로 입력받습니다.
- collector 별 병합 스크립트는 `merge_script` 메타데이터에 정의된 것을 사용합니다.

---

## 14. Smoke 테스트

모든 변경 후에는 `python master_collectors.py --test`를 실행하여 기본적인 기능이 정상 동작하는지 확인합니다.

```bash
python master_collectors.py --test
```

이 테스트는 다음을 검증합니다:
- ✅ 수집기 로드 (자동 등록 시스템)
- ✅ DB 연결
- ✅ CLI 기본 실행
- ✅ 병렬 스크립트 실행

---

## 15. 공통 유틸리티 활용

### 15.1. 로깅

```python
self.logger.info("수집 시작")
self.logger.warning("데이터 없음")
self.logger.error("오류 발생", exc_info=True)
```

### 15.2. 배치 처리 통계 (`core/collector_stats.py`)

`CollectorEngine`는 내부적으로 `CollectorStats` 인스턴스 (`self.stats`) 를 가지고 있으며, 각 배치 저장 시 처리 시간, 배치 크기, 성공 여부를 기록합니다. 수집 종료 시 자동으로 요약 로그가 출력됩니다.

```
📊 [neis_info] 배치 처리 통계
   ⏱️  경과 시간: 125.3 초
   📦 배치 수: 47 (실패: 0, 성공률: 100.0%)
   📋 처리 행: 9,421 행
   🚀 처리 속도: 75.2 행/초
```

### 15.3. 데이터 유효성 검증 (`core/data_validator.py`)

설정에서 `validate_data: true`로 활성화하면 저장 전에 다음을 검증합니다.
- PRIMARY KEY 컬럼이 NULL 인 경우 → 오류로 처리하고 해당 배치 저장 중단
- 스키마에 정의된 컬럼이 데이터에 없는 경우 → 경고 로그 출력

### 15.4. 주소 처리 (`core.address`)

```python
from core.address import parse_region_input, get_all_regions

# 지역 코드 파싱
codes = parse_region_input("서울,경기")  # → ["B10", "J10"]

# 전체 지역 목록
regions = get_all_regions()  # → [("B10","서울"), ...]
```

### 15.5. 시간 처리

```python
from core.kst_time import now_kst
from core.school_year import get_current_school_year

now = now_kst()  # KST 현재 시간
year = get_current_school_year(now)  # 현재 학년도
```

---

## 16. 참고할 기존 Collector

| 수집기 | 위치 | 상태 |
|--------|------|------|
| NEIS 학교정보 | `scripts/collector/neis_info.py` | ✅ 최신 구조 |
| 학교알리미 | `scripts/collector/school_info.py` | ✅ 최신 구조 |

> **참고:** 신규 수집기는 `scripts/collector/` 에 작성해야 합니다.

---

## 17. 중요: 설정 파일 및 의존성 관리

### 17.1. `config.yaml` 필수 항목

각 새 수집기는 `config.yaml`의 `collectors` 섹션에 설정을 추가해야 합니다.

```yaml
collectors:
  new_collector:
    timeout_seconds: 3600
    validate_data: true   # 데이터 검증 활성화
    metrics_config:
      enabled: true
```

### 17.2. `requirements.txt` 업데이트

새로운 외부 라이브러리가 필요하면 `requirements.txt`에 추가합니다.

---

## 18. 중앙 통제 철학 구현

| 변경 대상 | 수정 위치 | 자동 반영 범위 |
|-----------|----------|----------------|
| **API 응답 키** | `constants/api_mappings.py` | 모든 수집기 |
| **DB 컬럼** | `constants/schema.py` + `migrate.py` | 모든 샤드 DB |
| **샤딩 규칙** | `core/shard.py` | 수집/보강/병합 모든 스크립트 |
| **수집 로직** | `core/collector_engine.py` | 모든 수집기 (상속) |
| **DB 경로** | `constants/paths.py` | 전 프로젝트 경로 |
| **수집기 등록** | `scripts/collector/*.py` | 파일 추가만 하면 자동 인식 |

---

## 19. 요약: 자동 등록 시스템 + 중앙 대시보드

| 기능 | 기존 방식 | 개선 후 |
|------|----------|--------|
| **수집기 등록** | 수동 `COLLECTOR_MAP` 수정 | ✅ 파일만 추가하면 자동 인식 |
| **목록 일관성** | 등록 누락 시 실행 불가 | ✅ 무조건 일치 (파일 있으면 뜸) |
| **대시보드** | 없음 | ✅ `--dashboard` 옵션으로 실시간 모니터링 |
| **통계 조회** | 제한적 | ✅ `--stats <name>` 상세 통계 |
| **중앙 통제** | 분산 관리 | ✅ `core/` + `constants/` 중앙화 |

---

## 20. 네이밍 규칙

### 20.1. 클래스명

| 유형 | 규칙 | 예시 |
|------|------|------|
| Collector | `{도메인}Collector` | `NeisInfoCollector` |
| Engine | `{기능}Engine` | `CollectorEngine` |
| Manager | `{도메인}Manager` | `SchemaManager` |
| Filter | `{도메인}Filter` | `AddressFilter` |

### 20.2. 파일명

| 유형 | 규칙 | 예시 |
|------|------|------|
| Collector | `{도메인}.py` | `neis_info.py` |
| Engine | `{기능}_engine.py` | `collector_engine.py` |
| Utility | `{기능}.py` | `manage_schema.py` |

### 20.3. 함수명

| 유형 | 규칙 | 예시 |
|------|------|------|
| Public | `snake_case` | `get_collector_map()` |
| Private | `_snake_case` | `_fetch_paginated()` |
| Boolean | `is_`, `has_`, `should_` | `should_include_school()` |

---

## 21. 수집기 이름 해석 규칙 (신규)

CLI/대시보드에서 수집기를 지정할 때는 **수집기명, 도메인명, alias를 모두 허용**합니다.

### 21.1. 허용 입력

| 입력 유형 | 예시 | 해석 결과 |
|-----------|------|-----------|
| 수집기명 | `neis_info` | `neis_info` |
| 도메인명 | `school` | `neis_info` |
| alias | `neis`, `schoolinfo` | 각 도메인의 `collector_name` |

### 21.2. 정규화 규칙

입력값은 비교 전에 아래 규칙으로 정규화합니다.

1. 앞뒤 공백 제거 (`strip`)
2. 소문자 변환 (`lower`)
3. 하이픈(`-`)을 언더스코어(`_`)로 변환

예시:

- `NEIS-INFO` -> `neis_info`
- ` SchoolInfo ` -> `schoolinfo`

### 21.3. 적용 위치

- `collector_cli.py`: 수집기 실행 이름 해석
- `master_collectors.py`: `--run`, `--stats` 인자 해석
- `constants/domains.py`: `resolve_collector_name()` 단일 진입점

### 21.4. 운영 규칙

- 새 도메인을 추가할 때는 `DOMAIN_CONFIG`에 `collector_name`을 명시합니다.
- 사용자 입력 호환이 필요하면 `aliases`에 별칭을 추가합니다.
- 이름 해석 로직은 반드시 `resolve_collector_name()`만 사용합니다.

---

이 가이드를 따라 새로운 수집기를 만들면 **자동 등록 시스템**과 **중앙 대시보드**의 혜택을 모두 받을 수 있으며, 프로젝트 전체의 일관성을 유지할 수 있습니다.

궁금한 점이 있으면 기존 코드를 참조하거나 팀 리더에게 문의하세요. 😊
