## 📘 Hi-Dunkey 개발 가이드 (최종 업데이트)

이 문서는 Hi-Dunkey 프로젝트에서 새로운 수집기(Collector) 및 데이터 파이프라인을 개발할 때 반드시 따라야 할 표준 패턴과 규칙을 설명합니다.  
모든 수집기는 `core.base_collector.BaseCollector`를 상속받아야 하며, `collector_cli.py`에 등록되어야 합니다.  
또한 `run_collector.py`를 통해 자동화된 수집 파이프라인에 통합됩니다.

---

## 1. 프로젝트 구조 개요

```
📦 프로젝트 루트
├── config/                      # 설정 파일 디렉토리
│   └── config.yaml              # 환경별 설정 (경로, 타임아웃, 병렬 설정 등)
├── core/                        # 핵심 공통 모듈
│   ├── base_collector.py        # 모든 수집기의 베이스 클래스 (v2: 자동 테이블 생성, 통계, 검증)
│   ├── config.py                # 설정 파일 로더
│   ├── database.py              # DB 연결 공통 (연결, PRAGMA, 체크포인트)
│   ├── schema_manager.py        # 스키마 기반 테이블 생성 및 저장 유틸리티
│   ├── collector_stats.py       # 배치 처리 통계 (실시간 메트릭)
│   ├── data_validator.py        # 데이터 유효성 검증
│   ├── kst_time.py              # 시간 처리
│   ├── logger.py                # 로깅
│   ├── network.py               # 네트워크 요청 (safe_json_request 등)
│   ├── retry.py                 # 실패 재시도 관리
│   ├── shard.py                 # 샤딩/범위 필터링
│   ├── meta_vocab.py            # 전역 메타 정보 관리 (주소 ID 등)
│   ├── address/                 # 주소/지역 관련 모듈
│   │   ├── __init__.py
│   │   ├── region_filter.py     # 지역 코드 파싱, 이름 변환
│   │   ├── address_filter.py    # 주소 정제, 지번 추출, 해싱
│   │   ├── geo.py               # VWorldGeocoder (저수준 API 호출)
│   │   └── sgg_code_map.py      # 시군구 코드 매핑 (선택)
│   └── ...                       # 기타 유틸리티
├── collectors/                   # (점진적 폐기 예정) 기존 수집기 구현
│   ├── neis_info_collector.py    # NEIS 학교정보 (최신 구조 반영)
│   ├── school_info_collector.py  # 학교알리미 (최신 구조 반영)
│   ├── meal_collector.py         # 급식 (구조 업데이트 예정)
│   ├── schedule_collector.py     # 학사일정 (구조 업데이트 예정)
│   ├── timetable_collector.py    # 시간표 (구조 업데이트 예정)
│   └── geo_collector.py         # 지오코딩 전용 수집기
├── scripts/                      # 실행 스크립트 (기능별 디렉토리 분리)
│   ├── collector/                # 원시 데이터 수집기
│   │   ├── __init__.py
│   │   ├── neis_info.py          # NEIS 원시 데이터 수집
│   │   └── school_info.py        # 학교알리미 원시 데이터 수집
│   ├── enrich/                   # 데이터 보강 (정제, ID 생성 등)
│   │   ├── __init__.py
│   │   ├── neis_info.py          # NEIS 데이터 보강
│   │   └── school_info.py        # 학교알리미 데이터 보강
│   ├── merge/                     # DB 병합 (샤드 병합, 최종 통합)
│   │   ├── __init__.py
│   │   ├── neis_info.py          # NEIS 샤드 병합
│   │   ├── school_info.py        # 학교알리미 샤드 병합
│   │   └── master.py              # 최종 마스터 DB 통합
│   ├── additional/                # 추가 원시 데이터 생성 (파일 등)
│   │   ├── __init__.py
│   │   └── school_info.py        # JSON → additional_school_raw.db
│   ├── util/                       # 유틸리티 스크립트
│   │   ├── __init__.py
│   │   ├── vacuum.py
│   │   └── backup_move.py
│   ├── run_pipeline.py           # 병렬 실행 + 병합
│   ├── run_collector.py          # 정기 실행 (cron용)
│   ├── retry_worker.py           # 실패 작업 재시도
│   └── ...                        # 기타 스크립트
├── constants/                    # 상수 모듈 (중앙 통제)
│   ├── api_mappings.py          # API 응답 키 ↔ 내부 필드명 매핑 (중앙 관리)
│   ├── codes.py                 # API 엔드포인트, 지역 코드 등
│   ├── paths.py                 # 모든 DB 경로 중앙 정의
│   ├── schema.py                # 테이블 스키마 중앙 정의 (primary_key 명시)
│   ├── domains.py               # 도메인별 설정 (collector, enrich, merge 매핑)
│   └── errors.py                # API 오류 코드 중앙 관리
├── collector_cli.py              # 공통 CLI 진입점 (scripts.collector 기반)
├── master_collectors.py          # 메뉴 기반 마스터 제어 (--test 옵션 포함)
├── migrate.py                    # 통합 DB 마이그레이션 스크립트 (루트)
├── requirements.txt              # 의존성 목록
└── docs/                         # 문서
    └── developer_guide.md        # 이 문서
```

---

## 2. 데이터 파이프라인 개요 (신규)

모든 데이터는 다음 3단계로 처리됩니다.

| 단계 | 설명 | DB 예시 |
|------|------|---------|
| **원시(Raw) DB** | 수집기가 API 응답을 최소 가공하여 저장 (보강 필드는 `NULL`) | `neis_info.db`, `school_info.db` |
| **보강(Enriched) DB** | 원시 DB를 읽어 주소 정제, ID 생성 등 보강 | `neis_enriched.db`, `school_enriched.db` |
| **마스터(Master) DB** | 여러 보강 DB를 통합한 최종 서비스용 DB | `school_master.db` |

**모든 DB 경로는 `constants/paths.py`에서 중앙 정의합니다.**  
새로운 도메인을 추가할 때는 위 단계에 따라 스크립트를 `scripts/` 아래 적절한 디렉토리에 생성해야 합니다.

---

## 3. Collector 기본 구조 (BaseCollector v2)

```python
#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (모든 로컬 임포트보다 먼저!)
sys.path.append(str(Path(__file__).parent.parent))

from typing import List, Dict, Optional

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.config import config
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR
from constants.api_mappings import get_api_field   # ✅ 중앙 매핑 함수

API_URL = "https://api.example.com/endpoint"

class NewCollector(BaseCollector):
    # ----- 메타데이터 (클래스 변수) -----
    description = "새로운 수집기 설명"
    table_name = "my_table"                 # 실제 테이블명 (schema.py의 table_name과 일치)
    schema_name = "new_collector"            # schema.py의 키 (필수!)
    api_context = "my_context"               # api_mappings.py에서 사용할 컨텍스트
    merge_script = "scripts/merge_my_dbs.py" # 병합 스크립트 (없으면 None)

    # 설정 파일에서 값을 가져오되, 없으면 기본값 사용
    _cfg = config.get_collector_config("new_collector")
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 1800)

    parallel_script = _cfg.get("parallel_script", "scripts/run_pipeline.py")
    modes = _cfg.get("modes", ["통합", "odd 샤드", "even 샤드", "병렬 실행"])
    metrics_config = _cfg.get("metrics_config", {"enabled": False})
    parallel_config = _cfg.get("parallel_config", {})
    # ------------------------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False, quiet_mode=False, **kwargs):
        super().__init__("new_collector", str(MASTER_DIR), shard, school_range, quiet_mode=quiet_mode)
        self.debug_mode = debug_mode
        # _init_db()는 BaseCollector.__init__에서 자동 호출되므로 직접 호출하지 않음

    def fetch_region(self, region_code: str, year: int = None, date: str = None, **kwargs):
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)

        self.print(f"📡 [{region_name}] 수집 시작 (year={year})", level="debug")

        params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(API_URL, params, "rootKey", region=region_code, year=year)

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return

        for row in rows:
            school_code = get_api_field(row, "school_code", self.api_context)
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue
            # ✅ fetch_region에서 직접 변환 후 enqueue
            self.enqueue([self._transform_row(row, region_code)])

    def _transform_row(self, row: dict, region_code: str) -> dict:
        return {
            "school_code": get_api_field(row, "school_code", self.api_context),
            "some_data": get_api_field(row, "some_data", self.api_context),
            "collected_at": now_kst().isoformat(),
        }

    # _do_save_batch는 BaseCollector의 기본 구현(schema_manager.save_batch)을 사용하므로
    # 특별한 이유가 없으면 오버라이드하지 않음.
    # 만약 커스텀 저장이 필요하면 오버라이드.

if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)
    run_collector(NewCollector, _fetch, "새로운 수집기")
```

### 3.1. BaseCollector v2 주요 변경 사항
- **`schema_name` 필수**: 각 수집기는 자신의 스키마 이름을 클래스 변수로 반드시 정의해야 합니다.
- **`_init_db` 자동화**: `BaseCollector.__init__`에서 `schema_name`을 기반으로 테이블을 자동 생성하므로, 하위 클래스에서 `_init_db`를 구현할 필요가 없습니다.
- **`_process_item` 제거**: 더 이상 사용하지 않습니다. 모든 데이터 변환은 `fetch_region`에서 완료한 후 `enqueue`해야 합니다.
- **통계 자동 수집**: `BaseCollector`에 내장된 `CollectorStats`가 배치 저장 시마다 처리 시간, 배치 크기 등을 자동 기록하며, 종료 시 요약 로그를 출력합니다.
- **데이터 유효성 검증**: 설정에서 `validate_data: true`로 지정하면, 저장 전에 `DataValidator`를 통해 기본 검증(PK null, 컬럼 누락 등)을 수행합니다.

### 3.2. `__init__` 주요 인자
- `shard`: `"none"`, `"odd"`, `"even"` 중 하나 (기본값 `"none"`)
- `school_range`: `"A"`, `"B"`, `"none"` 또는 `None` (범위 필터)
- `debug_mode`: 디버그 출력 여부
- `quiet_mode`: 출력 최소화 여부 (GitHub Actions 등에서 사용)
- `**kwargs`: 추가 옵션 (예: `incremental`, `full`, `compare`)

### 3.3. DB 경로 자동 결정
`BaseCollector`가 `self.db_path`를 다음과 같이 생성합니다.
- `shard="none"` → `{base_dir}/{name}.db`
- `shard="odd"` → `{base_dir}/{name}_odd.db`
- `shard="even"` → `{base_dir}/{name}_even.db`
- `school_range`가 있으면 `{name}_{shard}_{school_range}.db`

※ `base_dir`은 일반적으로 `constants.paths.MASTER_DIR`을 사용합니다.

---

## 4. 스크립트 네이밍 및 배치 규칙 (신규)

모든 실행 스크립트는 `scripts/` 아래 **기능별 디렉토리**에 배치하며, 파일명은 `{도메인}.py`로 합니다.

| 기능 | 디렉토리 | 파일명 예시 | 실행 명령 |
|------|----------|------------|----------|
| 원시 수집 | `collector/` | `neis_info.py` | `python -m scripts.collector.neis_info` |
| 데이터 보강 | `enrich/` | `neis_info.py` | `python -m scripts.enrich.neis_info` |
| DB 병합 | `merge/` | `neis_info.py`, `master.py` | `python -m scripts.merge.master` |
| 추가 원시 데이터 생성 | `additional/` | `school_info.py` | `python -m scripts.additional.school_info` |
| 유틸리티 | `util/` | `vacuum.py` | `python -m scripts.util.vacuum` |

**중요**: 모든 스크립트는 모듈 실행(`-m`)을 권장하며, 최상단에 프로젝트 루트를 `sys.path`에 추가해야 합니다.

---

## 5. 설정 파일 (`config.yaml`) 사용법

모든 환경별 설정은 `config/config.yaml`에서 관리합니다.  
설정은 `core.config.Config` 싱글톤을 통해 로드되며, 환경변수로 오버라이드할 수 있습니다.

### 5.1. 설정 파일 예시
```yaml
# config/config.yaml
paths:
  master_dir: "data/master"
  active_dir: "data/active"
  logs_dir: "logs"

collectors:
  new_collector:
    timeout_seconds: 3600
    parallel_timeout_seconds: 7200
    merge_timeout_seconds: 1800
    max_workers: 4
    validate_data: true          # ✅ 데이터 유효성 검증 활성화
    metrics_config:
      enabled: true
  geo:
    daily_api_limit: 50000
    vworld_api_key_env: "VWORLD_API_KEY"
    kakao_api_key_env: "KAKAO_API_KEY"

api:
  neis_api_key_env: "NEIS_API_KEY"
  vworld_api_key_env: "VWORLD_API_KEY"
  school_info_api_key_env: "SCHOOL_INFO_API_KEY"
  kakao_api_key_env: "KAKAO_API_KEY"
```

### 5.2. 설정 값 접근
```python
from core.config import config

# 특정 collector 설정 가져오기
cfg = config.get_collector_config("new_collector")
timeout = cfg.get("timeout_seconds", 3600)

# 전체 설정에서 경로 가져오기
master_dir = config.get('paths', 'master_dir', default='data/master')
```

### 5.3. 환경변수 오버라이드
환경변수 `CONFIG__PATHS__MASTER_DIR=/custom/path` 형식으로 설정을 덮어쓸 수 있습니다.

---

## 6. DB 초기화 – 중앙 스키마 활용 (자동화)

각 수집기는 더 이상 `_init_db` 메서드를 구현하지 않습니다.  
`BaseCollector`가 `__init__`에서 자동으로 다음을 수행합니다.

```python
def _init_db(self):
    if self.schema_name:
        with get_db_connection(self.db_path) as conn:
            create_table_from_schema(conn, self.schema_name)
            self._init_db_common(conn)
```

- `create_table_from_schema`는 `constants/schema.py`에 정의된 스키마를 읽어 테이블과 인덱스를 생성합니다.
- `_init_db_common`은 체크포인트 테이블을 생성합니다.

따라서 새 수집기는 `schema_name`만 올바르게 설정하면 됩니다.

---

## 7. 데이터 수집 메서드 구현

대부분의 수집기는 `fetch_region(region_code, year=None, date=None, **kwargs)` 형태의 메서드를 구현합니다.  
급식처럼 일별 수집이 필요한 경우 `fetch_daily`를 구현할 수 있습니다.

```python
def fetch_region(self, region_code: str, year: int = None, date: str = None, **kwargs):
    if year is None:
        year = get_current_school_year(now_kst())
    region_name = REGION_NAMES.get(region_code, region_code)

    self.print(f"📡 [{region_name}] 수집 시작 (year={year})", level="debug")

    params = {"ATPT_OFCDC_SC_CODE": region_code}
    rows = self._fetch_paginated(API_URL, params, 'root_key', region=region_code, year=year)

    if not rows:
        self.logger.warning(f"[{region_name}] 데이터 없음")
        return

    for row in rows:
        school_code = get_api_field(row, "school_code", self.api_context)
        if not school_code or not should_include_school(self.shard, self.school_range, school_code):
            continue
        self.enqueue([self._transform_row(row, region_code)])
```

### 7.1. `_fetch_paginated` 사용법
`BaseCollector`에는 `_fetch_paginated` 메서드가 내장되어 있습니다.  
이 메서드는 `core.network.safe_json_request`를 사용하며, 자동으로 페이지를 순회하고 재시도합니다.  
재시도 시 지터(jitter)가 포함된 지수 백오프를 적용하여 네트워크 부하를 분산합니다.

```python
rows = self._fetch_paginated(
    url, base_params, response_key,
    page_size=100,
    region=region_code,
    year=year
)
```

- `response_key`: API 응답 JSON에서 데이터 배열이 있는 키 (예: `'schoolInfo'`, `'mealServiceDietInfo'`)
- 내부적으로 `AY` 파라미터에 `year`를 자동 추가합니다.

### 7.2. 샤드/범위 필터링
`should_include_school(self.shard, self.school_range, school_code)`를 사용하여 학교 코드가 현재 샤드와 범위에 속하는지 확인합니다.

---

## 8. 데이터 변환 (`_transform_row`)

API 응답 row를 DB 레코드 딕셔너리로 변환할 때 **반드시 `get_api_field`를 사용**하여 중앙 매핑 시스템을 활용합니다.

```python
def _transform_row(self, row: dict, region_code: str) -> dict:
    now = now_kst().isoformat()
    return {
        "school_code": get_api_field(row, "school_code", self.api_context),
        "some_data": get_api_field(row, "some_data", self.api_context),
        "collected_at": now,
        # 필요한 모든 필드를 동일한 방식으로 추가
    }
```

- `api_context`는 클래스 변수로 정의하며, `api_mappings.py`에 해당 컨텍스트가 정의되어 있어야 합니다.

---

## 9. 배치 저장 (`_do_save_batch`)

`BaseCollector`의 기본 `_do_save_batch`는 `schema_manager.save_batch`를 호출하여 자동 저장합니다.  
또한 설정에 따라 데이터 유효성 검증을 수행하고, 통계를 수집합니다.

```python
def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]) -> None:
    schema = SCHEMAS[self.schema_name]
    columns = [col[0].strip() for col in schema["columns"]]
    pk_columns = schema.get("primary_key", [])

    if self.validate_data:
        valid, errors, warnings = DataValidator.validate_batch(batch, columns, pk_columns)
        if not valid:
            self.logger.error(f"데이터 검증 실패: {errors}")
            return

    save_batch(conn, self.table_name, columns, batch)
```

- `validate_data`는 `config.yaml`에서 설정합니다.
- `DataValidator`는 PRIMARY KEY null, 컬럼 누락 등을 검사합니다.

---

## 10. 추가 메서드 구현 (필요시)

- `iterate_schools`, `iterate_schools_by_month` 등이 필요한 경우 직접 구현합니다. (예: 급식, 학사일정 등)
- **주의**: `_process_item`은 더 이상 사용되지 않으므로 구현하지 마세요.

---

## 11. `collector_cli.py`에 등록

### 11.1. Collector 클래스 임포트
`collector_cli.py` 상단에 새 수집기 클래스를 임포트합니다. (신규 구조에서는 `scripts.collector`에서 임포트)

```python
from scripts.collector import neis_info, school_info

COLLECTOR_MAP = {
    "neis_info": neis_info.NeisInfoCollector,
    "school_info": school_info.SchoolInfoCollector,
}
```

### 11.2. `COLLECTOR_MAP`에 추가
```python
COLLECTOR_MAP = {
    # ... 기존 항목
    "new_collector": NewCollector,
}
```

이제 `collector_cli.py`에서 `new_collector`를 인자로 실행할 수 있습니다.

---

## 12. `master_collectors.py`에서의 동작

`master_collectors.py`는 `collector_cli.py`의 `COLLECTOR_MAP`을 임포트하여 모든 수집기 목록을 동적으로 가져옵니다.  
각 collector 클래스에 정의된 메타데이터(설명, 테이블명, 병합 스크립트, 타임아웃 등)를 읽어와 메뉴를 구성하고 실행합니다.

- 새 수집기를 추가하면 `COLLECTOR_MAP`에 등록하는 것만으로 `master_collectors.py`에 자동 반영됩니다.
- 실행은 항상 `collector_cli.py`를 통해 이루어집니다.

### 12.1. Smoke 테스트 실행
`master_collectors.py`는 `--test` 옵션을 지원합니다.  
```bash
python master_collectors.py --test
```
이 명령은 다음을 테스트합니다:
- 수집기 로드
- DB 연결
- CLI 기본 실행
- 병렬 스크립트 실행

---

## 13. 병렬 실행 (`run_pipeline.py`)

`run_pipeline.py`는 하나의 collector에 대해 odd/even 샤드를 병렬로 실행하고, 선택적으로 병합까지 수행하는 통합 스크립트입니다.  
사용법:
```bash
python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [--regions REGIONS] [--quiet] [추가 인자...]
```

- `--regions`로 여러 지역을 지정하면 각 지역별로 odd/even을 병렬 실행합니다.
- `--year`를 생략하면 프롬프트로 입력받습니다.
- `--quiet` 옵션으로 진행률 표시를 끌 수 있습니다 (GitHub Actions 등에서 사용).
- collector별 병합 스크립트는 `merge_script` 메타데이터에 정의된 것을 사용합니다.

### 13.1. `rich` 라이브러리를 이용한 멀티바 진행률
`rich`가 설치되어 있으면 각 지역-샤드 작업의 진행 상황을 실시간으로 보여주는 멀티바가 표시됩니다.  
설치되지 않은 경우 단순 텍스트 진행률로 폴백됩니다.

---

## 14. 정기 실행 (`run_collector.py`)

`run_collector.py`(구 run_daily.py)는 cron 등에서 정기적으로 실행되는 스크립트입니다.  
내부적으로 `collector_cli.py`를 호출하여 필요한 수집기를 순차 실행합니다.  
새 수집기를 정기 실행에 추가하려면 `run_collector.py`에 해당 함수를 추가하고 `main()`에서 호출합니다.

```python
def run_new_collector():
    run_collector("new_collector", ["--regions", "ALL", "--year", str(now_kst().year)], "새 수집기")
```

---

## 15. 공통 유틸리티 활용

### 15.1. API 키 및 환경변수
- `constants.codes`에 정의된 API 키 상수를 사용합니다. (대부분 환경변수에서 로드)
- 설정 파일(`config.yaml`)의 `api` 섹션에서 환경변수명을 지정할 수 있습니다.
- `.env` 파일은 `core/__init__.py`에서 자동 로드됩니다.

### 15.2. 중앙 API 매핑 시스템 (`constants/api_mappings.py`)
모든 수집기는 API 응답 키를 내부 필드명으로 변환할 때 **`get_api_field` 함수**를 사용해야 합니다.  
이를 통해 API 키 변경 시 매핑 파일만 수정하면 모든 수집기에 일괄 적용됩니다.

**매핑 파일 예시:**
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
    # ...
}}

SCHOOL_INFO_MAP = {
    'SCHUL_CODE': 'school_code',
    'SCHUL_NM': 'school_name',
    # ...
}

NEIS_FIELD_MAP_BY_CONTEXT = {
    'common': NEIS_COMMON_MAP,
    'school': NEIS_SCHOOL_MAP,
    'school_info': SCHOOL_INFO_MAP,
    # ...
}
```

### 15.3. 에러 처리 및 재시도
- `RetryManager`를 사용하여 실패한 작업을 기록하고 재시도할 수 있습니다.
- `self.retry_mgr.record_failure(...)`로 실패를 기록하고, `scripts/retry_worker.py`가 처리합니다.

### 15.4. 로깅
- `self.logger`를 사용하여 로그를 남깁니다. (자동으로 파일과 콘솔에 출력)

### 15.5. 메트릭 생성 (`core/metrics.py`)
- `metrics.py`는 수집 완료 후 전체 DB 현황(레코드 수, 파일 크기, 좌표 확보율 등)을 요약하는 메트릭을 생성합니다.
- `master_collectors.py`에서 `--metrics` 옵션으로 실행할 수 있습니다.

### 15.6. 배치 처리 통계 (`core/collector_stats.py`)
`BaseCollector`는 내부적으로 `CollectorStats` 인스턴스(`self.stats`)를 가지고 있으며, 각 배치 저장 시 처리 시간, 배치 크기, 성공 여부를 기록합니다. 수집 종료 시 자동으로 요약 로그가 출력됩니다.

```python
# 로그 예시
📊 [neis_info] 배치 처리 통계
   ⏱️  경과 시간: 125.3초
   📦 배치 수: 47 (실패: 0, 성공률: 100.0%)
   📋 처리 행: 9,421행
   📏 평균 배치 크기: 200.45 (최소 150, 최대 487)
   ⏱️  평균 배치 시간: 0.123초 (최소 0.045초, 최대 0.345초)
   🚀 처리 속도: 75.2행/초
```

### 15.7. 데이터 유효성 검증 (`core/data_validator.py`)
설정에서 `validate_data: true`로 활성화하면 저장 전에 다음을 검증합니다.
- PRIMARY KEY 컬럼이 NULL인 경우 → 오류로 처리하고 해당 배치 저장 중단
- 스키마에 정의된 컬럼이 데이터에 없는 경우 → 경고 로그 출력 (NULL 저장됨)

### 15.8. Vocab 관리
- `MetaVocabManager`를 사용하여 정규화된 주소 ID 등을 생성하고 관리할 수 있습니다.

### 15.9. 주소 처리 (`core.address`)
주소 관련 기능은 `core.address` 패키지에 통합되어 있습니다.
- `region_filter.py`: 지역 코드 파싱, 이름 변환
  - `parse_region_input("서울,경기")` → `["B10", "J10"]`
  - `get_region_name("B10")` → `"서울"`
  - `get_all_regions()` → `[("B10","서울"), ...]`
- `address_filter.py`: 주소 정제, 지번 추출, 해싱
  - `AddressFilter.clean(address, level)`
  - `AddressFilter.extract_jibun(address)`
  - `AddressFilter.hash(address)`
- `geo.py`: VWorldGeocoder (저수준 API 호출)

### 15.10. 지오코딩 분리 (`geo_collector`)
좌표가 필요한 수집기(예: `neis_info_collector`)는 주소 정제까지만 수행하고, 실제 지오코딩은 `geo_collector`가 담당합니다.
- `geo_collector.batch_update_schools(limit=500)`를 주기적으로 실행하여 좌표가 없는 학교들을 업데이트합니다.
- 실패한 지오코딩 작업은 `failures.db`에 저장되고 `retry_worker`가 재시도합니다.

### 15.11. ID 생성
- `school_id.create_school_id()`로 교육청 코드+학교 코드 → 32비트 정수 ID 생성.

### 15.12. 시간 처리
- `core.kst_time.now_kst()`로 KST 현재 시간 획득.
- `core.school_year.get_current_school_year()`로 현재 학년도 계산.

### 15.13. API 오류 코드 처리 (`constants/errors.py`)
API 호출 시 발생할 수 있는 오류 코드는 `constants/errors.py`에 중앙 관리합니다.
```python
# constants/errors.py
NEIS_ERRORS = {
    "ERROR-337": "일별 트래픽 제한 초과",
    "ERROR-500": "서버 오류",
    # ...
}
```
`core.network.safe_json_request`에서 이 상수를 활용하여 세밀한 예외 처리를 구현할 수 있습니다.

---

## 16. 중앙 스키마 관리 (`constants/schema.py`)와 통합 마이그레이션

### 16.1. 스키마 정의 (최신 형식)
PRIMARY KEY 정보는 더 이상 컬럼 제약조건에 포함하지 않고, 별도의 `primary_key` 리스트로 명시합니다.

```python
# constants/schema.py
SCHEMAS = {
    "school_info": {
        "table_name": "schools_info",
        "primary_key": ["school_code"],                     # ✅ 명시적 PK
        "columns": [
            ("school_code", "TEXT", ""),                    # PRIMARY KEY 제거
            ("school_name", "TEXT", "NOT NULL"),
            ("region_code", "TEXT", "NOT NULL"),
            # ... 모든 컬럼
        ],
        "indexes": [
            ("idx_schools_info_region", "atpt_ofcdc_org_code"),
            ("idx_schools_info_type", "schul_knd_sc_code"),
        ],
    },
    "neis_info": {
        "table_name": "schools_neis",
        "primary_key": ["sc_code"],
        "columns": [ ... ],
        "indexes": [ ... ],
    },
    # 다른 수집기도 동일한 방식으로 추가
}
```

- **장점**: PRIMARY KEY 정보가 명확해지고, 문자열 파싱 의존성이 사라집니다.
- **향후 확장**: UNIQUE, FOREIGN KEY 등 다른 메타정보도 동일한 방식으로 추가 가능합니다.

### 16.2. 통합 마이그레이션 스크립트 (`migrate.py`)
루트 디렉토리의 `migrate.py`는 `schema.py`를 읽어 누락된 컬럼을 추가하고 인덱스를 생성합니다.

```bash
# 특정 스키마에 해당하는 모든 DB 파일 마이그레이션
python migrate.py school_info

# 특정 DB 파일만 마이그레이션
python migrate.py neis_info --db data/master/neis_info_odd.db

# 대화형 모드 (인자 없이 실행)
python migrate.py
```

스크립트는 `MASTER_DIR` 내에서 `{schema}*.db` 패턴의 파일을 찾아 순회하며 안전하게 컬럼을 추가합니다.  
이미 존재하는 컬럼은 건너뛰므로 여러 번 실행해도 문제없습니다.

### 16.3. 새 컬럼 추가 시
1. `constants/schema.py`에 컬럼 정의를 추가합니다.
2. `migrate.py`를 실행하여 모든 관련 DB에 컬럼을 추가합니다.
3. 필요시 `api_mappings.py`에도 새 내부 필드명과 API 키 매핑을 추가합니다.

이 방식으로 각 수집기별 개별 마이그레이션 스크립트를 유지할 필요가 없어집니다.

---

## 17. 참고할 기존 Collector

- **NEIS 학교정보** (`collectors/neis_info_collector.py`): 최신 구조 반영, `schema_name="neis_info"`, `_init_db` 없음, `_process_item` 없음, 지오코딩 분리, `MetaVocabManager` 사용 예.
- **학교알리미** (`collectors/school_info_collector.py`): 최신 구조 반영, `schema_name="school_info"`, `_init_db` 없음, `_process_item` 없음.
- **급식** (`collectors/meal_collector.py`): 일별 수집, `fetch_daily`, `BaseMealCollector` 상속 (향후 구조 업데이트 필요)
- **학사일정** (`collectors/schedule_collector.py`): 연간 수집, `fetch_year` (향후 구조 업데이트 필요)
- **시간표** (`collectors/timetable_collector.py`): 추가 옵션(`--semester`), `fetch_year` (향후 구조 업데이트 필요)
- **지오코딩** (`collectors/geo_collector.py`): 배치 지오코딩, API 사용량 관리, 캐싱

**참고**: 신규 수집기는 `collectors/` 대신 `scripts/collector/`에 작성해야 합니다. 기존 `collectors/` 디렉토리는 점진적으로 폐기될 예정입니다.

---

## 18. 중요: 설정 파일 및 의존성 관리

### 18.1. `config.yaml` 필수 항목
각 새 수집기는 `config.yaml`의 `collectors` 섹션에 설정을 추가해야 합니다.  
최소한 다음 항목을 포함하는 것이 좋습니다:
```yaml
collectors:
  new_collector:
    timeout_seconds: 3600
    parallel_timeout_seconds: 7200
    merge_timeout_seconds: 1800
    max_workers: 4
    validate_data: true   # 데이터 검증 활성화
    metrics_config:
      enabled: true
```

### 18.2. `requirements.txt` 업데이트
새로운 외부 라이브러리가 필요하면 `requirements.txt`에 추가합니다.

---

## 19. Smoke 테스트

모든 변경 후에는 `python master_collectors.py --test`를 실행하여 기본적인 기능이 정상 동작하는지 확인합니다.  
이 테스트는 다음을 검증합니다:

- 수집기 로드
- DB 연결
- CLI 실행
- 병렬 스크립트 실행

테스트가 통과하면 프로젝트가 정상 상태임을 의미합니다.

---

이 가이드를 따라 새로운 수집기를 만들면 `BaseCollector`의 최신 기능(자동 테이블 생성, 통계 수집, 데이터 검증)을 모두 활용할 수 있으며, 프로젝트 전체의 일관성을 유지할 수 있습니다.  
또한 `scripts/` 아래 기능별 디렉토리 구조를 통해 원시/보강/마스터 단계를 명확히 분리하여 유지보수성을 높일 수 있습니다.

궁금한 점이 있으면 기존 코드를 참조하거나 팀 리더에게 문의하세요.
