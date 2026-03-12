```markdown
## 📘 Hi-Dunkey Collector 개발 가이드 (최종 업데이트)

이 문서는 Hi-Dunkey 프로젝트에서 새로운 수집기(Collector)를 개발할 때 반드시 따라야 할 표준 패턴과 규칙을 설명합니다.  
모든 수집기는 `core.base_collector.BaseCollector`를 상속받아야 하며, `collector_cli.py`에 등록되어야 합니다.  
또한 `run_collector.py`를 통해 자동화된 수집 파이프라인에 통합됩니다.

---

## 1. 프로젝트 구조 개요

```
📦 프로젝트 루트
├── config/                      # 설정 파일 디렉토리
│   └── config.yaml              # 환경별 설정 (경로, 타임아웃, 병렬 설정 등)
├── core/                        # 핵심 공통 모듈
│   ├── base_collector.py        # 모든 수집기의 베이스 클래스
│   ├── config.py                # 설정 파일 로더
│   ├── database.py              # DB 연결 공통 (연결, PRAGMA, 체크포인트)
│   ├── schema_manager.py        # 스키마 기반 테이블 생성 및 저장 유틸리티
│   ├── kst_time.py              # 시간 처리
│   ├── logger.py                # 로깅
│   ├── network.py               # 네트워크 요청 (safe_json_request 등)
│   ├── retry.py                 # 실패 재시도 관리
│   ├── shard.py                 # 샤딩/범위 필터링
│   ├── address/                 # 주소/지역 관련 모듈
│   │   ├── __init__.py
│   │   ├── region_filter.py     # 지역 코드 파싱, 이름 변환
│   │   ├── address_filter.py    # 주소 정제, 지번 추출, 해싱
│   │   ├── geo.py               # VWorldGeocoder (저수준 API 호출)
│   │   └── sgg_code_map.py      # 시군구 코드 매핑 (선택)
│   └── ...                       # 기타 유틸리티
├── collectors/                   # 개별 수집기 구현
│   ├── neis_info_collector.py
│   ├── school_info_collector.py
│   ├── meal_collector.py
│   ├── schedule_collector.py
│   ├── timetable_collector.py
│   └── geo_collector.py         # 지오코딩 전용 수집기
├── constants/                    # 상수 모듈
│   ├── api_mappings.py          # API 응답 키 ↔ 내부 필드명 매핑 (중앙 관리)
│   ├── codes.py                 # API 엔드포인트, 지역 코드 등
│   ├── paths.py                 # 경로 상수 (설정 기반)
│   ├── schema.py                # 테이블 스키마 중앙 정의
│   └── errors.py                # API 오류 코드 중앙 관리
├── scripts/                      # 실행 스크립트
│   ├── run_pipeline.py           # 병렬 실행 + 병합
│   ├── run_collector.py          # 정기 실행 (cron용)
│   ├── retry_worker.py           # 실패 작업 재시도
│   └── ...                        # 기존 마이그레이션 스크립트는 migrate.py로 통합됨
├── collector_cli.py              # 공통 CLI 진입점
├── master_collectors.py          # 메뉴 기반 마스터 제어 (--test 옵션 포함)
├── migrate.py                    # 통합 DB 마이그레이션 스크립트 (루트)
├── requirements.txt              # 의존성 목록
└── docs/                         # 문서
    └── developer_guide.md        # 이 문서
```

---

## 2. Collector 기본 구조

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
from core.schema_manager import create_table_from_schema   # ✅ 스키마 기반 테이블 생성
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
    schema_name = "new_collector"            # schema.py의 키 (create_table_from_schema에 전달)
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
        self._init_db()

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            # constants/schema.py에 정의된 스키마를 사용하여 테이블 생성
            create_table_from_schema(conn, self.schema_name)   # 스키마 이름 전달
            self._init_db_common(conn)

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
            self.enqueue([self._transform_row(row, region_code)])

    def _transform_row(self, row: dict, region_code: str) -> dict:
        return {
            "school_code": get_api_field(row, "school_code", self.api_context),
            "some_data": get_api_field(row, "some_data", self.api_context),
            "collected_at": now_kst().isoformat(),
        }

    # _do_save_batch는 schema_manager.save_batch를 사용할 경우 오버라이드하지 않아도 됨.
    # 기본 저장 로직은 BaseCollector에서 schema_name을 이용해 처리할 수 있습니다.
    # 만약 커스텀 저장이 필요하면 아래와 같이 오버라이드합니다.
    # def _do_save_batch(self, conn, batch):
    #     sql = "INSERT OR REPLACE INTO my_table (school_code, some_data, collected_at) VALUES (?, ?, ?)"
    #     rows = [(r["school_code"], r["some_data"], r["collected_at"]) for r in batch]
    #     conn.executemany(sql, rows)

if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)
    run_collector(NewCollector, _fetch, "새로운 수집기")
```

### 2.1. `__init__` 주요 인자
- `shard`: `"none"`, `"odd"`, `"even"` 중 하나 (기본값 `"none"`)
- `school_range`: `"A"`, `"B"`, `"none"` 또는 `None` (범위 필터)
- `debug_mode`: 디버그 출력 여부
- `quiet_mode`: 출력 최소화 여부 (GitHub Actions 등에서 사용)
- `**kwargs`: 추가 옵션 (예: `incremental`, `full`, `compare`)

### 2.2. DB 경로 자동 결정
`BaseCollector`가 `self.db_path`를 다음과 같이 생성합니다.
- `shard="none"` → `{base_dir}/{name}.db`
- `shard="odd"` → `{base_dir}/{name}_odd.db`
- `shard="even"` → `{base_dir}/{name}_even.db`
- `school_range`가 있으면 `{name}_{shard}_{school_range}.db`

※ `base_dir`은 일반적으로 `constants.paths.MASTER_DIR`을 사용합니다.

---

## 3. 설정 파일 (`config.yaml`) 사용법

모든 환경별 설정은 `config/config.yaml`에서 관리합니다.  
설정은 `core.config.Config` 싱글톤을 통해 로드되며, 환경변수로 오버라이드할 수 있습니다.

### 3.1. 설정 파일 예시
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
    cpu_factor: 1.0
    max_by_api: 10
    absolute_max: 16
    metrics_config:
      enabled: true
      collect_geo: false
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

### 3.2. 설정 값 접근
```python
from core.config import config

# 특정 collector 설정 가져오기
cfg = config.get_collector_config("new_collector")
timeout = cfg.get("timeout_seconds", 3600)

# 전체 설정에서 경로 가져오기
master_dir = config.get('paths', 'master_dir', default='data/master')
```

### 3.3. 환경변수 오버라이드
환경변수 `CONFIG__PATHS__MASTER_DIR=/custom/path` 형식으로 설정을 덮어쓸 수 있습니다.

---

## 4. DB 초기화 (`_init_db`) – 중앙 스키마 활용

각 수집기는 더 이상 하드코딩된 `CREATE TABLE` 문을 사용하지 않고, **`core.schema_manager.create_table_from_schema`** 함수를 호출하여 테이블을 생성합니다.  
이 함수는 `constants/schema.py`에 정의된 스키마를 읽어 테이블과 인덱스를 생성합니다.

```python
from core.database import get_db_connection
from core.schema_manager import create_table_from_schema

def _init_db(self):
    with get_db_connection(self.db_path) as conn:
        create_table_from_schema(conn, self.schema_name)   # 스키마 이름 (constants/schema.py의 키)
        self._init_db_common(conn)
```

- `get_db_connection`은 WAL 모드, 적절한 PRAGMA 설정을 자동으로 적용합니다.
- `_init_db_common(conn)`을 호출하면 `collection_checkpoint` 테이블이 생성됩니다 (선택 사항).

---

## 5. 데이터 수집 메서드 구현

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

### 5.1. `_fetch_paginated` 사용법
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

### 5.2. 샤드/범위 필터링
`should_include_school(self.shard, self.school_range, school_code)`를 사용하여 학교 코드가 현재 샤드와 범위에 속하는지 확인합니다.

---

## 6. 데이터 변환 (`_transform_row`)

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

## 7. 배치 저장 (`_do_save_batch`)

`BaseCollector`는 내부 writer 스레드가 일정 크기마다 `_do_save_batch`를 호출합니다.  
**기본 저장 로직**을 사용하려면 `_do_save_batch`를 오버라이드하지 않아도 됩니다.  
`BaseCollector`는 `schema_name`을 통해 `core.schema_manager.save_batch` 함수를 호출하여 자동으로 저장합니다.  
만약 커스텀 저장 로직이 필요하다면 아래와 같이 오버라이드합니다.

```python
def _do_save_batch(self, conn, batch):
    sql = """
        INSERT OR REPLACE INTO my_table (school_code, some_data, collected_at)
        VALUES (?, ?, ?)
    """
    rows = [(r["school_code"], r["some_data"], r["collected_at"]) for r in batch]
    conn.executemany(sql, rows)
```

- `conn`은 `get_db_connection`으로 얻은 SQLite 연결 객체입니다.
- `batch`는 `_transform_row`에서 반환된 딕셔너리들의 리스트입니다.

---

## 8. 추가 메서드 구현 (필요시)

- `_process_item(self, raw_item)`: 단일 API row를 처리하여 enqueue할 아이템 리스트 반환.  
  기본적으로 `_transform_row`를 호출하는 간단한 구현이면 충분하지만, 복잡한 파싱이 필요하면 이 메서드를 오버라이드할 수 있습니다.
- `iterate_schools`, `iterate_schools_by_month` 등이 필요한 경우 직접 구현합니다. (예: 급식, 학사일정 등)

---

## 9. `collector_cli.py`에 등록

### 9.1. Collector 클래스 임포트
`collector_cli.py` 상단에 새 수집기 클래스를 임포트합니다.

```python
from collectors.new_collector import NewCollector
```

### 9.2. `COLLECTOR_MAP`에 추가
```python
COLLECTOR_MAP = {
    # ... 기존 항목
    "new_collector": NewCollector,
}
```

이제 `collector_cli.py`에서 `new_collector`를 인자로 실행할 수 있습니다.

---

## 10. `master_collectors.py`에서의 동작

`master_collectors.py`는 `collector_cli.py`의 `COLLECTOR_MAP`을 임포트하여 모든 수집기 목록을 동적으로 가져옵니다.  
각 collector 클래스에 정의된 메타데이터(설명, 테이블명, 병합 스크립트, 타임아웃 등)를 읽어와 메뉴를 구성하고 실행합니다.

- 새 수집기를 추가하면 `COLLECTOR_MAP`에 등록하는 것만으로 `master_collectors.py`에 자동 반영됩니다.
- 실행은 항상 `collector_cli.py`를 통해 이루어집니다.

### 10.1. Smoke 테스트 실행
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

## 11. 병렬 실행 (`run_pipeline.py`)

`run_pipeline.py`는 하나의 collector에 대해 odd/even 샤드를 병렬로 실행하고, 선택적으로 병합까지 수행하는 통합 스크립트입니다.  
사용법:
```bash
python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [--regions REGIONS] [--quiet] [추가 인자...]
```

- `--regions`로 여러 지역을 지정하면 각 지역별로 odd/even을 병렬 실행합니다.
- `--year`를 생략하면 프롬프트로 입력받습니다.
- `--quiet` 옵션으로 진행률 표시를 끌 수 있습니다 (GitHub Actions 등에서 사용).
- collector별 병합 스크립트는 `merge_script` 메타데이터에 정의된 것을 사용합니다.

### 11.1. `rich` 라이브러리를 이용한 멀티바 진행률
`rich`가 설치되어 있으면 각 지역-샤드 작업의 진행 상황을 실시간으로 보여주는 멀티바가 표시됩니다.  
설치되지 않은 경우 단순 텍스트 진행률로 폴백됩니다.

---

## 12. 정기 실행 (`run_collector.py`)

`run_collector.py`(구 run_daily.py)는 cron 등에서 정기적으로 실행되는 스크립트입니다.  
내부적으로 `collector_cli.py`를 호출하여 필요한 수집기를 순차 실행합니다.  
새 수집기를 정기 실행에 추가하려면 `run_collector.py`에 해당 함수를 추가하고 `main()`에서 호출합니다.

```python
def run_new_collector():
    run_collector("new_collector", ["--regions", "ALL", "--year", str(now_kst().year)], "새 수집기")
```

---

## 13. 공통 유틸리티 활용

### 13.1. API 키 및 환경변수
- `constants.codes`에 정의된 API 키 상수를 사용합니다. (대부분 환경변수에서 로드)
- 설정 파일(`config.yaml`)의 `api` 섹션에서 환경변수명을 지정할 수 있습니다.
- `.env` 파일은 `core/__init__.py`에서 자동 로드됩니다.

### 13.2. 중앙 API 매핑 시스템 (`constants/api_mappings.py`)
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

### 13.3. 에러 처리 및 재시도
- `RetryManager`를 사용하여 실패한 작업을 기록하고 재시도할 수 있습니다.
- `self.retry_mgr.record_failure(...)`로 실패를 기록하고, `scripts/retry_worker.py`가 처리합니다.

### 13.4. 로깅
- `self.logger`를 사용하여 로그를 남깁니다. (자동으로 파일과 콘솔에 출력)

### 13.5. 메트릭 생성
- `core.metrics` 모듈을 사용하여 수집 현황 통계를 생성할 수 있습니다. (선택 사항)
- collector 클래스의 `metrics_config`에 설정을 정의하면 `master_collectors.py`에서 메트릭을 생성할 수 있습니다.

### 13.6. Vocab 관리
- `MetaVocabManager`를 사용하여 정규화된 주소 ID 등을 생성하고 관리할 수 있습니다.

### 13.7. 주소 처리 (`core.address`)
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

### 13.8. 지오코딩 분리 (`geo_collector`)
좌표가 필요한 수집기(예: `neis_info_collector`)는 주소 정제까지만 수행하고, 실제 지오코딩은 `geo_collector`가 담당합니다.
- `geo_collector.batch_update_schools(limit=500)`를 주기적으로 실행하여 좌표가 없는 학교들을 업데이트합니다.
- 실패한 지오코딩 작업은 `failures.db`에 저장되고 `retry_worker`가 재시도합니다.

### 13.9. ID 생성
- `school_id.create_school_id()`로 교육청 코드+학교 코드 → 32비트 정수 ID 생성.

### 13.10. 시간 처리
- `core.kst_time.now_kst()`로 KST 현재 시간 획득.
- `core.school_year.get_current_school_year()`로 현재 학년도 계산.

### 13.11. API 오류 코드 처리 (`constants/errors.py`)
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

## 14. 중앙 스키마 관리 (`constants/schema.py`)와 통합 마이그레이션

### 14.1. 스키마 정의
모든 테이블의 스키마는 `constants/schema.py`에 중앙 정의됩니다.

```python
# constants/schema.py
SCHEMAS = {
    "school_info": {
        "table_name": "schools",
        "columns": [
            ("school_code", "TEXT", "PRIMARY KEY"),
            ("school_name", "TEXT", "NOT NULL"),
            ("region_code", "TEXT", "NOT NULL"),
            # ... 모든 컬럼
        ],
        "indexes": [
            ("idx_schools_region", "atpt_ofcdc_org_code"),
            ("idx_schools_type", "schul_knd_sc_code"),
        ],
    },
    "neis_info": {
        "table_name": "schools",
        "columns": [ ... ],
        "indexes": [ ... ],
    },
    # 다른 수집기도 동일한 방식으로 추가
}
```

### 14.2. 통합 마이그레이션 스크립트 (`migrate.py`)
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

### 14.3. 새 컬럼 추가 시
1. `constants/schema.py`에 컬럼 정의를 추가합니다.
2. `migrate.py`를 실행하여 모든 관련 DB에 컬럼을 추가합니다.
3. 필요시 `api_mappings.py`에도 새 내부 필드명과 API 키 매핑을 추가합니다.

이 방식으로 각 수집기별 개별 마이그레이션 스크립트를 유지할 필요가 없어집니다.

---

## 15. 참고할 기존 Collector

- **NEIS 학교정보** (`collectors/neis_info_collector.py`): 지오코딩 분리, 모든 API 필드 저장, `MetaVocabManager` 사용 예, 중앙 스키마 적용
- **학교알리미** (`collectors/school_info_collector.py`): 기본 구조, 학년도 필터링, 좌표 포함 API, 중앙 스키마 적용
- **급식** (`collectors/meal_collector.py`): 일별 수집, `fetch_daily`, `BaseMealCollector` 상속
- **학사일정** (`collectors/schedule_collector.py`): 연간 수집, `fetch_year`
- **시간표** (`collectors/timetable_collector.py`): 추가 옵션(`--semester`), `fetch_year`
- **지오코딩** (`collectors/geo_collector.py`): 배치 지오코딩, API 사용량 관리, 캐싱

---

## 16. 중요: 설정 파일 및 의존성 관리

### 16.1. `config.yaml` 필수 항목
각 새 수집기는 `config.yaml`의 `collectors` 섹션에 설정을 추가해야 합니다.  
최소한 다음 항목을 포함하는 것이 좋습니다:
```yaml
collectors:
  new_collector:
    timeout_seconds: 3600
    parallel_timeout_seconds: 7200
    merge_timeout_seconds: 1800
    max_workers: 4
    metrics_config:
      enabled: true
```

### 16.2. `requirements.txt` 업데이트
새로운 외부 라이브러리가 필요하면 `requirements.txt`에 추가합니다.

---

## 17. Smoke 테스트

모든 변경 후에는 `python master_collectors.py --test`를 실행하여 기본적인 기능이 정상 동작하는지 확인합니다.  
이 테스트는 다음을 검증합니다:

- 수집기 로드
- DB 연결
- CLI 실행
- 병렬 스크립트 실행

테스트가 통과하면 프로젝트가 정상 상태임을 의미합니다.

---

이 가이드를 따라 새로운 수집기를 만들면 `collector_cli.py`, `master_collectors.py`, `run_pipeline.py` 등에 자연스럽게 통합되며, 프로젝트의 일관성을 유지할 수 있습니다.  
궁금한 점이 있으면 기존 collector의 코드를 참조하거나 팀 리더에게 문의하세요.
```
