```markdown
# 📘 Hi-Dunkey Collector 개발 가이드 (최신 구조)

이 문서는 Hi-Dunkey 프로젝트에서 새로운 수집기(Collector)를 개발할 때 반드시 따라야 할 표준 패턴과 규칙을 설명합니다.  
모든 수집기는 `core.base_collector.BaseCollector`를 상속받아야 하며, `collector_cli.py`에 등록되어야 합니다.  
또한 `run_collector.py`를 통해 자동화된 수집 파이프라인에 통합됩니다.

---

## 1. Collector 기본 구조

```python
from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.network import safe_json_request, build_session
from constants.codes import REGION_NAMES, NEIS_ENDPOINTS  # 필요에 따라
from constants.paths import MASTER_DIR  # 또는 적절한 경로

class NewCollector(BaseCollector):
    # ----- 메타데이터 (클래스 변수) -----
    description = "새로운 수집기 설명"
    table_name = "my_table"                     # 기본 테이블명
    merge_script = "scripts/merge_my_dbs.py"    # 병합 스크립트 (없으면 None)
    # parallel_script는 기본값 "scripts/run_pipeline.py" 사용 (변경 필요시 오버라이드)
    timeout_seconds = 3600                       # 개별 수집 타임아웃
    parallel_timeout_seconds = 7200              # 병렬 실행 타임아웃
    merge_timeout_seconds = 1800                  # 병합 타임아웃
    modes = ["통합", "odd 샤드", "even 샤드", "병렬 실행"]  # 지원 모드 (필요시 변경)
    metrics_config = {"enabled": False}           # 메트릭 설정
    parallel_config = {}                           # 병렬 실행 설정
    # ------------------------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False, **kwargs):
        # 도메인명, 저장 디렉토리, 샤드, 범위를 BaseCollector에 전달
        super().__init__("new_domain", str(MASTER_DIR), shard, school_range)
        self.debug_mode = debug_mode
        # 추가 인자 처리 (예: incremental, full 등)
        self.incremental = kwargs.get('incremental', False)
        # 필요한 리소스 등록 (예: MetaVocabManager, GeoCollector)
        # self.meta_vocab = self.register_resource(MetaVocabManager(...))
        self._init_db()
```

### 1.1. `__init__` 주요 인자
- `shard`: `"none"`, `"odd"`, `"even"` 중 하나 (기본값 `"none"`)
- `school_range`: `"A"`, `"B"`, `"none"` 또는 `None` (범위 필터)
- `debug_mode`: 디버그 출력 여부
- `**kwargs`: 추가 옵션 (예: `incremental`, `full`, `compare`)

### 1.2. DB 경로 자동 결정
`BaseCollector`가 `self.db_path`를 다음과 같이 생성합니다.
- `shard="none"` → `{base_dir}/{name}.db`
- `shard="odd"` → `{base_dir}/{name}_odd.db`
- `shard="even"` → `{base_dir}/{name}_even.db`
- `school_range`가 있으면 `{name}_{shard}_{school_range}.db`

※ `base_dir`은 일반적으로 `constants.paths.MASTER_DIR`을 사용합니다.

---

## 2. DB 초기화 (`_init_db`)

각 수집기는 자신의 데이터를 저장할 테이블을 생성해야 합니다.

```python
def _init_db(self):
    with get_db_connection(self.db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS my_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                school_code TEXT,
                some_data TEXT,
                collected_at TEXT
            )
        """)
        # 필요한 인덱스 생성
        conn.execute("CREATE INDEX IF NOT EXISTS idx_school_code ON my_table(school_code)")
        # 공통 체크포인트 테이블 초기화 (필요시)
        self._init_db_common(conn)
```

- **주의**: `get_db_connection`은 WAL 모드, 적절한 PRAGMA 설정을 자동으로 적용합니다.
- `_init_db_common(conn)`을 호출하면 `collection_checkpoint` 테이블이 생성됩니다 (선택 사항).

---

## 3. 데이터 수집 메서드 구현

대부분의 수집기는 `fetch_region(region_code, year=None, date=None, **kwargs)` 형태의 메서드를 구현합니다.  
단, 급식처럼 일별 수집이 필요한 경우 `fetch_daily`를 구현할 수 있습니다.

```python
def fetch_region(self, region_code: str, year: int = None, date: str = None, **kwargs):
    if year is None:
        year = get_current_school_year(now_kst())
    region_name = REGION_NAMES.get(region_code, region_code)

    if self.debug_mode:
        print(f"📡 [{region_name}] 수집 시작 (year={year})")

    # API 요청 파라미터 구성
    params = {
        "ATPT_OFCDC_SC_CODE": region_code,
        # 기타 필수 파라미터
    }

    # 페이지네이션 처리
    rows = self._fetch_paginated(
        API_URL, params, 'root_key',  # root_key는 API 응답에서 데이터가 있는 키
        region=region_code,
        year=year,
        page_size=100
    )

    if not rows:
        self.logger.warning(f"[{region_name}] 데이터 없음")
        return

    # 샤드 필터링 후 enqueue
    for row in rows:
        school_code = row.get("SD_SCHUL_CODE")  # API에 따라 키 이름 다름
        if not school_code or not should_include_school(self.shard, self.school_range, school_code):
            continue
        self.enqueue([self._transform_row(row, region_code)])
```

### 3.1. `_fetch_paginated` 사용법
`BaseCollector`에는 `_fetch_paginated` 메서드가 내장되어 있습니다.  
이 메서드는 `core.network.safe_json_request`를 사용하며, 자동으로 페이지를 순회하고 재시도합니다.

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

### 3.2. 샤드/범위 필터링
`should_include_school(self.shard, self.school_range, school_code)`를 사용하여 학교 코드가 현재 샤드와 범위에 속하는지 확인합니다.

---

## 4. 데이터 변환 (`_transform_row`)

API 응답 row를 DB 레코드 딕셔너리로 변환합니다.

```python
def _transform_row(self, row: dict, region_code: str) -> dict:
    now = now_kst().isoformat()
    return {
        "school_code": row.get("SD_SCHUL_CODE"),
        "some_data": row.get("SOME_FIELD"),
        "collected_at": now,
        # ...
    }
```

- 이 딕셔너리는 나중에 `_do_save_batch`에서 사용됩니다.
- 필요한 경우 추가 필드를 포함할 수 있습니다.

---

## 5. 배치 저장 (`_do_save_batch`)

`BaseCollector`는 내부 writer 스레드가 일정 크기마다 `_do_save_batch`를 호출합니다.  
이 메서드를 오버라이드하여 실제 INSERT/REPLACE 로직을 구현합니다.

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

## 6. 추가 메서드 구현 (필요시)

- `_process_item(self, raw_item)`: 단일 API row를 처리하여 enqueue할 아이템 리스트 반환.  
  기본적으로 `_transform_row`를 호출하는 간단한 구현이면 충분하지만, 복잡한 파싱이 필요하면 이 메서드를 오버라이드할 수 있습니다.
- `iterate_schools`, `iterate_schools_by_month` 등이 필요한 경우 직접 구현합니다. (예: 급식, 학사일정 등)

---

## 7. `collector_cli.py`에 등록

### 7.1. Collector 클래스 임포트
`collector_cli.py` 상단에 새 수집기 클래스를 임포트합니다.

```python
from collectors.new_collector import NewCollector
```

### 7.2. `COLLECTOR_MAP`에 추가
```python
COLLECTOR_MAP = {
    # ... 기존 항목
    "new_collector": NewCollector,
}
```

이제 `collector_cli.py`에서 `new_collector`를 인자로 실행할 수 있습니다.

---

## 8. `master_collectors.py`에서의 동작

`master_collectors.py`는 더 이상 `collectors.json`을 읽지 않습니다. 대신 `collector_cli.py`의 `COLLECTOR_MAP`을 임포트하여 모든 수집기 목록을 동적으로 가져옵니다.  
각 collector 클래스에 정의된 메타데이터(설명, 테이블명, 병합 스크립트, 타임아웃 등)를 읽어와 메뉴를 구성하고 실행합니다.

- 새 수집기를 추가하면 `COLLECTOR_MAP`에 등록하는 것만으로 `master_collectors.py`에 자동 반영됩니다.
- 실행은 항상 `collector_cli.py`를 통해 이루어집니다.

---

## 9. 병렬 실행 (`run_pipeline.py`)

`run_pipeline.py`는 하나의 collector에 대해 odd/even 샤드를 병렬로 실행하고, 선택적으로 병합까지 수행하는 통합 스크립트입니다.  
사용법:
```bash
python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [--regions REGIONS] [추가 인자...]
```

- `--regions`로 여러 지역을 지정하면 각 지역별로 odd/even을 병렬 실행합니다.
- `--year`를 생략하면 프롬프트로 입력받습니다.
- collector별 병합 스크립트는 `merge_script` 메타데이터에 정의된 것을 사용합니다.

---

## 10. 정기 실행 (`run_collector.py`)

`run_collector.py`(구 run_daily.py)는 cron 등에서 정기적으로 실행되는 스크립트입니다.  
내부적으로 `collector_cli.py`를 호출하여 필요한 수집기를 순차 실행합니다.  
새 수집기를 정기 실행에 추가하려면 `run_collector.py`에 해당 함수를 추가하고 `main()`에서 호출합니다.

```python
def run_new_collector():
    run_collector("new_collector", ["--regions", "ALL", "--year", str(now_kst().year)], "새 수집기")
```

---

## 11. 공통 유틸리티 활용

### 11.1. API 키 및 환경변수
- `constants.codes`에 정의된 `NEIS_API_KEY`, `VWORLD_API_KEY` 등을 사용합니다.
- `.env` 파일을 통해 키를 관리할 수 있습니다 (`core/__init__.py`에서 로드).

### 11.2. 에러 처리 및 재시도
- `RetryManager`를 사용하여 실패한 작업을 기록하고 재시도할 수 있습니다.
- `self.retry_mgr.record_failure(...)`로 실패를 기록하고, `scripts/retry_worker.py`가 처리합니다.

### 11.3. 로깅
- `self.logger`를 사용하여 로그를 남깁니다. (자동으로 파일과 콘솔에 출력)

### 11.4. 메트릭 생성
- `core.metrics` 모듈을 사용하여 수집 현황 통계를 생성할 수 있습니다. (선택 사항)
- collector 클래스의 `metrics_config`에 설정을 정의하면 `master_collectors.py`에서 메트릭을 생성할 수 있습니다.

### 11.5. Vocab 관리
- `VocabManager`, `MetaVocabManager`를 사용하여 정규화된 ID를 생성하고 관리할 수 있습니다.

### 11.6. 주소 필터링
- `core.filters.AddressFilter`를 사용하여 주소를 정규화하고 지번을 추출할 수 있습니다.

### 11.7. ID 생성
- `school_id.create_school_id()`로 교육청 코드+학교 코드 → 32비트 정수 ID 생성.
- `IDGenerator`로 임의 텍스트 기반 63비트 ID 생성.

### 11.8. 시간 처리
- `core.kst_time.now_kst()`로 KST 현재 시간 획득.
- `core.school_year.get_current_school_year()`로 현재 학년도 계산.

---

## 12. 템플릿 예제 (전체 코드)

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR

API_URL = "https://api.example.com/endpoint"

class TemplateCollector(BaseCollector):
    # 메타데이터
    description = "템플릿 수집기"
    table_name = "template"
    merge_script = "scripts/merge_template_dbs.py"
    timeout_seconds = 3600
    parallel_timeout_seconds = 7200
    merge_timeout_seconds = 1800
    metrics_config = {"enabled": True}
    parallel_config = {"max_workers": 2}

    def __init__(self, shard="none", school_range=None, debug_mode=False, **kwargs):
        super().__init__("template", str(MASTER_DIR), shard, school_range)
        self.debug_mode = debug_mode
        self._init_db()

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS template (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    school_code TEXT,
                    data TEXT,
                    collected_at TEXT
                )
            """)
            self._init_db_common(conn)

    def fetch_region(self, region_code: str, year: int = None, date: str = None, **kwargs):
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)

        params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(API_URL, params, "rootKey", region=region_code, year=year)

        for row in rows:
            school_code = row.get("SD_SCHUL_CODE")
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue
            self.enqueue([self._transform_row(row, region_code)])

    def _transform_row(self, row, region_code):
        return {
            "school_code": row.get("SD_SCHUL_CODE"),
            "data": row.get("SOME_FIELD"),
            "collected_at": now_kst().isoformat(),
        }

    def _do_save_batch(self, conn, batch):
        sql = "INSERT OR REPLACE INTO template (school_code, data, collected_at) VALUES (?, ?, ?)"
        rows = [(r["school_code"], r["data"], r["collected_at"]) for r in batch]
        conn.executemany(sql, rows)


if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector, region, **kwargs):
        collector.fetch_region(region, **kwargs)
    run_collector(TemplateCollector, _fetch, "템플릿 수집기")
```

---

## 13. 참고할 기존 Collector

- **NEIS 학교정보** (`collectors/neis_info_collector.py`): 복잡한 지오코딩, Diff 처리, MetaVocabManager 사용 예
- **학교알리미** (`collectors/school_info_collector.py`): 기본 구조, 학년도 필터링
- **급식** (`collectors/meal_collector.py`): 일별 수집, `fetch_daily`, `BaseMealCollector` 상속
- **학사일정** (`collectors/schedule_collector.py`): 연간 수집, `fetch_year`
- **시간표** (`collectors/timetable_collector.py`): 추가 옵션(`--semester`), `fetch_year`

---

## 14. 중요: `collectors.json` 제거

프로젝트에서 `collectors.json` 파일은 더 이상 사용하지 않습니다.  
모든 설정은 각 collector 클래스의 메타데이터로 대체되었습니다.  
새로운 수집기 추가 시 `collector_cli.py`의 `COLLECTOR_MAP`에 등록하고, 클래스에 필요한 메타데이터를 정의하면 모든 도구(`master_collectors.py`, `run_pipeline.py` 등)에 자동으로 통합됩니다.

---

이 가이드를 따라 새로운 수집기를 만들면 `collector_cli.py`와 `run_collector.py`에 자연스럽게 통합되며, 프로젝트의 일관성을 유지할 수 있습니다.  
궁금한 점이 있으면 기존 collector의 코드를 참조하거나 팀 리더에게 문의하세요.
```