```markdown
# 📘 Hi-Dunkey Collector 개발 가이드

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

### 7.3. 추가 명령행 인자 처리
수집기별로 추가 옵션이 필요하면 `extra_parsers`에 등록합니다.

```python
extra_parsers = {
    "timetable_collector": lambda p: p.add_argument("--semester", type=int, default=1, choices=[1,2]),
    "new_collector": lambda p: p.add_argument("--my-option", type=int, help="설명"),
}
```

### 7.4. `run_collector` 함수 내 fetch 메서드 분기 추가
각 collector에 맞는 fetch 메서드를 호출하는 분기문을 추가합니다.

```python
if collector_name == "new_collector":
    collector.fetch_region(region, year=year, date=target_date, my_option=args.my_option)
elif collector_name == "meal_collector":
    collector.fetch_daily(region, target_date)
elif ...
```

---

## 8. `run_collector.py`에서 호출 (선택 사항)

`run_collector.py`(구 run_daily.py)는 정기적으로 실행되는 스크립트입니다.  
새 수집기를 추가하려면 적절한 함수(예: `run_new_collector_daily`)를 만들고 `main()`에서 호출합니다.  
단, 이 스크립트는 `collector_cli.py`를 통해 수집기를 호출하므로, 이미 `collector_cli.py`에 등록되었다면 별도 수정 없이 사용할 수 있습니다.  
필요한 경우 `run_collector` 함수를 사용하여 호출하면 됩니다.

```python
def run_new_collector():
    run_collector("new_collector", ["--regions", "ALL", "--year", str(now_kst().year)], "새 수집기")
```

---

## 9. 병렬 실행 스크립트 (선택 사항)

대량 수집을 위해 odd/even 샤드를 동시에 실행하는 병렬 래퍼 스크립트를 만들 수 있습니다.  
예: `neis_info_shard_collector.py`, `school_info_shard_collector.py` 참조.

```python
#!/usr/bin/env python3
import sys
import subprocess
from concurrent.futures import ProcessPoolExecutor

def run_shard(shard):
    cmd = [sys.executable, "collector_cli.py", "new_collector", "--shard", shard] + sys.argv[1:]
    return subprocess.run(cmd).returncode

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_shard, "odd"), executor.submit(run_shard, "even")]
        results = [f.result() for f in futures]
    sys.exit(0 if all(r == 0 for r in results) else 1)
```

---

## 10. 공통 유틸리티 활용

### 10.1. API 키 및 환경변수
- `constants.codes`에 정의된 `NEIS_API_KEY`, `VWORLD_API_KEY` 등을 사용합니다.
- `.env` 파일을 통해 키를 관리할 수 있습니다 (`core/__init__.py`에서 로드).

### 10.2. 에러 처리 및 재시도
- `RetryManager`를 사용하여 실패한 작업을 기록하고 재시도할 수 있습니다.
- `self.retry_mgr.record_failure(...)`로 실패를 기록하고, `scripts/retry_worker.py`가 처리합니다.

### 10.3. 로깅
- `self.logger`를 사용하여 로그를 남깁니다. (자동으로 파일과 콘솔에 출력)
- `build_logger`를 직접 호출할 필요 없이 `self.logger`를 사용하면 됩니다.

### 10.4. 메트릭 생성
- `core.metrics` 모듈을 사용하여 수집 현황 통계를 생성할 수 있습니다. (선택 사항)
- `collectors.json`에 `metrics_config`를 설정하면 `master_collectors.py`에서 메트릭을 생성할 수 있습니다.

### 10.5. Vocab 관리
- `VocabManager`, `MetaVocabManager`를 사용하여 정규화된 ID를 생성하고 관리할 수 있습니다.
- 예: 급식 메뉴명, 과목명, 주소 구성 요소 등.

### 10.6. 주소 필터링
- `AddressFilter`를 사용하여 주소를 정규화하고 지번을 추출할 수 있습니다.

### 10.7. ID 생성
- `school_id.create_school_id()`로 교육청 코드+학교 코드 → 32비트 정수 ID 생성.
- `IDGenerator`로 임의 텍스트 기반 63비트 ID 생성.

### 10.8. 시간 처리
- `core.kst_time.now_kst()`로 KST 현재 시간 획득.
- `core.school_year.get_current_school_year()`로 현재 학년도 계산.

---

## 11. 템플릿 예제 (전체 코드)

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

## 12. 참고할 기존 Collector

- **NEIS 학교정보** (`collectors/neis_info_collector.py`): 복잡한 지오코딩, Diff 처리, MetaVocabManager 사용 예
- **학교알리미** (`collectors/school_info_collector.py`): 기본 구조, 학년도 필터링
- **급식** (`collectors/meal_collector.py`): 일별 수집, `fetch_daily`, `BaseMealCollector` 상속
- **학사일정** (`collectors/schedule_collector.py`): 연간 수집, `fetch_year`
- **시간표** (`collectors/timetable_collector.py`): 추가 옵션(`--semester`), `fetch_year`

---

이 가이드를 따라 새로운 수집기를 만들면 `collector_cli.py`와 `run_collector.py`에 자연스럽게 통합되며, 프로젝트의 일관성을 유지할 수 있습니다.  
궁금한 점이 있으면 기존 collector의 코드를 참조하거나 팀 리더에게 문의하세요.
```

이 마크다운 파일을 프로젝트 루트에 `COLLECTOR_GUIDE.md` 또는 `docs/collector_guide.md`로 저장하면 됩니다. 필요에 따라 추가 내용을 업데이트할 수 있습니다.