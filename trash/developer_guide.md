## 📘 Hi-Dunkey Collector 개발 가이드

이 문서는 Hi-Dunkey 프로젝트에서 새로운 수집기(Collector)를 개발할 때 반드시 따라야 할 표준 패턴과 규칙을 설명합니다.  
모든 수집기는 `core.base_collector.BaseCollector`를 상속받아야 하며, `collector_cli.py`에 등록되어야 합니다.  
또한 `run_collector.py`를 통해 자동화된 수집 파이프라인에 통합됩니다.

---

## 1. 프로젝트 구조 개요

```
📦 프로젝트 루트
├── config/                 # 설정 파일 디렉토리
│   └── config.yaml         # 환경별 설정 (경로, 타임아웃, 병렬 설정 등)
├── core/                   # 핵심 공통 모듈
│   ├── base_collector.py   # 모든 수집기의 베이스 클래스 (print, print_progress 포함)
│   ├── config.py           # 설정 파일 로더
│   ├── database.py         # DB 연결 공통
│   ├── kst_time.py         # 시간 처리
│   ├── logger.py           # 로깅
│   ├── network.py          # 네트워크 요청
│   ├── retry.py            # 실패 재시도 관리
│   ├── shard.py            # 샤딩/범위 필터링
│   └── ...                 # 기타 유틸리티
├── collectors/             # 개별 수집기 구현
│   ├── neis_info_collector.py
│   ├── school_info_collector.py
│   ├── meal_collector.py
│   ├── schedule_collector.py
│   └── timetable_collector.py
├── constants/              # 상수 모듈
│   ├── codes.py            # API 엔드포인트, 지역 코드 등
│   └── paths.py            # 경로 상수 (모든 경로는 여기서 관리)
├── scripts/                # 실행 스크립트
│   ├── run_pipeline.py     # 병렬 실행 + 병합 (rich 멀티바)
│   ├── run_collector.py    # 정기 실행 (cron용)
│   └── retry_worker.py     # 실패 작업 재시도
├── collector_cli.py        # 공통 CLI 진입점 (순차 실행)
├── master_collectors.py    # 메뉴 기반 마스터 제어 (--test 옵션 포함)
├── requirements.txt        # 의존성 목록
└── docs/                   # 문서
    └── developer_guide.md  # 이 문서
```

---

## 2. Collector 기본 구조

```python
#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any

# 프로젝트 루트를 sys.path에 추가 (모든 로컬 임포트보다 먼저!)
sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.shard import should_include_school
from core.kst_time import now_kst
from core.school_year import get_current_school_year
from core.network import safe_json_request, build_session
from core.config import config
from core.neis_validator import neis_validator  # 필요시
from constants.codes import REGION_NAMES
from constants.paths import MASTER_DIR, ACTIVE_DIR  # 필요에 따라

API_URL = "https://api.example.com/endpoint"

class NewCollector(BaseCollector):
    # ----- 메타데이터 (클래스 변수) -----
    description = "새로운 수집기 설명"
    table_name = "my_table"                     # 기본 테이블명
    merge_script = "scripts/merge_my_dbs.py"    # 병합 스크립트 (없으면 None)
    
    # 설정 파일에서 값을 가져오되, 없으면 기본값 사용
    _cfg = config.get_collector_config("new_collector")  # config.yaml의 collectors.new_collector 섹션
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 1800)
    
    # parallel_script는 기본값 "scripts/run_pipeline.py" 사용 (변경시 오버라이드)
    parallel_script = _cfg.get("parallel_script", "scripts/run_pipeline.py")
    
    modes = _cfg.get("modes", ["통합", "odd 샤드", "even 샤드", "병렬 실행"])
    metrics_config = _cfg.get("metrics_config", {"enabled": False})
    parallel_config = _cfg.get("parallel_config", {})
    # ------------------------------------

    def __init__(self, shard: str = "none", school_range: Optional[str] = None, 
                 debug_mode: bool = False, quiet_mode: bool = False):
        # 도메인명, 저장 디렉토리, 샤드, 범위를 BaseCollector에 전달
        super().__init__("new_collector", str(MASTER_DIR), shard, school_range)
        self.debug_mode = debug_mode
        self.quiet_mode = quiet_mode
        self.incremental = kwargs.get('incremental', False)
        self._init_db()

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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_school_code ON my_table(school_code)")
            self._init_db_common(conn)

    def fetch_region(self, region_code: str, year: Optional[int] = None, 
                     date: Optional[str] = None) -> int:
        """
        지역별 데이터 수집
        Returns: 처리된 아이템 수 (진행률 표시용)
        """
        if year is None:
            year = get_current_school_year(now_kst())
        region_name = REGION_NAMES.get(region_code, region_code)

        self.print(f"📡 [{region_name}] 학년도 {year} 수집 시작 (샤드: {self.shard})", level="debug")

        params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(API_URL, params, "rootKey", region=region_code, year=year)

        if not rows:
            self.logger.warning(f"[{region_name}] 데이터 없음")
            return 0

        processed = 0
        total = len(rows)
        for idx, row in enumerate(rows, 1):
            school_code = row.get("SD_SCHUL_CODE")
            if not school_code or not should_include_school(self.shard, self.school_range, school_code):
                continue

            self.enqueue([self._transform_row(row, region_code)])
            processed += 1
            self.print_progress(idx, total, prefix=f"[{region_name}]")

        return processed

    def _transform_row(self, row: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        return {
            "school_code": row.get("SD_SCHUL_CODE"),
            "some_data": row.get("SOME_FIELD"),
            "collected_at": now_kst().isoformat(),
        }

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[Dict[str, Any]]) -> None:
        sql = "INSERT OR REPLACE INTO my_table (school_code, some_data, collected_at) VALUES (?, ?, ?)"
        rows = [(r["school_code"], r["some_data"], r["collected_at"]) for r in batch]
        conn.executemany(sql, rows)

    # (필요시 _parse_float, _get_field 등 오버라이드)

if __name__ == "__main__":
    from core.collector_cli import run_collector
    def _fetch(collector: NewCollector, region: str, **kwargs) -> None:
        collector.fetch_region(region, **kwargs)
    run_collector(NewCollector, _fetch, "새로운 수집기")
```

### 2.1. `__init__` 주요 인자
- `shard`: `"none"`, `"odd"`, `"even"` 중 하나 (기본값 `"none"`)
- `school_range`: `"A"`, `"B"`, `"none"` 또는 `None` (범위 필터)
- `debug_mode`: 디버그 출력 여부
- `quiet_mode`: 모든 출력 최소화 여부 (GitHub Actions 등에서 사용)
- `**kwargs`: 추가 옵션 (예: `incremental`, `full`, `compare`)

### 2.2. DB 경로 자동 결정
`BaseCollector`가 `self.db_path`를 다음과 같이 생성합니다.
- `shard="none"` → `{base_dir}/{name}.db`
- `shard="odd"` → `{base_dir}/{name}_odd.db`
- `shard="even"` → `{base_dir}/{name}_even.db`
- `school_range`가 있으면 `{name}_{shard}_{school_range}.db`

※ `base_dir`은 일반적으로 `constants.paths.MASTER_DIR` 또는 `constants.paths.ACTIVE_DIR`을 사용합니다.

---

## 3. 공통 출력 메서드 (`BaseCollector`)

모든 collector는 `BaseCollector`에 정의된 다음 메서드를 통해 출력을 제어해야 합니다.

```python
def print(self, *args, level: str = "info", **kwargs):
    """
    통합 출력 메서드.
    - quiet 모드면 모든 출력 차단.
    - level이 'debug'면 debug 모드에서만 출력.
    - 그 외는 항상 출력 (quiet 모드가 아니면).
    """

def print_progress(self, current: int, total: int, prefix: str = "", bar_length: int = 20):
    """
    간단한 진행률 바 출력 (디버그 모드 아닐 때만, quiet 모드면 미출력).
    완료 시 줄바꿈.
    """
```

- **`--quiet` 옵션**: 모든 `print` 호출을 무시합니다. (진행률도 출력 안 함)
- **`--debug` 옵션**: `level="debug"`로 호출된 `print`만 출력합니다. (일반 `print`는 출력)

**사용 예시**:
```python
self.print("일반 메시지")  # quiet 모드가 아니면 항상 출력
self.print("디버그 메시지", level="debug")  # debug 모드에서만 출력
self.print_progress(i, total, prefix=f"[{region}]")  # 진행률 표시
```

---

## 4. 경로 관리 (`constants.paths`)

모든 파일 경로는 `constants.paths.py`에 정의된 상수를 사용해야 합니다.  
직접 문자열 경로를 하드코딩하지 마세요.

### 4.1. 주요 상수
```python
# 디렉토리
DATA_DIR = PROJECT_ROOT / "data"
ACTIVE_DIR = DATA_DIR / "active"
MASTER_DIR = DATA_DIR / "master"
LOG_DIR = DATA_DIR / "logs"
METRICS_DIR = DATA_DIR / "metrics"
BACKUP_DIR = DATA_DIR / "backup"
ARCHIVE_DIR = DATA_DIR / "archive"

# DB 파일
NEIS_INFO_DB_PATH = MASTER_DIR / "neis_info.db"
SCHOOL_INFO_DB_PATH = MASTER_DIR / "school_info.db"
MEAL_DB_PATH = ACTIVE_DIR / "meal.db"
SCHEDULE_DB_PATH = ACTIVE_DIR / "schedule.db"
TIMETABLE_DB_PATH = ACTIVE_DIR / "timetable.db"
FAILURES_DB_PATH = DATA_DIR / "failures.db"
GLOBAL_VOCAB_DB_PATH = ACTIVE_DIR / "global_vocab.db"
UNKNOWN_DB_PATH = ACTIVE_DIR / "unknown_patterns.db"

# 샤드 DB (필요시)
NEIS_INFO_ODD_DB_PATH = MASTER_DIR / "neis_info_odd.db"
NEIS_INFO_EVEN_DB_PATH = MASTER_DIR / "neis_info_even.db"
# ... 등
```

### 4.2. 사용 예시
```python
from constants.paths import NEIS_INFO_DB_PATH, LOG_DIR

# 문자열이 필요한 함수에는 str()로 변환
conn = sqlite3.connect(str(NEIS_INFO_DB_PATH))

# 로그 파일 경로
log_file = LOG_DIR / "my_collector.log"
```

---

## 5. 설정 파일 (`config.yaml`)

모든 환경별 설정은 `config/config.yaml`에서 관리합니다.  
설정은 `core.config.Config` 싱글톤을 통해 로드됩니다.

### 5.1. 예시
```yaml
# config/config.yaml
paths:
  master_dir: "master"
  active_dir: "active"
  logs_dir: "logs"

collectors:
  neis_info:
    timeout_seconds: 3600
    parallel_timeout_seconds: 7200
    merge_timeout_seconds: 3600
    max_workers: 4
    cpu_factor: 1.0
    max_by_api: 10
    absolute_max: 16
    metrics_config:
      enabled: true
      collect_geo: true
      collect_global: true

api:
  neis_api_key_env: "NEIS_API_KEY"
  vworld_api_key_env: "VWORLD_API_KEY"
```

### 5.2. 설정 값 접근
```python
from core.config import config

# 특정 collector 설정 가져오기
cfg = config.get_collector_config("neis_info")
timeout = cfg.get("timeout_seconds", 3600)

# 전체 설정에서 경로 가져오기
master_dir = config.get('paths', 'master_dir', default='master')
```

### 5.3. 환경변수 오버라이드
환경변수 `CONFIG__PATHS__MASTER_DIR=/custom/path` 형식으로 설정을 덮어쓸 수 있습니다.

---

## 6. 병렬 실행 (`run_pipeline.py`)

`run_pipeline.py`는 하나의 collector에 대해 odd/even 샤드를 병렬로 실행하고, 선택적으로 병합까지 수행하는 통합 스크립트입니다.

```bash
python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [--regions REGIONS] [--quiet] [추가 인자...]
```

- `--regions`로 여러 지역을 지정하면 각 지역별로 odd/even을 병렬 실행합니다.
- `--year`를 생략하면 프롬프트로 입력받습니다.
- `--quiet` 옵션으로 진행률 표시를 끌 수 있습니다 (GitHub Actions 등에서 사용).
- collector별 병합 스크립트는 `merge_script` 메타데이터에 정의된 것을 사용합니다.

### 6.1. `rich` 라이브러리를 이용한 멀티바 진행률
`rich`가 설치되어 있으면 각 지역-샤드 작업의 진행 상황을 실시간으로 보여주는 멀티바가 표시됩니다.  
설치되지 않은 경우 단순 텍스트 진행률로 폴백됩니다.

---

## 7. 순차 실행 (`collector_cli.py`)

`collector_cli.py`는 단일 collector를 순차적으로 실행하는 CLI 진입점입니다.

```bash
python collector_cli.py <collector_name> [--regions REGIONS] [--shard none|odd|even] [--year YYYY] [--date YYYYMMDD] [--debug] [--quiet] [--limit N]
```

- 각 지역을 순차적으로 처리하며, collector 내부에서 `self.print_progress`로 진행률을 표시합니다.
- `--quiet` 옵션을 주면 모든 출력이 사라집니다.
- `--debug` 옵션을 주면 상세 디버그 로그가 출력됩니다.

---

## 8. 정기 실행 (`run_collector.py`)

`run_collector.py`(구 run_daily.py)는 cron 등에서 정기적으로 실행되는 스크립트입니다.  
내부적으로 `collector_cli.py`를 호출하여 필요한 수집기를 순차 실행합니다.  
새 수집기를 정기 실행에 추가하려면 `run_collector.py`에 해당 함수를 추가하고 `main()`에서 호출합니다.

```python
def run_new_collector():
    run_collector("new_collector", ["--regions", "ALL", "--year", str(now_kst().year)], "새 수집기")
```

---

## 9. 마스터 수집기 (`master_collectors.py`)

`master_collectors.py`는 메뉴 기반으로 수집기를 선택하고 실행할 수 있는 대화형 스크립트입니다.

```bash
python master_collectors.py [--test]
```

- `--test` 옵션으로 smoke test를 실행하여 모든 수집기 로드, DB 연결, CLI 실행, 병렬 스크립트 실행을 테스트할 수 있습니다.
- 실행 후에는 데이터 무결성 확인, 병합, 메트릭 생성, 로그 확인, 디버그 재실행 등의 후속 작업을 선택할 수 있습니다.

---

## 10. 테스트

### 10.1. Smoke Test
```bash
python master_collectors.py --test
```
다음을 테스트합니다:
- 수집기 로드
- DB 연결 (처음 두 개 collector)
- CLI 기본 실행 (`neis_info --limit 1`)
- 병렬 스크립트 실행 (`run_pipeline.py`)

### 10.2. 개별 collector 테스트
```bash
python collector_cli.py neis_info --regions B10 --limit 5 --debug
```

---

## 11. 새 Collector 추가 절차

1. **`collectors/` 디렉토리에 새 파일 생성** (예: `new_collector.py`)
2. **`BaseCollector`를 상속받는 클래스 작성** (위 템플릿 참조)
   - 메타데이터(클래스 변수) 정의
   - `_init_db`, `fetch_region`, `_transform_row`, `_do_save_batch` 구현
   - 필요한 경우 `_parse_float`, `_get_field` 등 오버라이드
3. **`collector_cli.py`의 `COLLECTOR_MAP`에 등록**
   ```python
   from collectors.new_collector import NewCollector
   COLLECTOR_MAP = {
       # ... 기존 항목
       "new_collector": NewCollector,
   }
   ```
4. **`config.yaml`에 collector 설정 추가** (선택 사항)
   ```yaml
   collectors:
     new_collector:
       timeout_seconds: 3600
       max_workers: 4
       metrics_config:
         enabled: true
   ```
5. **필요시 병합 스크립트(`scripts/merge_new_dbs.py`) 작성**
6. **정기 실행 스크립트(`run_collector.py`)에 추가** (필요시)
7. **smoke test 통과 확인**
   ```bash
   python master_collectors.py --test
   ```

---

## 12. 참고할 기존 Collector

- **NEIS 학교정보** (`neis_info_collector.py`): 복잡한 지오코딩, Diff 처리, `print_progress` 사용 예
- **학교알리미** (`school_info_collector.py`): 기본 구조, 학년도 필터링, NEIS Validator 연동
- **급식** (`meal_collector.py`): 일별 수집, `fetch_daily`, `BaseMealCollector` 상속
- **학사일정** (`schedule_collector.py`): 연간 수집, `fetch_year`
- **시간표** (`timetable_collector.py`): 추가 옵션(`--semester`), `fetch_year`

---

## 13. 주요 규칙 요약

- ✅ 모든 collector는 `BaseCollector`를 상속받는다.
- ✅ 모든 경로는 `constants.paths`의 상수를 사용한다. (절대 하드코딩 금지)
- ✅ 출력은 `self.print()`와 `self.print_progress()`를 사용한다. (`print` 직접 호출 금지)
- ✅ `fetch_region`은 처리된 아이템 수를 반환한다. (진행률 표시용)
- ✅ `__init__`에서 `debug_mode`와 `quiet_mode`를 받아 `BaseCollector`에 전달한다.
- ✅ 메타데이터는 클래스 변수로 정의하고, 설정 파일(`config.yaml`)에서 값을 가져올 수 있게 한다.
- ✅ `collector_cli.py`의 `COLLECTOR_MAP`에 등록한다.
- ✅ 병렬 실행은 `run_pipeline.py`에 위임하고, 각 collector는 `parallel_script` 메타데이터를 제공한다.
- ✅ 새 collector 추가 후 `python master_collectors.py --test`로 smoke test를 통과해야 한다.
