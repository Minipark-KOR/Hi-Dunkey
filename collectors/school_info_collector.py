#!/usr/bin/env python3
"""
학교알리미 공공데이터 수집기 (최종 안정화 버전)
- 공공데이터포털 '학교알리미' API 연동
- 시군구 코드 매핑(sgg_code_map) 통합
- 배치 UPSERT, 인덱스 최적화, 예외 처리 강화
"""
import os
import sys
import json
import time
import logging
import sqlite3
import hashlib
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlencode, quote_plus
import urllib.request
import urllib.error

# ANSI 색상 (콘솔 출력용)
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

# 프로젝트 루트 추가 (모듈 임포트용)
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

# ==================== 시군구 코드 매핑 임포트 ====================
SGG_AVAILABLE = False
SGG_NAMES = {}
SIDO_SGG = {}   # 시도별 시군구 목록 인덱스

# 여러 위치에서 sgg_code_map 시도
try:
    from core.sgg_code_map import SGG_NAMES as _map, get_sgg_name
    SGG_NAMES = _map
    SGG_AVAILABLE = True
except ImportError:
    try:
        from collectors.sgg_code_map import SGG_NAMES as _map, get_sgg_name
        SGG_NAMES = _map
        SGG_AVAILABLE = True
    except ImportError:
        # Fallback: 강원도만 포함한 최소 매핑 (실제 운영 시 모듈 설치 권장)
        import warnings
        warnings.warn("sgg_code_map 모듈을 찾을 수 없습니다. 내장 최소 매핑(강원도)을 사용합니다.", UserWarning)
        SGG_NAMES = {
            "51110": "춘천시", "51130": "원주시", "51150": "강릉시",
            "51170": "동해시", "51190": "태백시", "51210": "속초시",
            "51230": "삼척시", "51720": "홍천군", "51730": "횡성군",
            "51750": "영월군", "51760": "평창군", "51770": "정선군",
            "51780": "철원군", "51790": "화천군", "51800": "양구군",
            "51810": "인제군", "51820": "고성군", "51830": "양양군",
        }

# 시도별 시군구 인덱스 구축
for code, name in SGG_NAMES.items():
    sido = code[:2]
    SIDO_SGG.setdefault(sido, []).append(code)

logger = logging.getLogger(__name__)

# ==================== 설정 및 상수 ====================
# ✅ 공백 제거된 정확한 URL (사용 시 실제 서비스 ID로 교체)
API_UUID = "b2790938-f909-4c8d-9304-453185350b91"  # 예시 UUID
API_BASE_URL = f"https://api.odcloud.kr/api/15080155/v1/uddi:{API_UUID}"
API_KEY_ENV = "SCHOOLINFO_SERVICE_KEY"

# API 기본 설정
DEFAULT_API_RATE_LIMIT = 5
DEFAULT_API_RETRY_MAX = 3
DEFAULT_API_RETRY_DELAY = 1.0
DEFAULT_API_TIMEOUT = 30

# 지역 코드 매핑 (시도교육청 코드 → 행안부 시도코드)
REGION_TO_SIDO = {
    "B10": "11", "C10": "26", "D10": "27", "E10": "28",
    "F10": "29", "G10": "30", "H10": "31", "I10": "36",
    "J10": "41", "K10": "51", "L10": "43", "M10": "44",
    "N10": "52", "O10": "46", "P10": "47", "Q10": "48",
    "R10": "50", "S10": "99"   # 기타는 99로 처리
}

# 학교 유형 코드 (교육청 코드와 동일)
SCHOOL_TYPES = {
    "1": "유치원", "2": "초등학교", "3": "중학교", "4": "고등학교",
    "5": "특수학교", "6": "각종학교", "7": "대학", "8": "기타"
}

# 학교급 코드 (API 에 전달)
SCHUL_KND_MAP = {
    "02": "초등학교", "03": "중학교", "04": "고등학교",
    "05": "특수학교", "06": "그외", "07": "각종학교"
}

# DB 스키마 + 인덱스 최적화
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS schools (
    school_code TEXT PRIMARY KEY,
    school_name TEXT NOT NULL,
    region_code TEXT NOT NULL,          -- 시도교육청 코드 (B10 등)
    sido_code TEXT,                      -- 행안부 시도코드 (11, 26 등)
    sgg_code TEXT,                       -- 시군구 코드 (5자리)
    region_name TEXT,
    school_type TEXT,
    school_type_name TEXT,
    address TEXT,
    zip_code TEXT,
    phone TEXT,
    homepage TEXT,
    establishment_date TEXT,
    open_date TEXT,
    close_date TEXT,
    latitude REAL,
    longitude REAL,
    collected_at TEXT NOT NULL,
    updated_at TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_schools_region ON schools(region_code);
CREATE INDEX IF NOT EXISTS idx_schools_sido ON schools(sido_code);
CREATE INDEX IF NOT EXISTS idx_schools_sgg ON schools(sgg_code);
CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(school_type);
CREATE INDEX IF NOT EXISTS idx_schools_active ON schools(is_active);
"""

# ==================== 유틸리티 함수 ====================
def resolve_path(path_str: str) -> Optional[str]:
    if not path_str:
        return None
    p = Path(path_str)
    return str(p if p.is_absolute() else BASE_DIR / p)

def encode_service_key(key: str) -> str:
    if not key or '%' in key:
        return key
    return quote_plus(key)

def region_code_to_sido(region_code: str) -> str:
    return REGION_TO_SIDO.get(region_code, "")

def get_shard_key(school_code: str, shard_mode: str) -> Optional[str]:
    if shard_mode == "none" or not school_code:
        return None
    hash_val = int(hashlib.md5(school_code.encode()).hexdigest(), 16)
    if shard_mode == "odd":
        return "odd" if hash_val % 2 == 1 else None
    elif shard_mode == "even":
        return "even" if hash_val % 2 == 0 else None
    return None

def should_process(school_code: str, shard_mode: str) -> bool:
    if shard_mode == "none":
        return True
    return get_shard_key(school_code, shard_mode) is not None

def load_config(config_path: Optional[str]) -> Dict:
    if not config_path:
        return {}
    config_file = Path(resolve_path(config_path))
    if not config_file.exists():
        logger.warning(f"설정 파일 없음: {config_file}")
        return {}
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.warning(f"설정 파일 로드 실패: {e}")
        return {}

def setup_logging(debug: bool = False, log_file: Optional[str] = None) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    if log_file is None:
        log_dir = Path(__file__).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"schoolinfo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    else:
        log_file = Path(log_file)
        log_file.parent.mkdir(exist_ok=True, parents=True)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8', mode='a'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ==================== API 클라이언트 ====================
class SchoolInfoAPI:
    def __init__(self, api_key: str, config: Dict = None):
        self.api_key = encode_service_key(api_key)
        self.config = config or {}
        self.rate_limit = self.config.get('rate_limit', DEFAULT_API_RATE_LIMIT)
        self.retry_max = self.config.get('retry_max', DEFAULT_API_RETRY_MAX)
        self.retry_delay = self.config.get('retry_delay', DEFAULT_API_RETRY_DELAY)
        self.timeout = self.config.get('timeout', DEFAULT_API_TIMEOUT)
        self.last_call_time = 0
        self.call_count = 0
        if not api_key:
            logger.warning(f"API 키 없음. 환경변수 {API_KEY_ENV} 확인")

    def _rate_limit_wait(self):
        now = time.time()
        elapsed = now - self.last_call_time
        min_interval = 1.0 / self.rate_limit
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_call_time = time.time()
        self.call_count += 1

    def _build_url(self, params: Dict[str, str]) -> str:
        base_params = {
            "serviceKey": self.api_key,
            "returnType": "JSON"
        }
        base_params.update(params)
        return f"{API_BASE_URL}?{urlencode(base_params)}"

    def _fetch_with_retry(self, url: str) -> Optional[Dict]:
        for attempt in range(self.retry_max):
            try:
                self._rate_limit_wait()
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/json'
                })
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = self.retry_delay * (2 ** attempt)
                    logger.warning(f"레이트리밋, {wait:.1f}초 대기...")
                    time.sleep(wait)
                    continue
                elif e.code == 401:
                    logger.error("인증 실패 (401): 서비스키 확인")
                    return None
                else:
                    logger.error(f"HTTP 오류 {e.code}: {e.reason}")
                    return None
            except urllib.error.URLError as e:
                logger.warning(f"네트워크 오류 ({attempt+1}/{self.retry_max}): {e.reason}")
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return None
            except Exception as e:
                logger.error(f"예외: {e}")
                return None
        logger.error("최대 재시도 초과")
        return None

    def fetch_schools(self, sido_code: str, sgg_code: str, schul_knd_code: str,
                      page: int = 1, per_page: int = 100) -> Optional[List[Dict]]:
        """
        학교알리미 API 호출 (파라미터: sidoCode, sggCode, schulKndCode)
        Returns: 학교 목록 (list) 또는 None (오류)
        """
        params = {
            "sidoCode": sido_code,
            "sggCode": sgg_code,
            "schulKndCode": schul_knd_code,
            "page": str(page),
            "perPage": str(per_page)
        }
        url = self._build_url(params)
        logger.debug(f"API 요청: sido={sido_code}, sgg={sgg_code}, knd={schul_knd_code}, page={page}")
        result = self._fetch_with_retry(url)
        if not result:
            return None

        # 다양한 응답 구조 처리
        if "data" in result and isinstance(result["data"], dict):
            # 표준 공공데이터포털 구조
            return result["data"].get("list", [])
        elif "code" in result:
            if result.get("code") == "00000":
                # code가 성공이지만 data 키가 없는 경우
                return result.get("list", [])
            else:
                logger.warning(f"API 오류 [{result.get('code')}]: {result.get('message')}")
                return []
        elif isinstance(result, list):
            # 최상위가 리스트인 경우
            return result
        else:
            logger.warning(f"예상치 못한 응답 구조: {list(result.keys())}")
            return []

# ==================== 데이터베이스 (배치 UPSERT) ====================
class SchoolDatabase:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.conn = None
        self._init_db()

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.executescript(DB_SCHEMA)
            self.conn.commit()
            logger.info(f"DB 초기화 완료: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"DB 초기화 실패: {e}")
            raise

    def upsert_batch(self, schools: List[Tuple]) -> Tuple[int, int]:
        """
        배치 UPSERT 실행
        schools: (school_code, school_name, region_code, sido_code, sgg_code,
                  region_name, school_type, school_type_name, address, zip_code,
                  phone, homepage, establishment_date, open_date, close_date,
                  latitude, longitude, collected_at, updated_at, is_active)
        반환: (변경된 행 수, 0) - 정확한 구분은 생략 (total_changes 사용)
        """
        if not schools:
            return 0, 0
        query = """
        INSERT INTO schools (
            school_code, school_name, region_code, sido_code, sgg_code,
            region_name, school_type, school_type_name, address, zip_code,
            phone, homepage, establishment_date, open_date, close_date,
            latitude, longitude, collected_at, updated_at, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(school_code) DO UPDATE SET
            school_name = excluded.school_name,
            region_code = excluded.region_code,
            sido_code = excluded.sido_code,
            sgg_code = excluded.sgg_code,
            region_name = excluded.region_name,
            school_type = excluded.school_type,
            school_type_name = excluded.school_type_name,
            address = excluded.address,
            zip_code = excluded.zip_code,
            phone = excluded.phone,
            homepage = excluded.homepage,
            establishment_date = excluded.establishment_date,
            open_date = excluded.open_date,
            close_date = excluded.close_date,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            updated_at = excluded.updated_at,
            is_active = 1
        """
        try:
            self.conn.executemany(query, schools)
            self.conn.commit()
            changes = self.conn.total_changes
            return changes, 0
        except sqlite3.Error as e:
            logger.error(f"배치 UPSERT 오류: {e}")
            self.conn.rollback()
            return 0, 0

    def get_count(self, where: str = "", params: tuple = ()) -> int:
        try:
            sql = "SELECT COUNT(*) FROM schools WHERE is_active = 1"
            if where:
                sql += f" AND {where}"
            cur = self.conn.execute(sql, params)
            return cur.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"카운트 실패: {e}")
            return 0

    def close(self):
        if self.conn:
            self.conn.close()
            logger.debug("DB 연결 종료")

# ==================== 수집기 ====================
class SchoolInfoCollector:
    def __init__(self, args: argparse.Namespace, config: Optional[Dict] = None):
        self.args = args
        self.config = config or {}
        if args.debug:
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.DEBUG)
            for h in root_logger.handlers:
                h.setLevel(logging.DEBUG)
        api_key = os.getenv(API_KEY_ENV, "")
        api_config = self.config.get('api', {})
        self.api = SchoolInfoAPI(api_key, api_config)
        db_path = args.db_path or self.config.get('db_path', 'data/school_info.db')
        resolved = resolve_path(db_path)
        if not resolved:
            raise ValueError("db_path 없음")
        self.db = SchoolDatabase(resolved)
        self.stats = {
            "total": 0, "inserted": 0, "updated": 0, "errors": 0,
            "regions": set()
        }
        self.batch_size = self.config.get('batch_size', args.batch_size or 100)
        self.batch_buffer = []

    def _flush_batch(self):
        """버퍼에 쌓인 데이터를 DB 에 저장"""
        if not self.batch_buffer:
            return
        inserted, updated = self.db.upsert_batch(self.batch_buffer)
        self.stats["total"] += inserted + updated
        self.stats["inserted"] += inserted  # 실제 구분은 어려우나 대략
        self.batch_buffer = []
        logger.debug(f"배치 저장 완료 (누적: {self.stats['total']} 건)")

    def _parse_float(self, val: Optional[str], field: str = "") -> Optional[float]:
        if not val:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            if field:
                logger.debug(f"{field} 좌표 파싱 실패: '{val}'")
            return None

    def _process_school(self, school: Dict, sido_code: str, sgg_code: str, schul_knd_code: str):
        """단일 학교 데이터 처리 (배치에 추가)"""
        school_code = school.get("SCHUL_CODE")
        if not school_code:
            logger.warning("SCHUL_CODE 없음, 건너뜀")
            self.stats["errors"] += 1
            return
        if not should_process(school_code, self.args.shard):
            return
        now = datetime.now().isoformat()

        school_name = school.get("SCHUL_NM", "")
        region_code = school.get("ATPT_OFCDC_SC_CODE", "")
        school_type = schul_knd_code  # API에서 받은 학교급 코드
        school_type_name = SCHUL_KND_MAP.get(schul_knd_code, "")
        address = school.get("SCHUL_RDNMA") or school.get("SCHUL_LNMAD") or ""
        zip_code = school.get("SCHUL_RDNZC") or ""
        phone = school.get("USER_TELNO") or ""
        homepage = school.get("HMPG_ADRES") or ""
        est_date = school.get("FOND_YMD") or ""
        open_date = school.get("OPEN_YMD") or ""
        close_date = school.get("CLOSE_YMD") or ""
        lat = self._parse_float(school.get("LTTUD"), "latitude")
        lon = self._parse_float(school.get("LGTUD"), "longitude")
        region_name = ""  # 필요시 채움

        self.batch_buffer.append((
            school_code,
            school_name,
            region_code,
            sido_code,
            sgg_code,
            region_name,
            school_type,
            school_type_name,
            address,
            zip_code,
            phone,
            homepage,
            est_date,
            open_date,
            close_date,
            lat,
            lon,
            now,
            now,
            1
        ))
        if len(self.batch_buffer) >= self.batch_size:
            self._flush_batch()

    def collect_sido_sgg(self, sido_code: str, sgg_code: str, schul_knd_codes: List[str],
                         limit: Optional[int] = None) -> int:
        """하나의 시도-시군구-학교급 조합 수집"""
        collected = 0
        for knd in schul_knd_codes:
            if limit and collected >= limit:
                break
            page = 1
            while True:
                schools = self.api.fetch_schools(sido_code, sgg_code, knd, page=page, per_page=100)
                if schools is None:
                    logger.error(f"API 실패: {sido_code}-{sgg_code}-{knd} page {page}")
                    break
                if not schools:
                    logger.debug(f"데이터 없음: {sido_code}-{sgg_code}-{knd} page {page}")
                    break
                for school in schools:
                    if limit and collected >= limit:
                        break
                    # 시도/시군구 코드 수동 주입
                    school["sidoCode"] = sido_code
                    school["sggCode"] = sgg_code
                    self._process_school(school, sido_code, sgg_code, knd)
                    collected += 1
                page += 1
                time.sleep(0.1)  # 페이지 간 휴식
                if self.args.debug and page > 1:
                    break
        return collected

    def collect_all(self, limit: Optional[int] = None) -> bool:
        # 수집 대상 시도 결정
        if self.args.regions:
            # 사용자가 시도코드 직접 지정 (예: 11,26)
            sido_list = [r.strip() for r in self.args.regions.split(',') if r.strip()]
        elif SIDO_SGG:
            sido_list = list(SIDO_SGG.keys())
        else:
            logger.error("수집할 시도코드를 지정하세요 (--regions) 또는 sgg_code_map 설치")
            return False

        # 학교급 코드
        if self.args.school_type:
            knd_list = [t.strip() for t in self.args.school_type.split(',') if t.strip()]
        else:
            knd_list = list(SCHUL_KND_MAP.keys())

        logger.info(f"수집 대상: 시도 {len(sido_list)}개, 학교급 {len(knd_list)}개")

        for sido in sido_list:
            if limit and self.stats["total"] >= limit:
                break
            # 시도에 속한 시군구 목록
            if SIDO_SGG and sido in SIDO_SGG:
                sgg_list = SIDO_SGG[sido]
            else:
                logger.warning(f"시도 {sido}의 시군구 매핑 없음, 전체('00000')로 시도")
                sgg_list = ["00000"]
            for sgg in sgg_list:
                if limit and self.stats["total"] >= limit:
                    break
                collected = self.collect_sido_sgg(sido, sgg, knd_list, limit)
                if collected:
                    logger.info(f"✓ {sido}-{sgg}: {collected} 건")
                time.sleep(0.5)  # 시군구 변경 시 휴식

        # 마지막 배치 저장
        self._flush_batch()
        return True

    def print_summary(self):
        print(f"\n{BLUE}{'='*50}{RESET}")
        print(f"{BLUE}📊 수집 결과 요약{RESET}")
        print(f"{BLUE}{'='*50}{RESET}")
        print(f"   총 처리: {self.stats['total']} 건")
        print(f"   신규/갱신: {self.stats['inserted']} 건 (변경 기준)")
        print(f"   오류: {self.stats['errors']} 건")
        print(f"   처리 지역: {len(self.stats['regions'])} 개")
        total = self.db.get_count()
        print(f"   DB 총 레코드: {total} 건")
        print(f"{BLUE}{'='*50}{RESET}\n")

    def run(self) -> int:
        start = time.time()
        logger.info(f"시작: 샤드={self.args.shard}, 제한={self.args.limit}")
        try:
            success = self.collect_all(self.args.limit)
            self.print_summary()
            logger.info(f"완료: {time.time()-start:.1f}초")
            return 0 if success else 1
        except KeyboardInterrupt:
            logger.warning("사용자 중단")
            self.print_summary()
            return 130
        except Exception as e:
            logger.error(f"수집 중 오류: {e}", exc_info=True)
            return 1
        finally:
            # 항상 미처리 배치 저장 후 종료
            self._flush_batch()
            self.db.close()

# ==================== CLI ====================
def parse_args():
    p = argparse.ArgumentParser(description="학교알리미 수집기")
    p.add_argument('--regions', '-r', help='시도코드 (쉼표 구분, 예: 11,26)')
    p.add_argument('--school-type', '-t', help='학교급코드 (쉼표 구분, 02,03,04)')
    p.add_argument('--limit', '-l', type=int, help='제한 개수')
    p.add_argument('--shard', '-s', choices=['none','odd','even'], default='none')
    p.add_argument('--debug', '-d', action='store_true')
    p.add_argument('--db-path', help='DB 경로')
    p.add_argument('--log-file', help='로그 파일')
    p.add_argument('--config', help='설정 파일')
    p.add_argument('--batch-size', type=int, default=100, help='배치 크기 (기본 100)')
    return p.parse_args()

def main():
    args = parse_args()
    config = load_config(args.config)
    if config.get('logging', {}).get('level') == 'DEBUG':
        args.debug = True
    collector = SchoolInfoCollector(args, config)
    sys.exit(collector.run())

if __name__ == "__main__":
    main()
    