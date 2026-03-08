#!/usr/bin/env python3
"""
학교알리미 오픈서비스 수집기
- NEIS 학교알리미 API 를 활용한 학교 기본정보 수집
- 마스터 수집기 (master_collectors.py) 와 연동 가능한 구조
- 설정 파일을 통한 API 타임아웃/재시도/레이트리밋 제어
"""
import os
import sys
import json
import time
import logging
import sqlite3
import hashlib
import argparse
import socket
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urlencode
import urllib.request
import urllib.error

# ANSI 색상 (콘솔 출력용)
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"

LOG_DIR.mkdir(exist_ok=True, parents=True)
DATA_DIR.mkdir(exist_ok=True, parents=True)

def setup_logging(debug: bool = False, log_file: Optional[str] = None):
    level = logging.DEBUG if debug else logging.INFO
    if log_file is None:
        log_file = LOG_DIR / f"school_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

# ==================== 설정 및 상수 ====================

API_BASE_URL = "https://open.neis.go.kr/hub/schoolInfo"
API_KEY_ENV = "SCHOOL_INFO_API_KEY"
DEFAULT_API_KEY = os.getenv(API_KEY_ENV, "")

DEFAULT_API_RATE_LIMIT = 10
DEFAULT_API_RETRY_MAX = 3
DEFAULT_API_RETRY_DELAY = 1.0
DEFAULT_API_TIMEOUT = 30

REGION_CODES = {
    "B10": "서울", "C10": "부산", "D10": "대구", "E10": "인천",
    "F10": "광주", "G10": "대전", "H10": "울산", "I10": "세종",
    "J10": "경기", "K10": "강원", "L10": "충북", "M10": "충남",
    "N10": "전북", "O10": "전남", "P10": "경북", "Q10": "경남",
    "R10": "제주", "S10": "외국"
}

SCHOOL_TYPES = {
    "1": "유치원", "2": "초등학교", "3": "중학교", "4": "고등학교",
    "5": "특수학교", "6": "각종학교", "7": "대학", "8": "기타"
}

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS schools (
    school_code TEXT PRIMARY KEY,
    school_name TEXT NOT NULL,
    region_code TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_schools_type ON schools(school_type);
CREATE INDEX IF NOT EXISTS idx_schools_active ON schools(is_active);
"""

# ==================== 유틸리티 함수 ====================

def resolve_path(path_str: str) -> Optional[str]:
    if not path_str:
        return None
    p = Path(path_str)
    return str(p if p.is_absolute() else BASE_DIR / p)

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

def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, str):
        import re
        def repl(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))
        return re.sub(r'\$\{(\w+)\}', repl, obj)
    elif isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    else:
        return obj

def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    if not config_path:
        return {}
    config_file = Path(resolve_path(config_path))
    if not config_file.exists():
        logger.warning(f"설정 파일 없음: {config_file}")
        return {}
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return _expand_env_vars(config)
    except Exception as e:
        logger.warning(f"설정 파일 로드 실패: {e}")
        return {}

# ==================== API 클라이언트 ====================

class SchoolInfoAPI:
    def __init__(self, api_key: str, config: Dict[str, Any] = None):
        self.api_key = api_key
        self.config = config or {}
        
        self.rate_limit = self.config.get('rate_limit', DEFAULT_API_RATE_LIMIT)
        self.retry_max = self.config.get('retry_max', DEFAULT_API_RETRY_MAX)
        self.retry_delay = self.config.get('retry_delay', DEFAULT_API_RETRY_DELAY)
        self.timeout = self.config.get('timeout', DEFAULT_API_TIMEOUT)
        
        self.last_call_time = 0
        self.call_count = 0
        
        if not api_key:
            logger.warning(f"API 키가 설정되지 않았습니다. 환경변수 {API_KEY_ENV} 을 확인하세요.")
    
    def _rate_limit_wait(self):
        now = time.time()
        elapsed = now - self.last_call_time
        if elapsed < 1.0 / self.rate_limit:
            sleep_time = (1.0 / self.rate_limit) - elapsed
            time.sleep(sleep_time)
        self.last_call_time = time.time()
        self.call_count += 1
        if self.call_count % 100 == 0:
            logger.debug(f"API 호출 횟수: {self.call_count}")
    
    def _build_url(self, params: Dict[str, str]) -> str:
        base_params = {
            "KEY": self.api_key,
            "Type": "json",
            "pIndex": "1",
            "pSize": "1000"
        }
        base_params.update(params)
        return f"{API_BASE_URL}?{urlencode(base_params)}"
    
    def _fetch_with_retry(self, url: str) -> Optional[Dict]:
        for attempt in range(self.retry_max):
            try:
                self._rate_limit_wait()
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    return data
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"레이트 리밋 초과, {wait_time:.1f} 초 대기 후 재시도...")
                    time.sleep(wait_time)
                    continue
                elif e.code == 401:
                    logger.error(f"API 인증 실패: 키를 확인하세요")
                    return None
                else:
                    logger.error(f"HTTP 오류 {e.code}: {e.reason}")
                    return None
            except urllib.error.URLError as e:
                logger.warning(f"네트워크 오류, 재시도 {attempt + 1}/{self.retry_max}: {e.reason}")
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return None
            except json.JSONDecodeError as e:
                logger.error(f"JSON 파싱 오류: {e}")
                return None
            except socket.timeout:
                logger.warning(f"타임아웃, 재시도 {attempt + 1}/{self.retry_max}")
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return None
        logger.error(f"API 호출 최대 재시도 초과")
        return None
    
    def fetch_schools(self, region_code: str, school_type: Optional[str] = None, 
                     page: int = 1, page_size: int = 1000) -> Optional[List[Dict]]:
        params = {
            "ATPT_OFCDC_SC_CODE": region_code,
            "pIndex": str(page),
            "pSize": str(page_size)
        }
        if school_type:
            params["LCLAS_SC_CODE"] = school_type
        url = self._build_url(params)
        logger.debug(f"API 요청: {url[:100]}...")
        result = self._fetch_with_retry(url)
        if not result:
            return None
        try:
            if "schoolInfo" in result and len(result["schoolInfo"]) > 1:
                rows = result["schoolInfo"][1].get("row", [])
                return rows
            elif "schoolInfo" in result and len(result["schoolInfo"]) > 0:
                error_row = result["schoolInfo"][0].get("row", [{}])[0]
                if error_row.get("RESULT") and error_row["RESULT"].get("CODE") != "INFO-000":
                    logger.warning(f"API 응답 오류: {error_row.get('RESULT', {}).get('MESSAGE', 'Unknown')}")
                    return []
            return []
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"응답 파싱 오류: {e}")
            return None

# ==================== 데이터베이스 ====================

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
            logger.info(f"데이터베이스 초기화 완료: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"DB 초기화 실패: {e}")
            raise
    
    def upsert_school(self, school_data: Dict[str, Any]) -> bool:
        try:
            required = ["SD_SCHUL_CODE", "SCHUL_NM", "ATPT_OFCDC_SC_CODE"]
            if not all(k in school_data for k in required):
                logger.warning(f"필수 필드 누락: {school_data.get('SD_SCHUL_CODE', 'Unknown')}")
                return False
            now = datetime.now().isoformat()
            query = """
            INSERT INTO schools (
                school_code, school_name, region_code, region_name,
                school_type, school_type_name, address, zip_code,
                phone, homepage, establishment_date, open_date, close_date,
                latitude, longitude, collected_at, updated_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(school_code) DO UPDATE SET
                school_name = excluded.school_name,
                region_name = excluded.region_name,
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
            values = (
                school_data.get("SD_SCHUL_CODE"),
                school_data.get("SCHUL_NM"),
                school_data.get("ATPT_OFCDC_SC_CODE"),
                REGION_CODES.get(school_data.get("ATPT_OFCDC_SC_CODE")),
                school_data.get("LCLAS_SC_CODE"),
                SCHOOL_TYPES.get(school_data.get("LCLAS_SC_CODE")),
                school_data.get("ADDR"),
                school_data.get("POST"),
                school_data.get("TELNO"),
                school_data.get("HMPG_ADDR"),
                school_data.get("FNDT_YMD"),
                school_data.get("OPEN_YMD"),
                school_data.get("CLOSE_YMD"),
                self._parse_coord(school_data.get("SCNL_LAT")),
                self._parse_coord(school_data.get("SCNL_LON")),
                now, now, 1
            )
            self.conn.execute(query, values)
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"DB 업서트 실패 {school_data.get('SD_SCHUL_CODE')}: {e}")
            self.conn.rollback()
            return False
    
    def _parse_coord(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def get_count(self) -> int:
        try:
            cur = self.conn.execute("SELECT COUNT(*) FROM schools WHERE is_active = 1")
            return cur.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"레코드 수 조회 실패: {e}")
            return 0
    
    def get_count_by_region(self, region_code: str) -> int:
        try:
            cur = self.conn.execute(
                "SELECT COUNT(*) FROM schools WHERE region_code = ? AND is_active = 1",
                (region_code,)
            )
            return cur.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"지역별 카운트 실패: {e}")
            return 0
    
    def close(self):
        if self.conn:
            self.conn.close()
            logger.debug("DB 연결 종료")

# ==================== 수집기 메인 로직 ====================

class SchoolInfoCollector:
    def __init__(self, args: argparse.Namespace, config: Optional[Dict[str, Any]] = None):
        self.args = args
        self.config = config or {}
        if args.debug:
            global logger
            logger = setup_logging(debug=True)
        
        api_key = os.getenv(API_KEY_ENV, DEFAULT_API_KEY)
        api_config = self.config.get('api', {})
        self.api = SchoolInfoAPI(api_key, api_config)
        
        db_path = args.db_path or self.config.get('db_path', 'data/school_info.db')
        self.db = SchoolDatabase(resolve_path(db_path))
        
        self.stats = {
            "total_processed": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "total_errors": 0,
            "regions_processed": set()
        }
    
    def collect_region(self, region_code: str, school_type: Optional[str] = None, 
                      limit: Optional[int] = None) -> bool:
        logger.info(f"수집 시작: {REGION_CODES.get(region_code, region_code)} (타입: {school_type or '전체'})")
        page = 1
        collected_count = 0
        
        while True:
            schools = self.api.fetch_schools(region_code, school_type, page=page)
            if schools is None:
                logger.error(f"지역 {region_code} 페이지 {page} 조회 실패")
                return False
            if not schools:
                logger.debug(f"지역 {region_code} 추가 데이터 없음 (페이지 {page})")
                break
            
            for school in schools:
                school_code = school.get("SD_SCHUL_CODE")
                if not should_process(school_code, self.args.shard):
                    continue
                if limit and collected_count >= limit:
                    logger.info(f"수집 제한 도달 ({limit} 건)")
                    return True
                
                if self.db.upsert_school(school):
                    self.stats["total_processed"] += 1
                    collected_count += 1
                    self.stats["total_inserted"] += 1
                else:
                    self.stats["total_errors"] += 1
                
                if self.stats["total_processed"] % 100 == 0:
                    logger.info(f"진행: {self.stats['total_processed']} 건 처리 완료")
            
            page += 1
            if self.args.debug and page > 1:
                logger.debug("디버그 모드: 1 페이지만 수집")
                break
        
        self.stats["regions_processed"].add(region_code)
        logger.info(f"지역 {region_code} 수집 완료: {collected_count} 건")
        return True
    
    def collect_all(self, limit: Optional[int] = None) -> bool:
        regions = self.args.regions or list(REGION_CODES.keys())
        logger.info(f"수집 대상 지역: {', '.join(REGION_CODES.get(r, r) for r in regions)}")
        success = True
        for region in regions:
            if not self.collect_region(region, limit=limit):
                success = False
                logger.error(f"지역 {region} 수집 실패, 계속 진행...")
        return success
    
    def print_summary(self):
        print(f"\n{BLUE}{'='*50}{RESET}")
        print(f"{BLUE}📊 수집 결과 요약{RESET}")
        print(f"{BLUE}{'='*50}{RESET}")
        print(f"   총 처리: {self.stats['total_processed']} 건")
        print(f"   신규 삽입: {self.stats['total_inserted']} 건")
        print(f"   업데이트: {self.stats['total_updated']} 건")
        print(f"   오류: {self.stats['total_errors']} 건")
        print(f"   처리 지역: {len(self.stats['regions_processed'])} 개")
        total_in_db = self.db.get_count()
        print(f"   DB 총 레코드: {total_in_db} 건")
        print(f"{BLUE}{'='*50}{RESET}\n")
        logger.info(f"수집 완료: 처리 {self.stats['total_processed']}, DB 총 {total_in_db}")
    
    def run(self) -> int:
        start_time = time.time()
        logger.info(f"수집 시작: 샤드 모드={self.args.shard}, 제한={self.args.limit}")
        try:
            if self.args.regions:
                success = self.collect_all(limit=self.args.limit)
            else:
                success = self.collect_all(limit=self.args.limit)
            self.print_summary()
            elapsed = time.time() - start_time
            logger.info(f"수집 완료: {elapsed:.1f} 초 소요")
            return 0 if success else 1
        except KeyboardInterrupt:
            logger.warning("사용자에 의해 수집 중단")
            print(f"\n{YELLOW}⚠️ 수집이 중단되었습니다.{RESET}")
            self.print_summary()
            return 130
        except Exception as e:
            logger.error(f"수집 중 예외 발생: {e}", exc_info=True)
            print(f"{RED}❌ 오류 발생: {e}{RESET}")
            return 1
        finally:
            self.db.close()

# ==================== CLI 파서 ====================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="학교알리미 오픈서비스 학교 기본정보 수집기",
        epilog="""
예시:
  # 전체 지역 수집
  %(prog)s
  # 서울, 경기 지역만 수집
  %(prog)s --regions B10,J10
  # 테스트 모드 (50 건 제한 + 디버그)
  %(prog)s --limit 50 --debug
  # odd 샤드만 수집 (병렬 처리용)
  %(prog)s --shard odd
  # 중고등학교만 수집
  %(prog)s --school-type 3,4
        """
    )
    parser.add_argument('--regions', '-r', type=str, help='수집 대상 지역 코드 (쉼표 구분, 예: B10,J10)')
    parser.add_argument('--school-type', '-t', type=str, help='수집 대상 학교 유형 코드 (쉼표 구분, 예: 2,3,4)')
    parser.add_argument('--limit', '-l', type=int, help='수집 제한 개수 (테스트용)')
    parser.add_argument('--shard', '-s', choices=['none', 'odd', 'even'], default='none', help='샤딩 모드: none(통합), odd, even. 기본값: none')
    parser.add_argument('--debug', '-d', action='store_true', help='디버그 모드 (상세 로그 출력)')
    parser.add_argument('--db-path', type=str, help='데이터베이스 파일 경로. 기본값: data/school_info.db')
    parser.add_argument('--log-file', type=str, help='로그 파일 경로')
    parser.add_argument('--config', type=str, help='추가 설정 파일 경로 (JSON)')
    return parser.parse_args()

# ==================== 메인 ====================

def main():
    args = parse_args()
    config = load_config(args.config)
    collector = SchoolInfoCollector(args, config)
    sys.exit(collector.run())

if __name__ == "__main__":
    main()
