#!/usr/bin/env python3
"""
학교알리미 공공데이터 수집기
- 한국교육학술정보원(KERIS) 학교알리미 공공데이터 API 연동
- 마스터 수집기 (master_collectors.py) 와 완전 호환
- 설정 기반 제어, 재시도, 샤딩, 메트릭 연동 지원
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

# 기준 디렉토리 설정
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"

# 디렉토리 생성
LOG_DIR.mkdir(exist_ok=True, parents=True)
DATA_DIR.mkdir(exist_ok=True, parents=True)

# ==================== 로깅 설정 ====================

def setup_logging(debug: bool = False, log_file: Optional[str] = None) -> logging.Logger:
    """로깅 초기화 (파일 + 콘솔)"""
    level = logging.DEBUG if debug else logging.INFO
    
    if log_file is None:
        log_file = LOG_DIR / f"school_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    else:
        log_file = Path(log_file)
        log_file.parent.mkdir(exist_ok=True, parents=True)
    
    # 기존 핸들러 제거 (중복 방지)
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

# ✅ 학교알리미 공공데이터 API 엔드포인트 (공공데이터포털 기준)
# ※ 실제 사용 시 공공데이터포털에서 발급받은 정확한 서비스 URL 로 교체 필요
# 예시: 학교기본정보 (서비스번호 15080155)
API_BASE_URL = "https://api.odcloud.kr/api/15080155/v1/uddi:b2790938-f909-4c8d-9304-453185350b91"

# 환경변수명 (NEIS 와 혼동 방지)
API_KEY_ENV = "SCHOOLINFO_SERVICE_KEY"

# 기본값 (설정 파일로 오버라이드 가능)
DEFAULT_API_RATE_LIMIT = 5       # 공공데이터는 더 엄격한 레이트 리밋
DEFAULT_API_RETRY_MAX = 3
DEFAULT_API_RETRY_DELAY = 1.0
DEFAULT_API_TIMEOUT = 30

# 지역 코드 매핑 (시도교육청 - NEIS 와 호환)
REGION_CODES = {
    "B10": "서울", "C10": "부산", "D10": "대구", "E10": "인천",
    "F10": "광주", "G10": "대전", "H10": "울산", "I10": "세종",
    "J10": "경기", "K10": "강원", "L10": "충북", "M10": "충남",
    "N10": "전북", "O10": "전남", "P10": "경북", "Q10": "경남",
    "R10": "제주", "S10": "외국"
}

# 학교 유형 코드
SCHOOL_TYPES = {
    "1": "유치원", "2": "초등학교", "3": "중학교", "4": "고등학교",
    "5": "특수학교", "6": "각종학교", "7": "대학", "8": "기타"
}

# 데이터베이스 스키마
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
    """상대 경로를 절대 경로로 변환 (None 허용)"""
    if not path_str:
        return None
    p = Path(path_str)
    return str(p if p.is_absolute() else BASE_DIR / p)

def encode_service_key(key: str) -> str:
    """
    공공데이터포털 API 키 URL 인코딩
    - 401 에러 방지를 위한 필수 처리
    - 이미 인코딩된 키는 중복 방지
    """
    if not key:
        return key
    # 이미 인코딩된 문자 (% 포함) 가 있으면 그대로 반환
    if '%' in key:
        return key
    # 특수문자 인코딩
    return quote_plus(key)

def get_shard_key(school_code: str, shard_mode: str) -> Optional[str]:
    """샤딩 키 계산 (odd/even/none)"""
    if shard_mode == "none" or not school_code:
        return None
    hash_val = int(hashlib.md5(school_code.encode()).hexdigest(), 16)
    if shard_mode == "odd":
        return "odd" if hash_val % 2 == 1 else None
    elif shard_mode == "even":
        return "even" if hash_val % 2 == 0 else None
    return None

def should_process(school_code: str, shard_mode: str) -> bool:
    """현재 샤드 모드에서 처리 대상인지 판단"""
    if shard_mode == "none":
        return True
    return get_shard_key(school_code, shard_mode) is not None

def load_config(config_path: Optional[str]) -> Dict:
    """추가 설정 파일 로드 (환경변수 치환 지원)"""
    if not config_path:
        return {}
    
    config_file = Path(resolve_path(config_path))
    if not config_file.exists():
        logger.warning(f"설정 파일 없음: {config_file}")
        return {}
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        config = _expand_env_vars(config)
        return config
    except Exception as e:
        logger.warning(f"설정 파일 로드 실패: {e}")
        return {}

def _expand_env_vars(obj: Any) -> Any:
    """딕셔너리/리스트/문자열 내 ${VAR}를 환경변수로 치환"""
    import re
    if isinstance(obj, str):
        def repl(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))
        return re.sub(r'\$\{(\w+)\}', repl, obj)
    elif isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    return obj

# ==================== API 클라이언트 ====================

class SchoolInfoAPI:
    """학교알리미 공공데이터 API 클라이언트"""
    
    def __init__(self, api_key: str, config: Dict = None):
        # ✅ 인증키 URL 인코딩 처리 (401 에러 방지)
        self.api_key = encode_service_key(api_key)
        self.config = config or {}
        
        # 설정값 적용
        self.rate_limit = self.config.get('rate_limit', DEFAULT_API_RATE_LIMIT)
        self.retry_max = self.config.get('retry_max', DEFAULT_API_RETRY_MAX)
        self.retry_delay = self.config.get('retry_delay', DEFAULT_API_RETRY_DELAY)
        self.timeout = self.config.get('timeout', DEFAULT_API_TIMEOUT)
        
        self.last_call_time = 0
        self.call_count = 0
        
        if not api_key:
            logger.warning(f"API 키가 설정되지 않았습니다. 환경변수 {API_KEY_ENV} 을 확인하세요.")
    
    def _rate_limit_wait(self):
        """API 호출 레이트 리미팅"""
        now = time.time()
        elapsed = now - self.last_call_time
        if elapsed < 1.0 / self.rate_limit:
            time.sleep((1.0 / self.rate_limit) - elapsed)
        self.last_call_time = time.time()
        self.call_count += 1
        if self.call_count % 100 == 0:
            logger.debug(f"API 호출 횟수: {self.call_count}")
    
    def _build_url(self, params: Dict[str, str]) -> str:
        """API 요청 URL 구성 (공공데이터포털 파라미터 구조)"""
        base_params = {
            "serviceKey": self.api_key,      # ← KEY → serviceKey
            "page": "1",
            "perPage": "100",                 # ← pSize → perPage
            "returnType": "JSON"              # ← Type → returnType
        }
        base_params.update(params)
        return f"{API_BASE_URL}?{urlencode(base_params)}"
    
    def _fetch_with_retry(self, url: str) -> Optional[Dict]:
        """재시도 로직 포함 API 호출"""
        for attempt in range(self.retry_max):
            try:
                self._rate_limit_wait()
                
                # ✅ User-Agent 헤더 추가 (공공데이터포털 권장)
                req = urllib.request.Request(
                    url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json'
                    }
                )
                
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    return data
                    
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    # ✅ 401 에러: 인증키 문제 상세 안내
                    logger.error(f"인증 실패 (401): 서비스 키 유효성 또는 URL 인코딩을 확인하세요")
                    logger.debug(f"키 프리픽스: {self.api_key[:15]}...")
                    return None
                elif e.code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"레이트 리밋 초과, {wait_time:.1f} 초 대기 후 재시도...")
                    time.sleep(wait_time)
                    continue
                elif e.code == 400:
                    logger.error(f"잘못된 요청 (400): 파라미터를 확인하세요 - {e.reason}")
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
                logger.warning(f"소켓 타임아웃, 재시도 {attempt + 1}/{self.retry_max}")
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return None
        
        logger.error(f"API 호출 최대 재시도 ({self.retry_max} 회) 초과")
        return None
    
    def fetch_schools(self, region_code: Optional[str] = None,
                     school_type: Optional[str] = None,
                     page: int = 1, per_page: int = 100) -> Optional[List[Dict]]:
        """
        학교 정보 조회 (공공데이터포털 학교알리미 API)
        
        Args:
            region_code: 시도교육청코드 (예: "B10"=서울)
            school_type: 학교급코드 (예: "2"=초등학교)
            page: 페이지 번호 (1 부터)
            per_page: 페이지당 결과 수 (최대 1000 권장)
        
        Returns:
            학교 정보 리스트 또는 None (실패 시)
        """
        params = {
            "page": str(page),
            "perPage": str(per_page)
        }
        
        # 필터링 파라미터 (API 명세에 따라 조정 필요)
        if region_code:
            params["ATPT_OFCDC_SC_CODE"] = region_code
        if school_type:
            params["LCLAS_SC_CODE"] = school_type
        
        url = self._build_url(params)
        logger.debug(f"API 요청: {url[:120]}...")
        
        result = self._fetch_with_retry(url)
        if not result:
            return None
        
        # ✅ 공공데이터포털 표준 응답 구조 파싱
        try:
            # 성공 응답: { "data": { "currentPage": N, "totalCount": N, "list": [...] } }
            if "data" in result and isinstance(result["data"], dict):
                schools = result["data"].get("list", [])
                
                # 페이지 정보 로깅
                total = result["data"].get("totalCount", 0)
                current = result["data"].get("currentPage", page)
                match = result["data"].get("matchCount", len(schools))
                
                logger.debug(f"페이지 {current}/{(total + per_page - 1) // per_page}: "
                           f"{len(schools)} 건 수신 (총 {total} 건, 매칭 {match} 건)")
                
                return schools
            
            # 에러 응답: { "code": "99999", "message": "..." }
            elif "code" in result and result.get("code") != "00000":
                logger.warning(f"API 응답 오류 [{result.get('code')}]: {result.get('message', 'Unknown')}")
                return []
            
            # 예상치 못한 응답 구조
            logger.warning(f"예상치 못한 응답 구조: {list(result.keys())}")
            return []
            
        except (KeyError, TypeError, AttributeError) as e:
            logger.error(f"응답 파싱 오류: {e}", exc_info=True)
            logger.debug(f"원본 응답: {json.dumps(result, ensure_ascii=False)[:200]}")
            return None

# ==================== 데이터베이스 ====================

class SchoolDatabase:
    """학교 정보 SQLite 데이터베이스"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True, parents=True)
        self.conn = None
        self._init_db()
    
    def _init_db(self):
        """데이터베이스 초기화 (스키마 생성)"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA journal_mode=WAL")  # 동시성 향상
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.executescript(DB_SCHEMA)
            self.conn.commit()
            logger.info(f"데이터베이스 초기화 완료: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"DB 초기화 실패: {e}")
            raise
    
    def upsert_school(self, school_ Dict[str, Any]) -> bool:
        """
        학교 정보 삽입 또는 업데이트 (UPSERT)
        
        Args:
            school_data: API 응답에서 파싱된 학교 정보 딕셔너리
        
        Returns:
            성공 시 True, 실패 시 False
        """
        try:
            # ✅ 필수 필드 검증 (학교알리미 API 필드명 기준)
            required = ["SCHUL_CODE", "SCHUL_NM", "ATPT_OFCDC_SC_CODE"]
            if not all(k in school_data for k in required):
                missing = [k for k in required if k not in school_data]
                logger.warning(f"필수 필드 누락 [{school_data.get('SCHUL_CODE', 'Unknown')}]: {missing}")
                return False
            
            now = datetime.now().isoformat()
            
            # ✅ UPSERT 쿼리 (SQLite 3.24+ 지원)
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
            
            # ✅ 필드 매핑 (학교알리미 API 응답 → DB 컬럼)
            # ※ 실제 API 응답 필드명에 맞게 조정 필요
            values = (
                school_data.get("SCHUL_CODE"),                    # 학교코드
                school_data.get("SCHUL_NM"),                      # 학교명
                school_data.get("ATPT_OFCDC_SC_CODE"),            # 시도교육청코드
                REGION_CODES.get(school_data.get("ATPT_OFCDC_SC_CODE")),  # 지역명
                school_data.get("LCLAS_SC_CODE"),                 # 학교급코드
                SCHOOL_TYPES.get(school_data.get("LCLAS_SC_CODE")),  # 학교급명
                school_data.get("ADDR"),                          # 주소
                school_data.get("POST"),                          # 우편번호
                school_data.get("TELNO"),                         # 전화번호
                school_data.get("HMPG_ADDR"),                     # 홈페이지
                school_data.get("FNDT_YMD"),                      # 개교연월일
                school_data.get("OPEN_YMD"),                      # 개교일자
                school_data.get("CLOSE_YMD"),                     # 폐교일자
                self._parse_coord(school_data.get("SCNL_LAT")),   # 위도
                self._parse_coord(school_data.get("SCNL_LON")),   # 경도
                now,                                               # collected_at
                now,                                               # updated_at
                1                                                  # is_active
            )
            
            self.conn.execute(query, values)
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"DB 업서트 실패 [{school_data.get('SCHUL_CODE')}]: {e}")
            self.conn.rollback()
            return False
    
    def _parse_coord(self, value: Optional[str]) -> Optional[float]:
        """좌표 문자열을 float 로 파싱"""
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def get_count(self, where_clause: str = "", params: tuple = ()) -> int:
        """레코드 수 조회 (조건부 지원)"""
        try:
            query = f"SELECT COUNT(*) FROM schools WHERE is_active = 1"
            if where_clause:
                query += f" AND {where_clause}"
            cursor = self.conn.execute(query, params)
            return cursor.fetchone()[0]
        except sqlite3.Error as e:
            logger.error(f"레코드 수 조회 실패: {e}")
            return 0
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            logger.debug("DB 연결 종료")

# ==================== 수집기 메인 로직 ====================

class SchoolInfoCollector:
    """학교알리미 정보 수집기"""
    
    def __init__(self, args: argparse.Namespace, config: Optional[Dict] = None):
        self.args = args
        self.config = config or {}
        
        # 로깅 레벨 설정 (재할당 대신 레벨만 조정)
        if args.debug:
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.DEBUG)
            for handler in root_logger.handlers:
                handler.setLevel(logging.DEBUG)
            logger.setLevel(logging.DEBUG)
        
        # API 클라이언트 초기화 (설정 전달)
        api_key = os.getenv(API_KEY_ENV, "")
        api_config = self.config.get('api', {})
        self.api = SchoolInfoAPI(api_key, api_config)
        
        # 데이터베이스 초기화
        db_path = args.db_path or self.config.get('db_path', 'data/school_info.db')
        resolved_path = resolve_path(db_path)
        if resolved_path is None:
            raise ValueError("db_path cannot be None")
        self.db = SchoolDatabase(resolved_path)
        
        # 수집 통계
        self.stats = {
            "total_processed": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "total_errors": 0,
            "regions_processed": set()
        }
    
    def collect_region(self, region_code: str, school_type: Optional[str] = None,
                      limit: Optional[int] = None) -> bool:
        """단일 지역 수집"""
        region_name = REGION_CODES.get(region_code, region_code)
        type_name = SCHOOL_TYPES.get(school_type, "전체") if school_type else "전체"
        logger.info(f"수집 시작: {region_name} (학교급: {type_name})")
        
        page = 1
        collected_count = 0
        per_page = 100  # API 권장값
        
        while True:
            # 페이지 단위 조회
            schools = self.api.fetch_schools(
                region_code=region_code,
                school_type=school_type,
                page=page,
                per_page=per_page
            )
            
            if schools is None:
                logger.error(f"지역 {region_code} 페이지 {page} 조회 실패")
                return False
            
            if not schools:
                logger.debug(f"지역 {region_code} 추가 데이터 없음 (페이지 {page})")
                break
            
            # 각 학교 처리
            for school in schools:
                school_code = school.get("SCHUL_CODE")
                
                # 샤딩 필터
                if not should_process(school_code, self.args.shard):
                    continue
                
                # 제한 개수 체크
                if limit and collected_count >= limit:
                    logger.info(f"수집 제한 도달 ({limit} 건)")
                    return True
                
                # 데이터 저장
                if self.db.upsert_school(school):
                    self.stats["total_processed"] += 1
                    collected_count += 1
                    self.stats["total_inserted"] += 1  # UPSERT 이므로 단순 카운트
                else:
                    self.stats["total_errors"] += 1
                
                # 진행 상황 로그
                if self.stats["total_processed"] % 100 == 0:
                    logger.info(f"진행: {self.stats['total_processed']} 건 처리 완료")
            
            page += 1
            
            # 디버그 모드: 1 페이지만 테스트
            if self.args.debug and page > 1:
                logger.debug("디버그 모드: 1 페이지만 수집")
                break
        
        self.stats["regions_processed"].add(region_code)
        logger.info(f"지역 {region_name} 수집 완료: {collected_count} 건")
        return True
    
    def collect_all(self, limit: Optional[int] = None) -> bool:
        """전체 지역 수집"""
        # 지역 필터 적용
        if self.args.regions:
            regions = [r.strip() for r in self.args.regions.split(',') if r.strip()]
        else:
            regions = list(REGION_CODES.keys())
        
        # 학교급 필터 적용
        school_types = None
        if self.args.school_type:
            school_types = [t.strip() for t in self.args.school_type.split(',') if t.strip()]
        
        logger.info(f"수집 대상: 지역 {len(regions)} 개, 학교급 {school_types or '전체'}")
        
        success = True
        for region in regions:
            if school_types:
                for st in school_types:
                    if not self.collect_region(region, school_type=st, limit=limit):
                        success = False
                        logger.error(f"지역 {region} 학교급 {st} 수집 실패, 계속 진행...")
            else:
                if not self.collect_region(region, limit=limit):
                    success = False
                    logger.error(f"지역 {region} 수집 실패, 계속 진행...")
        
        return success
    
    def print_summary(self):
        """수집 결과 요약 출력"""
        print(f"\n{BLUE}{'='*50}{RESET}")
        print(f"{BLUE}📊 수집 결과 요약{RESET}")
        print(f"{BLUE}{'='*50}{RESET}")
        print(f"   총 처리: {self.stats['total_processed']} 건")
        print(f"   신규/업데이트: {self.stats['total_inserted']} 건")
        print(f"   오류: {self.stats['total_errors']} 건")
        print(f"   처리 지역: {len(self.stats['regions_processed'])} 개")
        
        total_in_db = self.db.get_count()
        print(f"   DB 총 레코드: {total_in_db} 건")
        print(f"{BLUE}{'='*50}{RESET}\n")
        
        logger.info(f"수집 완료: 처리 {self.stats['total_processed']}, DB 총 {total_in_db}")
    
    def run(self) -> int:
        """수집 실행 (메인 엔트리포인트)"""
        start_time = time.time()
        logger.info(f"수집 시작: 샤드={self.args.shard}, 제한={self.args.limit}, 디버그={self.args.debug}")
        
        try:
            success = self.collect_all(limit=self.args.limit)
            self.print_summary()
            
            elapsed = time.time() - start_time
            logger.info(f"수집 완료: {elapsed:.1f} 초 소요")
            
            return 0 if success else 1
            
        except KeyboardInterrupt:
            logger.warning("사용자에 의해 수집 중단")
            print(f"\n{YELLOW}⚠️ 수집이 중단되었습니다.{RESET}")
            self.print_summary()
            return 130  # SIGINT exit code
        except Exception as e:
            logger.error(f"수집 중 예외 발생: {e}", exc_info=True)
            print(f"{RED}❌ 오류 발생: {e}{RESET}")
            return 1
        finally:
            self.db.close()

# ==================== CLI 파서 ====================

def parse_args() -> argparse.Namespace:
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="학교알리미 공공데이터 학교 기본정보 수집기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
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

환경변수:
  SCHOOLINFO_SERVICE_KEY: 공공데이터포털 서비스 키 (필수, URL 인코딩 자동 처리)
        """
    )
    
    # 수집 옵션
    parser.add_argument(
        '--regions', '-r',
        type=str,
        help='수집 대상 지역 코드 (쉼표 구분, 예: B10,J10). 기본값: 전체'
    )
    parser.add_argument(
        '--school-type', '-t',
        type=str,
        help='수집 대상 학교 유형 코드 (쉼표 구분, 예: 2,3,4). 기본값: 전체'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='수집 제한 개수 (테스트용). 기본값: 전체'
    )
    
    # 실행 모드
    parser.add_argument(
        '--shard', '-s',
        choices=['none', 'odd', 'even'],
        default='none',
        help='샤딩 모드: none(통합), odd, even. 기본값: none'
    )
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='디버그 모드 (상세 로그 출력)'
    )
    
    # 출력 옵션
    parser.add_argument(
        '--db-path',
        type=str,
        help='데이터베이스 파일 경로. 기본값: data/school_info.db'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='로그 파일 경로'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='추가 설정 파일 경로 (JSON, 환경변수 ${VAR} 치환 지원)'
    )
    
    return parser.parse_args()

# ==================== 메인 ====================

def main():
    """메인 엔트리포인트"""
    args = parse_args()
    
    # 설정 파일 로드 (환경변수 치환 포함)
    config = load_config(args.config)
    
    # 설정값을 로깅에 반영 (선택)
    if config.get('logging', {}).get('level') == 'DEBUG':
        args.debug = True
    
    # 수집기 실행
    collector = SchoolInfoCollector(args, config)
    exit_code = collector.run()
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
    