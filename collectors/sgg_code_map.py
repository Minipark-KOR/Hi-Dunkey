"""
시군구 코드 제공자 모듈
- 정적 파일 기반 / 행정안전부 API 기반 구현체 지원
- 하드코딩된 기본 매핑 포함 (파일 없을 시 fallback)
- 행정구역 개편 반영 (군위군: 27720, 대구 편입)
"""
import json
import logging
import time
import os
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urlencode
import urllib.request
import urllib.error
from collections import Counter

logger = logging.getLogger(__name__)


# ==================== 하드코딩된 기본 매핑 (fallback) ====================
# 시군구 코드 → 한글명 매핑 (앞 2자리는 시도 코드)
# 최신 행정구역 반영 (군위군: 27720, 대구 편입)
SGG_NAMES = {
    # 서울특별시 (11)
    "11110": "종로구", "11140": "중구", "11170": "용산구", "11200": "성동구", "11215": "광진구",
    "11230": "동대문구", "11260": "중랑구", "11290": "성북구", "11305": "강북구", "11320": "도봉구",
    "11350": "노원구", "11380": "은평구", "11410": "서대문구", "11440": "마포구", "11470": "양천구",
    "11500": "강서구", "11530": "구로구", "11545": "금천구", "11560": "영등포구", "11590": "동작구",
    "11620": "관악구", "11650": "서초구", "11680": "강남구", "11710": "송파구", "11740": "강동구",

    # 부산광역시 (26)
    "26110": "중구", "26140": "서구", "26170": "동구", "26200": "영도구", "26230": "부산진구",
    "26260": "동래구", "26290": "남구", "26320": "북구", "26350": "해운대구", "26380": "사하구",
    "26410": "금정구", "26440": "강서구", "26470": "연제구", "26500": "수영구", "26530": "사상구",
    "26710": "기장군",

    # 대구광역시 (27) - 군위군(27720) 대구 편입 반영
    "27110": "중구", "27140": "동구", "27170": "서구", "27200": "남구", "27230": "북구",
    "27260": "수성구", "27290": "달서구", "27710": "달성군", "27720": "군위군",

    # 인천광역시 (28)
    "28110": "중구", "28140": "동구", "28177": "미추홀구", "28185": "연수구", "28200": "남동구",
    "28237": "부평구", "28245": "계양구", "28260": "서구", "28710": "강화군", "28720": "옹진군",

    # 광주광역시 (29)
    "29110": "동구", "29140": "서구", "29155": "남구", "29170": "북구", "29200": "광산구",

    # 대전광역시 (30)
    "30110": "동구", "30140": "중구", "30170": "서구", "30200": "유성구", "30230": "대덕구",

    # 울산광역시 (31)
    "31110": "중구", "31140": "남구", "31170": "동구", "31200": "북구", "31710": "울주군",

    # 세종특별자치시 (36)
    "36110": "세종특별자치시",

    # 경기도 (41)
    "41111": "수원시 장안구", "41113": "수원시 권선구", "41115": "수원시 팔달구", "41117": "수원시 영통구",
    "41131": "성남시 수정구", "41133": "성남시 중원구", "41135": "성남시 분당구", "41150": "의정부시",
    "41171": "안양시 만안구", "41173": "안양시 동안구", "41190": "부천시", "41210": "광명시",
    "41220": "평택시", "41250": "동두천시", "41271": "안산시 상록구", "41273": "안산시 단원구",
    "41281": "고양시 덕양구", "41285": "고양시 일산동구", "41287": "고양시 일산서구", "41290": "과천시",
    "41310": "구리시", "41360": "남양주시", "41370": "오산시", "41390": "시흥시", "41410": "군포시",
    "41430": "의왕시", "41450": "하남시", "41461": "용인시 처인구", "41463": "용인시 기흥구",
    "41465": "용인시 수지구", "41480": "파주시", "41500": "이천시", "41550": "안성시",
    "41570": "김포시", "41590": "화성시", "41610": "광주시", "41630": "양주시", "41650": "포천시",
    "41670": "여주시", "41800": "연천군", "41820": "가평군", "41830": "양평군",

    # 강원특별자치도 (51)
    "51110": "춘천시", "51130": "원주시", "51150": "강릉시", "51170": "동해시", "51190": "태백시",
    "51210": "속초시", "51230": "삼척시", "51720": "홍천군", "51730": "횡성군", "51750": "영월군",
    "51760": "평창군", "51770": "정선군", "51780": "철원군", "51790": "화천군", "51800": "양구군",
    "51810": "인제군", "51820": "고성군", "51830": "양양군",

    # 충청북도 (43)
    "43111": "청주시 상당구", "43112": "청주시 서원구", "43113": "청주시 흥덕구", "43114": "청주시 청원구",
    "43130": "충주시", "43150": "제천시", "43720": "보은군", "43730": "옥천군", "43740": "영동군",
    "43745": "증평군", "43750": "진천군", "43760": "괴산군", "43770": "음성군", "43800": "단양군",

    # 충청남도 (44)
    "44131": "천안시 동남구", "44133": "천안시 서북구", "44150": "공주시", "44180": "보령시",
    "44200": "아산시", "44210": "서산시", "44230": "논산시", "44250": "계룡시", "44270": "당진시",
    "44710": "금산군", "44760": "부여군", "44770": "서천군", "44790": "청양군", "44800": "홍성군",
    "44810": "예산군", "44825": "태안군",

    # 전북특별자치도 (52)
    "52111": "전주시 완산구", "52113": "전주시 덕진구", "52130": "군산시", "52140": "익산시",
    "52180": "정읍시", "52190": "남원시", "52210": "김제시", "52710": "완주군", "52720": "진안군",
    "52730": "무주군", "52740": "장수군", "52750": "임실군", "52770": "순창군", "52790": "고창군",
    "52800": "부안군",

    # 전라남도 (46)
    "46110": "목포시", "46130": "여수시", "46150": "순천시", "46170": "나주시", "46230": "광양시",
    "46710": "담양군", "46720": "곡성군", "46730": "구례군", "46770": "고흥군", "46780": "보성군",
    "46790": "화순군", "46800": "장흥군", "46810": "강진군", "46820": "해남군", "46830": "영암군",
    "46840": "무안군", "46860": "함평군", "46870": "영광군", "46880": "장성군", "46890": "완도군",
    "46900": "진도군", "46910": "신안군",

    # 경상북도 (47) - 군위군 제거됨 (대구로 이관)
    "47110": "포항시", "47111": "포항시 남구", "47113": "포항시 북구", "47130": "경주시", "47150": "김천시",
    "47170": "안동시", "47190": "구미시", "47210": "영주시", "47230": "영천시", "47250": "상주시",
    "47280": "문경시", "47290": "경산시", "47720": "의성군", "47730": "청송군", "47750": "영양군",
    "47760": "영덕군", "47770": "청도군", "47820": "고령군", "47830": "성주군", "47840": "칠곡군",
    "47850": "예천군", "47900": "봉화군", "47920": "울진군", "47930": "울릉군",

    # 경상남도 (48)
    "48120": "창원시", "48121": "창원시 의창구", "48123": "창원시 성산구", "48125": "창원시 마산합포구",
    "48127": "창원시 마산회원구", "48129": "창원시 진해구", "48170": "진주시", "48220": "통영시",
    "48240": "사천시", "48250": "김해시", "48270": "밀양시", "48310": "거제시", "48330": "양산시",
    "48720": "의령군", "48730": "함안군", "48740": "창녕군", "48820": "고성군", "48840": "남해군",
    "48850": "하동군", "48860": "산청군", "48870": "함양군", "48880": "거창군", "48890": "합천군",

    # 제주특별자치도 (50)
    "50110": "제주시", "50130": "서귀포시",
}


# ==================== 유틸리티 함수 (매핑 데이터 직접 접근용) ====================

def get_sgg_name(code: str, default: Optional[str] = None) -> str:
    """
    시군구 코드를 한글 명칭으로 변환합니다.
    
    Args:
        code: 5자리 시군구 코드 (예: "11110")
        default: 코드가 없을 때 반환할 기본값 (None이면 "알 수 없는 지역")
    
    Returns:
        시군구 이름 (예: "종로구")
    """
    code_str = str(code).strip()
    name = SGG_NAMES.get(code_str)
    if name is not None:
        return name
    if default is not None:
        return default
    return "알 수 없는 지역"


def get_sido_code(code: str) -> str:
    """
    시군구 코드에서 앞 2자리를 추출하여 시도 코드를 반환합니다.
    
    Args:
        code: 5자리 시군구 코드
    
    Returns:
        2자리 시도 코드 (예: "11")
    """
    code_str = str(code).strip()
    return code_str[:2] if len(code_str) >= 2 else ""


def is_valid_sgg(code: str) -> bool:
    """
    유효한 시군구 코드인지 확인합니다.
    
    Args:
        code: 5자리 시군구 코드
    
    Returns:
        True if code exists in SGG_NAMES
    """
    return str(code).strip() in SGG_NAMES


def get_sgg_code(name: str, sido_code: Optional[str] = None) -> Optional[str]:
    """
    시군구 이름으로 코드를 찾습니다. (역방향 조회)
    주의: 같은 이름의 구가 여러 시도에 존재할 수 있으므로 sido_code를 함께 제공하면 정확도가 높아집니다.
    중복 발생 시 첫 번째 코드를 반환하고 경고를 로깅합니다.
    
    Args:
        name: 시군구 이름 (예: "중구")
        sido_code: 선택적 시도 코드 (예: "11")
    
    Returns:
        시군구 코드 또는 None
    """
    candidates = []
    for code, n in SGG_NAMES.items():
        if n == name:
            if sido_code is None or code.startswith(sido_code):
                candidates.append(code)
    
    if not candidates:
        return None
    if len(candidates) > 1:
        logger.warning(f"시군구명 '{name}'이(가) 중복됨: {candidates}. sido_code={sido_code}로 필터링했으나 여전히 다수. 첫 번째({candidates[0]}) 반환.")
    return candidates[0]


# ==================== 추가 편의 함수 ====================

def get_sido_counts() -> Counter:
    """시도별 시군구 개수 반환 (통계용)"""
    return Counter(code[:2] for code in SGG_NAMES.keys())


def search_sgg(keyword: str) -> List[tuple]:
    """키워드가 포함된 시군구 목록 반환 (code, name)"""
    return [(code, name) for code, name in SGG_NAMES.items() if keyword in name]


# ==================== 인터페이스 (SGGCodeProvider) ====================

class SGGCodeProvider(ABC):
    """시군구 코드 제공자 인터페이스"""
    
    @abstractmethod
    def get_codes_by_sido(self, sido_code: str) -> List[Dict[str, str]]:
        """
        시도코드에 해당하는 시군구 목록 조회
        Returns: [{"sgg_code": "11110", "sgg_name": "종로구"}, ...]
        """
        pass
    
    @abstractmethod
    def get_all_codes(self) -> Dict[str, str]:
        """
        전체 시군구 코드 매핑 조회
        Returns: {"11110": "종로구", "11140": "중구", ...}
        """
        pass
    
    @abstractmethod
    def refresh(self) -> bool:
        """데이터 새로고침 (동적 소스용)"""
        pass


class StaticSGGProvider(SGGCodeProvider):
    """정적 JSON 파일 기반 시군구 코드 제공자 (fallback 포함)"""
    
    def __init__(self, filepath: str, auto_create: bool = False, use_fallback: bool = True):
        """
        Args:
            filepath: JSON 파일 경로
            auto_create: 파일 없을 때 자동 생성 시도 여부 (Fallback 사용 시에도)
            use_fallback: 파일 없을 때 하드코딩된 기본 매핑 사용 여부
        """
        self.filepath = Path(filepath)
        self.use_fallback = use_fallback
        self._cache: Optional[Dict[str, str]] = None
        self._sido_index: Optional[Dict[str, List[str]]] = None
        
        # 파일이 없고 auto_create가 True이면 빈 파일 생성 (fallback 사용 시에도)
        if auto_create and not self.filepath.exists():
            self.filepath.parent.mkdir(exist_ok=True, parents=True)
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump({}, f)
            logger.info(f"빈 매핑 파일 생성: {self.filepath}")
        
        # 파일이 없고 fallback을 사용한다면 기본 매핑으로 캐시 초기화
        if not self.filepath.exists() and self.use_fallback:
            logger.warning(f"매핑 파일 없음: {self.filepath}, fallback 사용")
            self._cache = SGG_NAMES.copy()
            self._build_sido_index()
            logger.info(f"Fallback 매핑 사용: {len(self._cache)} 개 코드")
    
    def _load_data(self) -> Dict[str, str]:
        """데이터 로드 (캐싱, 파일 우선, fallback 차선)"""
        if self._cache is not None:
            return self._cache
        
        # 파일이 존재하면 파일에서 로드
        if self.filepath.exists():
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                logger.info(f"시군구 매핑 로드 완료: {self.filepath} ({len(self._cache)} 개 코드)")
                self._build_sido_index()
                return self._cache
            except Exception as e:
                logger.error(f"매핑 파일 로드 실패: {e}, fallback 시도")
        
        # 파일이 없거나 로드 실패 시 fallback 사용
        if self.use_fallback:
            logger.warning("파일 로드 실패, fallback 매핑 사용")
            self._cache = SGG_NAMES.copy()
            self._build_sido_index()
            return self._cache
        
        # fallback도 없으면 예외
        raise FileNotFoundError(f"시군구 매핑을 찾을 수 없음: {self.filepath}")
    
    def _build_sido_index(self):
        """시도별 시군구 코드 인덱스 생성"""
        self._sido_index = {}
        data = self._cache if self._cache is not None else {}
        for sgg_code, sgg_name in data.items():
            sido_code = sgg_code[:2]
            if sido_code not in self._sido_index:
                self._sido_index[sido_code] = []
            self._sido_index[sido_code].append(sgg_code)
    
    def get_codes_by_sido(self, sido_code: str) -> List[Dict[str, str]]:
        data = self._load_data()
        # 인덱스가 없으면 생성 (이미 _load_data에서 생성했을 수 있지만, 안전장치)
        if self._sido_index is None:
            self._build_sido_index()
        sgg_codes = self._sido_index.get(sido_code, [])
        return [{"sgg_code": code, "sgg_name": data[code]} for code in sgg_codes]
    
    def get_all_codes(self) -> Dict[str, str]:
        return self._load_data().copy()
    
    def refresh(self) -> bool:
        """파일이 존재하면 다시 로드, 없으면 fallback"""
        if self.filepath.exists():
            self._cache = None
            self._sido_index = None
            self._load_data()
            return True
        elif self.use_fallback:
            # fallback 사용 중이면 캐시 초기화 후 재로드
            self._cache = None
            self._sido_index = None
            self._load_data()
            return True
        return False


class APISGGProvider(SGGCodeProvider):
    """
    행정안전부 공공코드포털 API 기반 제공자
    (실제 구현: https://www.code.go.kr/api/openapi.do)
    """
    BASE_URL = "https://www.code.go.kr/code/api.do"
    
    def __init__(self, api_key: str, config: Optional[Dict] = None):
        self.api_key = api_key
        self.config = config or {}
        self.rate_limit = self.config.get('rate_limit', 2)
        self.retry_max = self.config.get('retry_max', 3)
        self.retry_delay = self.config.get('retry_delay', 1.0)
        self.timeout = self.config.get('timeout', 30)
        self._cache: Optional[Dict[str, str]] = None
        self._last_fetch: Optional[float] = None
        self._cache_ttl = self.config.get('cache_ttl', 3600)
        if not api_key:
            logger.warning("행정안전부 API 키가 설정되지 않았습니다.")
    
    def _rate_limit_wait(self):
        now = time.time()
        if self._last_fetch:
            elapsed = now - self._last_fetch
            min_interval = 1.0 / self.rate_limit
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_fetch = time.time()
    
    def _fetch_with_retry(self, params: Dict[str, str]) -> Optional[Dict]:
        url = f"{self.BASE_URL}?{urlencode(params)}"
        for attempt in range(self.retry_max):
            try:
                self._rate_limit_wait()
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    raw = response.read().decode('utf-8')
                    # XML 응답 처리
                    root = ET.fromstring(raw)
                    err_code = root.findtext('errorCode')
                    if err_code and err_code != '0':
                        err_msg = root.findtext('errorMessage', '알 수 없는 오류')
                        logger.error(f"API 오류 [{err_code}]: {err_msg}")
                        return None
                    items = root.findall('.//cmmnCode')
                    result = []
                    for item in items:
                        result.append({
                            'code': item.findtext('code'),
                            'name': item.findtext('name'),
                            'upperCode': item.findtext('upperCode')
                        })
                    return {'items': result}
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = self.retry_delay * (2 ** attempt)
                    logger.warning(f"레이트 리밋, {wait:.1f}초 대기...")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP 오류 {e.code}: {e.reason}")
                return None
            except Exception as e:
                logger.warning(f"API 호출 실패 ({attempt+1}/{self.retry_max}): {e}")
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                return None
        return None
    
    def _fetch_all_sgg(self) -> Dict[str, str]:
        params = {
            'apiKey': self.api_key,
            'clCode': 'SIG',      # 시군구 코드
            'pageSize': 1000,
            'pageIndex': 1,
            'resultType': 'xml'
        }
        resp = self._fetch_with_retry(params)
        if not resp or 'items' not in resp:
            return {}
        sgg_map = {}
        for item in resp['items']:
            code = item.get('code', '')
            name = item.get('name', '')
            upper = item.get('upperCode', '')
            if len(code) == 5 and len(upper) == 2:
                sgg_map[code] = name
        return sgg_map
    
    def _load_cache(self) -> Dict[str, str]:
        now = time.time()
        if self._cache is not None and self._last_fetch and (now - self._last_fetch) < self._cache_ttl:
            return self._cache
        self._cache = self._fetch_all_sgg()
        self._last_fetch = now
        return self._cache or {}
    
    def get_codes_by_sido(self, sido_code: str) -> List[Dict[str, str]]:
        data = self._load_cache()
        result = []
        for code, name in data.items():
            if code.startswith(sido_code):
                result.append({"sgg_code": code, "sgg_name": name})
        return result
    
    def get_all_codes(self) -> Dict[str, str]:
        return self._load_cache().copy()
    
    def refresh(self) -> bool:
        self._cache = None
        self._last_fetch = None
        return bool(self._load_cache())


class HybridSGGProvider(SGGCodeProvider):
    """정적 파일 우선 + API 폴백 하이브리드 제공자"""
    
    def __init__(self, static_path: str, api_key: Optional[str] = None, 
                 config: Optional[Dict] = None, use_fallback: bool = True,
                 save_static_on_refresh: bool = False):
        """
        Args:
            static_path: 정적 파일 경로
            api_key: API 키
            config: API 설정
            use_fallback: 정적 파일 없을 시 SGG_NAMES 사용 여부
            save_static_on_refresh: API 갱신 시 정적 파일로 저장할지 여부
        """
        self.static_path = static_path
        self.api_key = api_key
        self.config = config or {}
        self.use_fallback = use_fallback
        self.save_static_on_refresh = save_static_on_refresh
        self._static: Optional[StaticSGGProvider] = None
        self._api: Optional[APISGGProvider] = None
        
        # 정적 프로바이더 초기화 (fallback 허용)
        try:
            self._static = StaticSGGProvider(static_path, use_fallback=use_fallback)
            self._static.get_all_codes()  # 로드 테스트
            logger.info("정적 시군구 매핑 사용 중")
        except Exception as e:
            logger.warning(f"정적 매핑 로드 실패: {e}, API 폴백 준비")
            if api_key:
                self._api = APISGGProvider(api_key, config)
    
    def _ensure_api(self):
        if self._api is None and self.api_key:
            self._api = APISGGProvider(self.api_key, self.config)
    
    def get_codes_by_sido(self, sido_code: str) -> List[Dict[str, str]]:
        if self._static:
            try:
                return self._static.get_codes_by_sido(sido_code)
            except Exception as e:
                logger.warning(f"정적 파일 조회 실패, 폴백: {e}")
        self._ensure_api()
        if self._api:
            return self._api.get_codes_by_sido(sido_code)
        logger.error("시군구 코드 조회 실패: 정적 파일도, API도 사용 불가")
        return []
    
    def get_all_codes(self) -> Dict[str, str]:
        if self._static:
            try:
                return self._static.get_all_codes()
            except:
                pass
        self._ensure_api()
        if self._api:
            return self._api.get_all_codes()
        return {}
    
    def refresh(self) -> bool:
        self._ensure_api()
        if self._api:
            success = self._api.refresh()
            if success and self.save_static_on_refresh:
                self._save_to_static(self._api.get_all_codes())
            return success
        elif self._static:
            return self._static.refresh()
        return False
    
    def _save_to_static(self, data: Dict[str, str]):
        try:
            path = Path(self.static_path)
            path.parent.mkdir(exist_ok=True, parents=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"시군구 매핑 파일 저장: {path}")
        except Exception as e:
            logger.error(f"정적 파일 저장 실패: {e}")


def create_provider(config: Dict[str, Any]) -> SGGCodeProvider:
    """설정에 따라 적절한 제공자 생성"""
    provider_type = config.get('type', 'static')
    
    if provider_type == 'static':
        return StaticSGGProvider(
            filepath=config.get('filepath', 'config/sgg_codes.json'),
            auto_create=config.get('auto_create', False),
            use_fallback=config.get('use_fallback', True)
        )
    elif provider_type == 'api':
        api_key = config.get('api_key') or os.getenv('ADMIN_API_KEY')
        if not api_key:
            raise ValueError("API 제공자 사용을 위해서는 api_key 또는 ADMIN_API_KEY 환경변수가 필요합니다.")
        return APISGGProvider(api_key, config.get('api_config', {}))
    elif provider_type == 'hybrid':
        api_key = config.get('api_key') or os.getenv('ADMIN_API_KEY')
        return HybridSGGProvider(
            static_path=config.get('static_filepath', 'config/sgg_codes.json'),
            api_key=api_key,
            config=config.get('api_config', {}),
            use_fallback=config.get('use_fallback', True),
            save_static_on_refresh=config.get('save_static_on_refresh', False)
        )
    else:
        raise ValueError(f"알 수 없는 provider_type: {provider_type}")
        