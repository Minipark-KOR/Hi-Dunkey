#!/usr/bin/env python3
"""
네트워크 요청 공통 모듈 (재시도, 세션 관리)
"""
import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any

# API 설정 (constants/codes.py 에서 import 가능)
API_CONFIG = {
    'timeout': 20,
    'max_retries': 3,
    'backoff': 1.0,
    'retry_status': [429, 500, 502, 503, 504],
}

def build_session() -> requests.Session:
    """재시도 기능이 내장된 세션 생성"""
    session = requests.Session()
    retry = Retry(
        total=API_CONFIG['max_retries'],
        backoff_factor=API_CONFIG['backoff'],
        status_forcelist=API_CONFIG['retry_status'],
        allowed_methods=['GET'],
        raise_on_status=False
    )
    session.mount('https://', HTTPAdapter(max_retries=retry))
    session.mount('http://', HTTPAdapter(max_retries=retry))
    return session

def safe_json_request(session: requests.Session, url: str, params: dict,
                      logger=None) -> Optional[Dict[str, Any]]:
    """
    안전한 JSON 요청
    - HTTP 에러 시 None 반환 (logger에 경고)
    - JSON 디코딩 실패 시 None 반환
    """
    try:
        resp = session.get(url, params=params, timeout=API_CONFIG['timeout'])
        resp.raise_for_status()
        # Content-Type 확인
        content_type = resp.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            if logger:
                logger.warning(f"Non-JSON response: {content_type}")
            return None
        return resp.json()
    except json.JSONDecodeError as e:
        if logger:
            logger.error(f"JSON decode error: {e}")
        return None
    except requests.RequestException as e:
        if logger:
            logger.error(f"Request failed: {e}")
        raise   # 재시도 데코레이터가 처리할 수 있도록 예외 다시 발생
        