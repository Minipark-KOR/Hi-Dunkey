#!/usr/bin/env python3
"""
네트워크 요청 공통 모듈 (멀티키 지원)
"""
import time
import requests
from typing import Optional
from constants.codes import neis_key_manager

def safe_json_request(session, url, params, logger, key_manager=None):
    if key_manager is None:
        key_manager = neis_key_manager

    api_key_obj = key_manager.get_key()
    if not api_key_obj:
        logger.error("❌ 사용 가능한 NEIS API 키 없음")
        return None

    params["KEY"] = api_key_obj.key
    params["Type"] = "json"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=20)
            if resp.status_code == 200:
                key_manager.report_success(api_key_obj)
                return resp.json()
            elif resp.status_code == 429:
                logger.warning(f"⏳ 키 {api_key_obj.key[:6]}... Rate limit 초과, 재시도 ({attempt+1}/{max_retries})")
                key_manager.report_failure(api_key_obj)
                time.sleep(2 ** attempt)
                continue
            else:
                logger.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
                key_manager.report_failure(api_key_obj)
                return None
        except requests.exceptions.Timeout:
            logger.error(f"⏰ 요청 타임아웃 (키 {api_key_obj.key[:6]}...)")
            key_manager.report_failure(api_key_obj)
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"요청 예외: {e}")
            key_manager.report_failure(api_key_obj)
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
    return None

def build_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=100,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session