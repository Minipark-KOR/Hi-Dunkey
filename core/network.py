#!/usr/bin/env python3
"""
네트워크 요청 처리 - 세션 생성 + 지수 백오프 + Rate Limit 처리
"""
import time
import random
import requests
from typing import Optional, Dict, Any
import json

from constants.codes import API_CONFIG


class APILimitExceededException(Exception):
    """API 일일 한도 초과 예외"""
    pass


def build_session() -> requests.Session:
    """
    HTTP 세션 생성 (재사용)
    
    Returns:
        requests.Session 객체
    """
    session = requests.Session()
    
    # User-Agent 설정 (NEIS API 요구사항)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; NEIS-Collector/2.0)',
        'Accept': 'application/json'
    })
    
    return session


def safe_json_request(session, url: str, params: dict, logger, max_retries: int = 5) -> Optional[Dict[str, Any]]:
    """
    API 요청 + 재시도 + Rate Limit 처리 + NEIS 에러 응답 처리
    """
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            
            resp = session.get(url, params=params, timeout=API_CONFIG.get('timeout', 10))
            
            # Rate Limit (429) 처리
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                logger.warning(f"⏳ Rate limit 초과. {retry_after}초 대기 후 재시도")
                time.sleep(retry_after)
                continue
            
            # API 키 만료 (401)
            if resp.status_code == 401:
                logger.critical("❌ API 키 만료 또는 인증 오류! 관리자 확인 필요")
                return None
            
            # 성공 (200)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except json.JSONDecodeError:
                    logger.error(f"❌ JSON 파싱 실패: {resp.text[:100]}")
                    continue
                
                # NEIS API 비즈니스 로직 에러 체크
                if "RESULT" in data:
                    code = data["RESULT"].get("CODE")
                    msg = data["RESULT"].get("MESSAGE")
                    
                    # 데이터 없음 (정상)
                    if code == "INFO-200":
                        logger.debug(f"📭 데이터 없음: {msg}")
                        return None
                    
                    # API 일일 한도 초과 - 즉시 전파
                    if code == "ERROR-333":
                        logger.critical(f"🚨 API 일일 한도 초과! 수집 중단 필요: {msg}")
                        raise APILimitExceededException(f"API 일일 한도 초과: {msg}")
                    
                    # 인증/권한 오류 (재시도 무의미)
                    if code == "INFO-300" or code.startswith("ERROR-"):
                        logger.error(f"❌ NEIS API 오류 {code}: {msg}")
                        return None
                
                elapsed = (time.time() - start_time) * 1000
                logger.debug(f"✅ 응답 시간: {elapsed:.1f}ms")
                return data
            
            # 서버 에러 (5xx)
            if 500 <= resp.status_code < 600:
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"⚠️ 서버 에러 {resp.status_code}. {wait:.1f}초 후 재시도")
                time.sleep(wait)
                continue
            
            # 기타 오류
            logger.error(f"❌ HTTP 오류 {resp.status_code}: {resp.text[:200]}")
            
        except APILimitExceededException:
            # 즉시 상위로 전파
            raise
            
        except requests.exceptions.ConnectionError as e:
            if "Name or service not known" in str(e):
                logger.critical("🌐 DNS 조회 실패! 네트워크 연결 확인 필요")
                return None
            logger.error(f"🔌 연결 오류: {e}")
        
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ 타임아웃 (attempt {attempt+1}/{max_retries})")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 요청 예외: {e}")
        
        except Exception as e:
            logger.error(f"💥 예상치 못한 오류: {e}")
        
        # 지수 백오프
        if attempt < max_retries - 1:
            wait = (2 ** attempt) + random.uniform(0, 1)
            logger.info(f"🔄 {wait:.1f}초 후 재시도 (attempt {attempt+2}/{max_retries})")
            time.sleep(wait)
    
    logger.error(f"❌ {max_retries}회 재시도 실패")
    return None