#!/usr/bin/env python3
"""
VWorld Geocoder - 주소를 좌표로 변환 (개선 버전)
"""
import time
import requests
import logging
from typing import Optional, Tuple
from constants.codes import VWORLD_API_KEY

# 모듈 레벨 로거 생성
logger = logging.getLogger(__name__)

class VWorldGeocoder:
    def __init__(self, calls_per_second: float = 3.0):
        self.calls_per_second = calls_per_second
        self.last_call_time = 0.0
        self.api_key = VWORLD_API_KEY

    def _wait_rate_limit(self):
        elapsed = time.time() - self.last_call_time
        wait = 1.0 / self.calls_per_second - elapsed
        if wait > 0:
            time.sleep(wait)
        self.last_call_time = time.time()

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        if not address or not self.api_key:
            logger.warning("주소 또는 API 키 없음")
            return None

        url = "https://api.vworld.kr/req/address"
        params_base = {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4326",
            "address": address,
            "refine": "true",
            "simple": "false",
            "format": "json",
            "key": self.api_key,
        }

        for addr_type in ['road', 'jibun']:
            for attempt in range(3):
                self._wait_rate_limit()
                try:
                    response = requests.get(
                        url,
                        params={**params_base, "type": addr_type},
                        timeout=30
                    )
                    if response.status_code == 429:
                        logger.warning("API 요청 제한 초과 (429), 재시도 대기")
                        time.sleep(5 * (attempt + 1))
                        continue

                    if response.status_code == 200:
                        data = response.json()
                        status = data.get('response', {}).get('status')
                        msg = data.get('response', {}).get('msg', '')
                        if status == 'OK':
                            point = data['response']['result']['point']
                            return (float(point['x']), float(point['y']))
                        elif status == 'NOT_FOUND':
                            logger.debug(f"주소를 찾을 수 없음 (addr_type={addr_type}): {address[:50]}...")
                            break  # 다음 addr_type 시도
                        else:
                            logger.error(f"VWorld API 오류 (status={status}, msg={msg}): {address[:50]}...")
                            break  # 해당 타입 실패, 다음 타입으로
                    else:
                        logger.warning(f"HTTP 오류: {response.status_code}")

                except requests.exceptions.Timeout:
                    logger.warning(f"타임아웃 (addr_type={addr_type}, 재시도 {attempt+1}/3)")
                    time.sleep(2 ** attempt)
                except requests.exceptions.RequestException as e:
                    logger.error(f"요청 오류: {e}")
                    break
                except Exception as e:
                    logger.error(f"Geocoding 예외: {e}", exc_info=True)
                    break

        logger.warning(f"모든 시도 실패: {address[:50]}...")
        return None
        