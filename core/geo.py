#!/usr/bin/env python3
"""
VWorld Geocoder - 주소를 좌표로 변환 (개선 버전)
- 타임아웃 30초, 최대 3회 재시도, 지수 백오프
- rate limit: 호출 직전 시간 기록 (API 서버 TPS 기준)
"""
import time
import requests
from typing import Optional, Tuple
from constants.codes import VWORLD_API_KEY


class VWorldGeocoder:
    def __init__(self, calls_per_second: float = 3.0):
        self.calls_per_second = calls_per_second
        self.last_call_time = 0.0
        self.api_key = VWORLD_API_KEY

    def _wait_rate_limit(self):
        """API 호출 직전 rate limit 대기"""
        elapsed = time.time() - self.last_call_time
        wait = 1.0 / self.calls_per_second - elapsed
        if wait > 0:
            time.sleep(wait)
        self.last_call_time = time.time()

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        if not address or not self.api_key:
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
                        time.sleep(5 * (attempt + 1))
                        continue

                    if response.status_code == 200:
                        data = response.json()
                        status = data.get('response', {}).get('status')
                        if status == 'OK':
                            point = data['response']['result']['point']
                            return (float(point['x']), float(point['y']))
                        elif status == 'NOT_FOUND':
                            break  # 다음 addr_type 시도
                except requests.exceptions.Timeout:
                    print(f"⏰ 타임아웃 (addr_type={addr_type}, 재시도 {attempt+1}/3)")
                    time.sleep(2 ** attempt)
                except requests.exceptions.RequestException as e:
                    print(f"⚠️ 요청 오류: {e}")
                    break
                except Exception as e:
                    print(f"⚠️ Geocoding 예외: {e}")
                    break
        return None
        