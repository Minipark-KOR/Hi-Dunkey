#!/usr/bin/env python3
"""
VWorld Geocoder - 주소를 좌표로 변환 및 역지오코딩
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
        """
        API 호출 직전 rate limit 대기.
        last_call_time은 대기 완료 후 즉시 기록 → 실제 전송 시점 기준으로 TPS 계산.
        타임아웃 등 예외 발생 시에도 last_call_time이 기록되어 rate limit이 유지됨.
        """
        elapsed = time.time() - self.last_call_time
        wait = 1.0 / self.calls_per_second - elapsed
        if wait > 0:
            time.sleep(wait)
        self.last_call_time = time.time()  # 요청 전송 직전 기록

    def geocode(self, address: str, addr_type: str = "road") -> Optional[Tuple[float, float]]:
        """
        주소 → 좌표 변환 (도로명 또는 지번)
        addr_type: "road" 또는 "jibun"
        """
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
                        return None
            except requests.exceptions.Timeout:
                print(f"⏰ 타임아웃 (재시도 {attempt+1}/3)")
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                print(f"⚠️ 요청 오류: {e}")
                break
            except Exception as e:
                print(f"⚠️ Geocoding 예외: {e}")
                break
        return None

    def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """
        좌표 → 도로명 주소 역지오코딩
        """
        if not self.api_key:
            return None
        url = "https://api.vworld.kr/req/address"
        params = {
            "service": "address",
            "request": "getAddress",
            "version": "2.0",
            "crs": "epsg:4326",
            "point": f"{lon},{lat}",
            "format": "json",
            "type": "road",
            "zipcode": "false",
            "simple": "false",
            "key": self.api_key,
        }
        self._wait_rate_limit()
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('response', {}).get('status') == 'OK':
                    return data['response']['result']['text']
        except Exception as e:
            print(f"⚠️ Reverse geocoding 예외: {e}")
        return None
        