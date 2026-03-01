#!/usr/bin/env python3
"""
VWorld Geocoder - 주소를 좌표로 변환
"""
import time
import requests
from typing import Optional, Tuple
from constants.codes import VWORLD_API_KEY


class VWorldGeocoder:
    """
    VWorld API를 사용한 지오코딩 (주소 → 좌표)
    - rate limit 준수 (초당 호출 수 제한)
    """
    
    def __init__(self, calls_per_second: float = 3.0):
        self.calls_per_second = calls_per_second
        self.last_call_time = 0
        self.api_key = VWORLD_API_KEY

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """
        주소를 위도, 경도로 변환 (longitude, latitude 순서)
        """
        if not address or not self.api_key:
            return None

        # rate limit
        now = time.time()
        elapsed = now - self.last_call_time
        if elapsed < 1.0 / self.calls_per_second:
            time.sleep(1.0 / self.calls_per_second - elapsed)
        
        url = "https://api.vworld.kr/req/address"
        params = {
            "service": "address",
            "request": "getcoord",
            "version": "2.0",
            "crs": "epsg:4326",
            "address": address,
            "refine": "true",
            "simple": "false",
            "format": "json",
            "type": "road",  # 도로명 주소 우선
            "key": self.api_key,
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('response', {}).get('status') == 'OK':
                    point = data['response']['result']['point']
                    lon = float(point['x'])
                    lat = float(point['y'])
                    self.last_call_time = time.time()
                    return (lon, lat)
                elif data.get('response', {}).get('status') == 'NOT_FOUND':
                    # 도로명 주소 실패 시 지번 주소로 재시도
                    params['type'] = 'jibun'
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('response', {}).get('status') == 'OK':
                            point = data['response']['result']['point']
                            lon = float(point['x'])
                            lat = float(point['y'])
                            self.last_call_time = time.time()
                            return (lon, lat)
            # rate limit 초과 시
            if response.status_code == 429:
                time.sleep(5)
        except Exception as e:
            print(f"⚠️ Geocoding error: {e}")
        
        return None
        