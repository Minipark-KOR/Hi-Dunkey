#!/usr/bin/env python3
"""
주소 정제 필터 - AddressFilter (분리됨)
"""
import re
import hashlib
from typing import Optional
from core.filters import TextFilter  # TextFilter는 core.filters에 그대로 둠

class AddressFilter:
    """주소 정제, 분류, 해싱, 지번 추출"""

    REGION_SHORTEN_PREFIX = [
        (r"^서울특별시\s*", "서울 "),
        (r"^부산광역시\s*", "부산 "),
        (r"^대구광역시\s*", "대구 "),
        (r"^인천광역시\s*", "인천 "),
        (r"^광주광역시\s*", "광주 "),
        (r"^대전광역시\s*", "대전 "),
        (r"^울산광역시\s*", "울산 "),
        (r"^세종특별자치시\s*", "세종 "),
        (r"^경기도\s*", "경기 "),
        (r"^강원도\s*", "강원 "),
        (r"^충청북도\s*", "충북 "),
        (r"^충청남도\s*", "충남 "),
        (r"^전라북도\s*", "전북 "),
        (r"^전라남도\s*", "전남 "),
        (r"^경상북도\s*", "경북 "),
        (r"^경상남도\s*", "경남 "),
        (r"^제주특별자치도\s*", "제주 "),
    ]

    _ROAD_TOKEN = re.compile(r"[가-힣0-9]+(?:대로|로|길)\b")
    _JIBUN_TOKEN = re.compile(r"(?:[가-힣][가-힣0-9]*동|[가-힣][가-힣0-9]*리|[가-힣][가-힣0-9]*읍|[가-힣][가-힣0-9]*면|[가-힣0-9]+가)\s*\d")

    ADMIN_DISTRICT_MAP = {
        "일광면": "일광읍",
        "산동면": "산동읍",
        "양북면": "문무대왕면",
        "퇴계원면": "퇴계원읍",
        "이동면": "이동읍",
        "비봉면": "비봉읍",
        "홍북면": "홍북읍",
        "양지면": "양지읍",
        "정관면": "정관읍",
        "능서면": "세종대왕면",
        "안중읍": "현덕면",  # 특수 케이스
    }

    REMOVE_DISTRICT = ["오포읍", "남면"]

    @staticmethod
    def advanced_clean(address: str) -> str:
        if not address:
            return ""
        addr = address
        addr = re.sub(r'^[가-힣]+교육청\s*', '', addr).strip()
        for old, new in AddressFilter.ADMIN_DISTRICT_MAP.items():
            pattern = rf'(?<![가-힣]){re.escape(old)}(?![가-힣])'
            addr = re.sub(pattern, new, addr)
        for district in AddressFilter.REMOVE_DISTRICT:
            pattern = rf'(?<![가-힣]){re.escape(district)}(?![가-힣])'
            addr = re.sub(pattern, ' ', addr)
        addr = re.sub(r'\([^)]*\)', '', addr)
        addr = re.sub(r'\[[^\]]*\]', '', addr)
        addr = re.sub(r'\s+', ' ', addr).strip()
        return addr

    @staticmethod
    def clean(address: str, level: int = 1) -> str:
        """
        주소 정제
        level 1: 기본 (괄호 제거, 우편번호 제거, '번지' 제거)
        level 2: 도로명/지번 형식 정리
        level 3: 시/도 약칭
        level 4: 고급 (교육청 제거, 행정구역명 변경 등)
        """
        if not address:
            return ""
        addr = TextFilter.normalize(address, strip_html=False)  # HTML 제거 안 함
        if level >= 1:
            addr = re.sub(r'\([^)]*\)', '', addr)
            addr = re.sub(r'\[[^\]]*\]', '', addr)
            addr = re.sub(r'^\s*\d{5}\s+', '', addr)
            addr = re.sub(r'\s+\d{5}\s*$', '', addr)
            addr = re.sub(r'(\d+(?:-\d+)?)\s*번지\b', r'\1', addr)
        if level >= 2:
            addr = re.sub(r'(?<=\d)\s*-\s*(?=\d)', "-", addr)
            addr = re.sub(r'\b([가-힣0-9]+)\s+(대로|로|길)\b', r'\1\2', addr)
            addr = re.sub(r'\s+', " ", addr).strip()
        if level >= 3:
            for pat, rep in AddressFilter.REGION_SHORTEN_PREFIX:
                addr = re.sub(pat, rep, addr)
            addr = re.sub(r'\s+', " ", addr).strip()
        if level >= 4:
            addr = AddressFilter.advanced_clean(addr)
        return addr

    @staticmethod
    def classify(address: str) -> str:
        """주소 유형 분류: road / jibun / mixed / unknown"""
        a = TextFilter.normalize(address, strip_html=False)
        is_road = bool(AddressFilter._ROAD_TOKEN.search(a))
        is_jibun = bool(AddressFilter._JIBUN_TOKEN.search(a))
        if is_road and not is_jibun:
            return "road"
        if is_jibun and not is_road:
            return "jibun"
        if is_road and is_jibun:
            return "mixed"
        return "unknown"

    @staticmethod
    def hash(address: str) -> str:
        """주소의 SHA-256 해시 앞 16자리 반환"""
        if not address:
            return ""
        return hashlib.sha256(address.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def extract_jibun(address: str) -> Optional[str]:
        """
        주소에서 지번 (번지) 부분을 추출합니다.
        예: "서울특별시 종로구 사직동 123-4" → "사직동 123-4"
            "경기도 광주시 오포읍 산 123" → "오포읍 산 123"
        """
        if not address:
            return None
        a = TextFilter.normalize(address, strip_html=False)
        patterns = [
            r'([가-힣]+(?:읍|면|동|리))\s+(\d+(?:-\d+)?)',
            r'([가-힣]+(?:읍|면|동|리))\s+(산\s*\d+(?:-\d+)?)',
            r'([가-힣]+(?:시|도)?\s+[가-힣]+(?:시|군|구)?\s+[가-힣]+(?:읍|면|동|리))\s+(\d+(?:-\d+)?)',
        ]
        for pat in patterns:
            m = re.search(pat, a)
            if m:
                jibun_part = m.group(1).strip()
                number_part = m.group(2).strip()
                return re.sub(r'\s+', ' ', f"{jibun_part} {number_part}").strip()
        return None
        