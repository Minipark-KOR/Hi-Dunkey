#!/usr/bin/env python3
# core/filters.py
import re
import unicodedata
import hashlib
from typing import Optional

class TextFilter:
    @staticmethod
    def normalize(text: str) -> str:
        if text is None:
            return ""
        s = str(text)
        s = unicodedata.normalize("NFKC", s)
        s = s.replace("\u00A0", " ").replace("\u200b", "").replace("\ufeff", "")
        s = re.sub(r"[\r\n\t]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def normalize_for_id(text: str) -> str:
        if not text:
            return ""
        t = TextFilter.normalize(text)
        t = re.sub(r"\s+", "", t)
        t = re.sub(r"[^a-zA-Z0-9가-힣]", "", t)
        return t.lower()


class AddressFilter:
    # 도로명 주소 판별용 토큰 (더 포괄적인 버전)
    _ROAD_TOKEN = re.compile(r"[가-힣0-9]+(?:대로|로|길)\b")
    _JIBUN_TOKEN = re.compile(r"(?:[가-힣][가-힣0-9]*동|[가-힣][가-힣0-9]*리|[가-힣][가-힣0-9]*읍|[가-힣][가-힣0-9]*면|[가-힣0-9]+가)\s*\d")

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

    ADMIN_DISTRICT_MAP = {
        "일광면": "일광읍",
        "산동면": "산동읍",
        "양북면": "문무대왕면",
        "퇴계원면": "퇴계원읍",
        "이동면": "이동읍",
        "비봉면": "비봉읍",
        # 자동 학습 결과 추가 가능
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
        if not address:
            return ""
        addr = TextFilter.normalize(address)

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
        a = TextFilter.normalize(address)
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
        if not address:
            return ""
        return hashlib.sha256(address.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def extract_jibun(address: str) -> Optional[str]:
        """
        주소에서 지번 주소 추출 (VWorld API에 최적화)
        - 시/도 + 시/군/구 + 읍/면/동 + 번지(숫자/가산) 형태
        - 도로명 주소는 제외하고 지번만 반환
        """
        if not address:
            return None
        a = TextFilter.normalize(address)
        # 도로명 주소 패턴이 보이면 지번 추출 스킵
        if AddressFilter._ROAD_TOKEN.search(a):
            return None
        # 지번 패턴 (전체 매칭 문자열 그대로 반환)
        patterns = [
            # 1. 시도/시군구 포함 풀 패턴
            r'[가-힣]+(?:시|도)\s+[가-힣]+(?:시|군|구)\s*[가-힣]+(?:읍|면|동|리)\s+(?:[가-힣0-9]+(?:가)?\s*\d+(?:-\d+)?(?:번지)?)',
            # 2. 읍면동 + 번지 (시/군/구 생략)
            r'[가-힣]+(?:읍|면|동|리)\s+(?:[가-힣0-9]+(?:가)?\s*\d+(?:-\d+)?(?:번지)?)',
            # 3. 산(山) 패턴
            r'[가-힣]+(?:읍|면|동|리)\s+(?:산\s*\d+(?:-\d+)?(?:번지)?)'
        ]
        for pat in patterns:
            m = re.search(pat, a)
            if m:
                return m.group(0).strip()
        return None
        