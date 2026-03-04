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
        # ✅ 수정: literal newline → \n
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
        # ✅ learn_address_map.py 분석 결과 새로 추가할 매핑
        "홍북면": "홍북읍",
        "정관면": "정관읍",
        "삼남면": "삼남읍",
        "양지면": "양지읍",
        "능서면": "세종대왕면",
        # "안중읍": "현덕면",  # 이 패턴은 특이 케이스라 우선 주석 처리
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
        