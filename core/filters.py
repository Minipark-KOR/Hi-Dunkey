#!/usr/bin/env python3
"""
텍스트 필터링 및 정규화 + 주소 정제
"""
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

    @staticmethod
    def clean(address: str, level: int = 1) -> str:
        if not address:
            return ""
        addr = TextFilter.normalize(address)

        if level >= 1:
            addr = re.sub(r"\([^)]*\)", "", addr)
            addr = re.sub(r"\[[^\]]*\]", "", addr)
            addr = re.sub(r"^\s*\d{5}\s+", "", addr)
            addr = re.sub(r"\s+\d{5}\s*$", "", addr)
            addr = re.sub(r"(\d+(?:-\d+)?)\s*번지\b", r"\1", addr)

        if level >= 2:
            addr = re.sub(r"(?<=\d)\s*-\s*(?=\d)", "-", addr)
            addr = re.sub(r"\b([가-힣0-9]+)\s+(대로|로|길)\b", r"\1\2", addr)
            addr = re.sub(r"\s+", " ", addr).strip()

        if level >= 3:
            for pat, rep in AddressFilter.REGION_SHORTEN_PREFIX:
                addr = re.sub(pat, rep, addr)
            addr = re.sub(r"\s+", " ", addr).strip()

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
        