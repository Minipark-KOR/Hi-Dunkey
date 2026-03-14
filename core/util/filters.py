#!/usr/bin/env python3
"""텍스트 필터링 및 정규화 유틸."""

import re
import unicodedata
from typing import Optional


class TextFilter:
    """기본 텍스트 필터 (HTML 제거 옵션 포함)."""

    @staticmethod
    def normalize(text: Optional[str], strip_html: bool = False) -> str:
        """
        텍스트 정규화
        - strip_html=True : HTML 태그 제거
        - 유니코드 정규화 (NFKC)
        - 특수 공백/제어 문자 제거 (\u00A0, \u200b, \ufeff)
        - 개행 문자 -> 공백
        - 연속 공백 압축 및 trim
        """
        if not text:
            return ""
        s = str(text)

        if strip_html:
            s = re.sub(r"<[^>]*>", " ", s)

        s = unicodedata.normalize("NFKC", s)
        s = s.replace("\u00A0", " ").replace("\u200b", "").replace("\ufeff", "")
        s = re.sub(r"[\r\n\t]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def normalize_for_id(text: Optional[str]) -> str:
        """
        ID 생성용 강력 정규화
        - HTML 제거, 공백 제거, 특수문자 제거, 소문자 변환
        """
        if not text:
            return ""
        t = TextFilter.normalize(text, strip_html=True)
        t = re.sub(r"\s+", "", t)
        t = re.sub(r"[^a-zA-Z0-9가-힣]", "", t)
        return t.lower()

    @staticmethod
    def clean_html(text: Optional[str]) -> str:
        """HTML 태그만 제거 (표시용)."""
        if not text:
            return ""
        return re.sub(r"<[^>]*>", " ", text).strip()


class SubjectNameFilter:
    """과목명 특화 필터 (레벨 정보 분리)."""

    ROMAN_TO_NUMBER = {
        "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
        "Ⅰ": "1", "Ⅱ": "2", "Ⅲ": "3", "Ⅳ": "4", "Ⅴ": "5",
    }

    @classmethod
    def normalize_for_id(cls, subject_name: Optional[str]) -> str:
        """과목명 ID 생성 (레벨 정보 제거)."""
        if not subject_name:
            return ""
        name = TextFilter.normalize(subject_name, strip_html=True)
        name = re.sub(r"\([^)]*\)", "", name)
        name = re.sub(r"\[[^\]]*\]", "", name)
        for roman, num in cls.ROMAN_TO_NUMBER.items():
            name = re.sub(rf"{roman}$", "", name)
            name = re.sub(rf"{num}$", "", name)
        return TextFilter.normalize_for_id(name)

    @classmethod
    def extract_level(cls, subject_name: Optional[str]) -> str:
        """과목명에서 레벨 정보 추출 (I, II, 1, 2 등)."""
        if not subject_name:
            return ""
        name = TextFilter.normalize(subject_name, strip_html=True)
        match = re.search(r"([IVXⅠⅡⅢⅣⅤ]+|[0-9]+)$", name)
        return match.group(1) if match else ""
