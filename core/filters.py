#!/usr/bin/env python3
"""
텍스트 필터링 및 정규화
"""
import re
import unicodedata
from typing import Optional

class TextFilter:
    """기본 텍스트 필터"""
    
    @staticmethod
    def normalize(text: Optional[str]) -> str:
        """
        일반 텍스트 정규화
        - HTML 태그 제거
        - 유니코드 정규화 (NFKC)
        - 연속 공백 제거
        """
        if not text:
            return ""
        # HTML 태그 제거
        text = re.sub(r'<[^>]*>', ' ', text)
        # 유니코드 정규화 (전각→반각)
        text = unicodedata.normalize('NFKC', text)
        # 연속 공백을 하나로
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    @staticmethod
    def normalize_for_id(text: Optional[str]) -> str:
        """
        ID 생성용 강력 정규화
        - HTML 제거, 공백 제거, 특수문자 제거, 소문자 변환
        """
        if not text:
            return ""
        text = TextFilter.normalize(text)
        # 모든 공백 제거
        text = re.sub(r'\s', '', text)
        # 알파벳/숫자/한글만 남김
        text = re.sub(r'[^a-zA-Z0-9가-힣]', '', text)
        return text.lower()
    
    @staticmethod
    def clean_html(text: Optional[str]) -> str:
        """HTML 태그만 제거 (표시용)"""
        if not text:
            return ""
        return re.sub(r'<[^>]*>', ' ', text).strip()

class AddressFilter:
    """주소 필터 및 해시 생성"""
    
    @staticmethod
    def hash(address: str) -> str:
        """
        주소 해시 생성 (SHA256)
        - school_info_collector에서 주소 변경 감지용으로 사용
        """
        if not address:
            return ""
        import hashlib
        return hashlib.sha256(address.encode('utf-8')).hexdigest()[:16]


class SubjectNameFilter:
    """과목명 특화 필터 (수학I → 수학)"""
    
    ROMAN_TO_NUMBER = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
        'Ⅰ': '1', 'Ⅱ': '2', 'Ⅲ': '3', 'Ⅳ': '4', 'Ⅴ': '5',
    }
    
    @classmethod
    def normalize_for_id(cls, subject_name: Optional[str]) -> str:
        """과목명 ID 생성용 정규화 (레벨 정보 제거)"""
        if not subject_name:
            return ""
        name = TextFilter.normalize(subject_name)
        # 괄호 내용 제거
        name = re.sub(r'\([^)]*\)', '', name)
        name = re.sub(r'\[[^\]]*\]', '', name)
        # 끝에 붙은 로마자/숫자 제거
        for roman, num in cls.ROMAN_TO_NUMBER.items():
            name = re.sub(rf'{roman}$', '', name)
            name = re.sub(rf'{num}$', '', name)
        return TextFilter.normalize_for_id(name)
    
    @classmethod
    def extract_level(cls, subject_name: Optional[str]) -> str:
        """과목명에서 레벨 정보 추출 (I, II, 1, 2 등)"""
        if not subject_name:
            return ""
        name = TextFilter.normalize(subject_name)
        match = re.search(r'([IVXⅠⅡⅢⅣⅤ]+|[0-9]+)$', name)
        return match.group(1) if match else ""
        