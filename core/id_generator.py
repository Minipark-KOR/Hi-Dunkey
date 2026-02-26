#!/usr/bin/env python3
"""
ID 생성 유틸리티 (63비트 정수)
"""
import hashlib
import struct
from .filters import TextFilter

BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

class IDGenerator:
    """텍스트 기반 ID 생성"""
    
    @staticmethod
    def text_to_int(text: str, namespace: str = "", bits: int = 63) -> int:
        """
        텍스트 → 63비트 양의 정수
        - SHA-256 해시의 앞 8바이트를 64비트 정수로 변환
        - 최상위 비트를 0으로 만들어 63비트 보장
        """
        if not text:
            return 0
        
        # ID 생성용 정규화
        clean = TextFilter.normalize_for_id(text)
        if namespace:
            clean = f"{namespace}:{clean}"
        
        # SHA-256 해시
        hash_bytes = hashlib.sha256(clean.encode('utf-8')).digest()
        # 앞 8바이트를 빅엔디언 unsigned long long 으로 변환
        value = struct.unpack('>Q', hash_bytes[:8])[0]
        # 최상위 비트 제거 → 63비트 양수
        value &= (1 << 63) - 1
        
        if bits < 63:
            value >>= (63 - bits)
        return value
    
    @staticmethod
    def int_to_base62(num: int, length: int = 8) -> str:
        """정수 ID를 Base62 문자열로 변환 (디버깅용)"""
        if num == 0:
            return '0' * length
        
        result = []
        temp = num
        while temp > 0:
            temp, rem = divmod(temp, 62)
            result.append(BASE62[rem])
        
        result_str = ''.join(reversed(result))
        if len(result_str) < length:
            result_str = '0' * (length - len(result_str)) + result_str
        return result_str[:length]


class IDHelper:
    """ID 변환 헬퍼 (디버깅 모드 지원)"""
    
    DEBUG_MODE = False
    _cache = {}
    
    @classmethod
    def format_id(cls, int_id: int, length: int = 8) -> str:
        """
        디버그 모드에서는 Base62, 아니면 숫자 문자열 반환
        """
        if not cls.DEBUG_MODE:
            return str(int_id)
        
        if int_id in cls._cache:
            return cls._cache[int_id]
        
        base62 = IDGenerator.int_to_base62(int_id, length)
        cls._cache[int_id] = base62
        return base62