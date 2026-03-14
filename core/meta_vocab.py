#!/usr/bin/env python3
"""
통합 메타데이터 Vocab 관리자
- 모든 도메인의 메타데이터 통합 관리
- 실제 데이터 분석 기반 FREQUENT_NUMBERS 적용
- 주소 계층 구조 + 하이브리드 번호 저장
"""
import sqlite3
import json
import os
from typing import Optional, Dict, List, Tuple, Set
from core.id_generator import IDGenerator, IDHelper
from core.text_filter import TextFilter


class MetaVocabManager:
    """
    통합 메타데이터 vocab 관리자
    
    Example:
        meta = MetaVocabManager('global.db')
        meta_id = meta.get_or_create('meal', 'cooking_method', '매운맛')
        
        # 주소 저장
        addr_ids = meta.save_address("서울특별시 강남구 테헤란로 212")
    """
    
    # 실제 데이터 분석 결과 (2026-02-27)
    # 총 12,611개 학교 분석
    # 상위 32개 번호 (전체의 6.7% 차지)
    FREQUENT_NUMBERS = [
        100, 115, 117, 112, 101, 120, 123, 110, 165, 107,
        109, 111, 130, 105, 104, 106, 119, 125, 127, 140,
        139, 133, 113, 150, 102, 121, 135, 155, 145, 131,
        205, 138
    ]
    
    # 자주 사용되는 번호는 비트마스크로 (4바이트)
    NUMBER_BITS = {num: 1 << i for i, num in enumerate(FREQUENT_NUMBERS[:32])}
    
    def __init__(self, db_path: str, debug_mode: bool = False):
        self.db_path = db_path
        self.cache: Dict[Tuple[str, str, str], int] = {}
        self.reverse_cache: Dict[int, Tuple[str, str, str]] = {}
        self.pending_inserts: Set[Tuple[int, str, str, str, str, int]] = set()
        
        IDHelper.DEBUG_MODE = debug_mode
        
        self._init_table()
        self._load_cache()
        
        if debug_mode:
            print(f"📊 FREQUENT_NUMBERS: {len(self.FREQUENT_NUMBERS)}개 (상위 6.7%)")
    
    def _init_table(self):
        """통합 메타데이터 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta_vocab (
                    meta_id     INTEGER PRIMARY KEY,
                    domain      TEXT NOT NULL,
                    meta_type   TEXT NOT NULL,
                    meta_key    TEXT NOT NULL,
                    meta_value  TEXT NOT NULL,
                    display_value TEXT NOT NULL,
                    parent_id   INTEGER,
                    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(domain, meta_type, meta_key)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_domain ON meta_vocab(domain)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_type ON meta_vocab(meta_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_parent ON meta_vocab(parent_id)")
    
    def _load_cache(self):
        """시작 시 전체 캐시 로드"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("""
                    SELECT meta_id, domain, meta_type, meta_value, display_value 
                    FROM meta_vocab
                """)
                for meta_id, domain, meta_type, meta_value, display_value in cur:
                    key = (domain, meta_type, meta_value)
                    self.cache[key] = meta_id
                    self.reverse_cache[meta_id] = (domain, meta_type, display_value)
            
            if IDHelper.DEBUG_MODE:
                print(f"✅ 메타 vocab 캐시 로드: {len(self.cache)}개")
                
        except Exception as e:
            print(f"⚠️ 메타 캐시 로드 실패: {e}")
    
    def _normalize_key(self, value: str) -> str:
        """메타값 정규화"""
        if not value:
            return ""
        return TextFilter.normalize_for_id(value)
    
    def _generate_meta_id(self, domain: str, meta_type: str, meta_key: str) -> int:
        """메타데이터 ID 생성"""
        key = f"{domain}:{meta_type}:{meta_key}"
        return IDGenerator.text_to_int(key, bits=63)
    
    def get_or_create(self, domain: str, meta_type: str, raw_value: str, parent_id: int = None) -> int:
        """
        메타데이터 ID 조회/생성
        
        Args:
            domain: 도메인 ('address', 'meal', 'timetable', 'schedule')
            meta_type: 메타타입 ('city', 'district', 'street', 'cooking_method')
            raw_value: 원본 값
            parent_id: 상위 ID (계층 구조용)
        
        Returns:
            meta_id
        """
        if not raw_value:
            return 0
        
        meta_key = self._normalize_key(raw_value)
        if not meta_key:
            return 0
        
        key = (domain, meta_type, raw_value)
        
        if key in self.cache:
            meta_id = self.cache[key]
            if IDHelper.DEBUG_MODE:
                print(f"🔍 캐시 히트: {domain}.{meta_type}={raw_value} → {IDHelper.format_id(meta_id)}")
            return meta_id
        
        meta_id = self._generate_meta_id(domain, meta_type, meta_key)
        display_value = TextFilter.normalize(raw_value)
        
        self.pending_inserts.add((meta_id, domain, meta_type, meta_key, raw_value, display_value, parent_id))
        
        if len(self.pending_inserts) >= 100:
            self.flush()
        
        self.cache[key] = meta_id
        self.reverse_cache[meta_id] = (domain, meta_type, display_value)
        
        if IDHelper.DEBUG_MODE:
            print(f"🆕 새 메타: {domain}.{meta_type}={raw_value} → {IDHelper.format_id(meta_id)}")
        
        return meta_id
    
    # ========================================================
    # 주소 저장 (하이브리드 방식)
    # ========================================================
    
    def parse_number(self, number_str: str) -> dict:
        """
        번호 파싱 (하이브리드)
        
        Examples:
            "212" → {"type": "single", "value": 212}
            "295-658" → {"type": "range", "start": 295, "end": 658}
            "일광로" → {"type": "none", "value": 0}  # ✅ 숫자가 아닌 경우 처리
        """
        if not number_str:
            return {"type": "none", "value": 0
            }
        
        if '-' in number_str:
            try:
                start, end = number_str.split('-')
                return {
                    "type": "range",
                    "start": int(start),
                    "end": int(end)
                }
            except ValueError:
                return {"type": "none", "value": 0
                }
        else:
            try:
                return {
                    "type": "single",
                    "value": int(number_str)
                }
            except ValueError:
                return {"type": "none", "value": 0
                }
    
    def save_address(self, full_address: str) -> dict:
        """
        주소 저장 (계층 구조 + 하이브리드 번호)
        
        Args:
            full_address: "서울특별시 강남구 테헤란로 212" 또는
                         "경기도 성남시 분당구 불정로 295-658"
        
        Returns:
            {
                "city_id": 101,
                "district_id": 201,
                "street_id": 301,
                "number_type": "single"/"range"/"bit",
                "number": 212,              # single
                # 또는
                "number_start": 295,         # range
                "number_end": 658,
                # 또는
                "number_bit": 1              # bit (frequent)
            }
        """
        if not full_address:
            return {
                "city_id": 0,
                "district_id": 0,
                "street_id": 0,
                "number_type": "none"
            }
        
        parts = full_address.split()
        
        # 시/도
        city = parts[0] if len(parts) > 0 else ""
        city_id = self.get_or_create('address', 'city', city) if city else 0
        
        # 시/군/구 (시/도를 parent로)
        district = parts[1] if len(parts) > 1 else ""
        district_id = self.get_or_create('address', 'district', district, parent_id=city_id) if district else 0
        
        # 도로명 (구를 parent로)
        street = parts[2] if len(parts) > 2 else ""
        street_id = self.get_or_create('address', 'street', street, parent_id=district_id) if street else 0
        
        # 번호 처리 (하이브리드)
        number_part = parts[3] if len(parts) > 3 else ""
        number_info = self.parse_number(number_part)
        
        result = {
            "city_id": city_id,
            "district_id": district_id,
            "street_id": street_id,
            "number_type": number_info["type"]
        }
        
        if number_info["type"] == "range":
            result["number_start"] = number_info["start"]
            result["number_end"] = number_info["end"]
        
        elif number_info["type"] == "single":
            value = number_info["value"]
            # 자주 사용되는 번호는 비트마스크로
            if value in self.NUMBER_BITS:
                result["number_type"] = "bit"
                result["number_bit"] = self.NUMBER_BITS[value]
            else:
                result["number"] = value
        
        return result
    
    def get_address_string(self, addr_data: dict) -> str:
        """
        저장된 데이터로 주소 복원
        """
        parts = []
        
        # 시/도
        if addr_data.get("city_id"):
            info = self.get_meta_info(addr_data["city_id"])
            if info:
                parts.append(info[2])
        
        # 시/군/구
        if addr_data.get("district_id"):
            info = self.get_meta_info(addr_data["district_id"])
            if info:
                parts.append(info[2])
        
        # 도로명
        if addr_data.get("street_id"):
            info = self.get_meta_info(addr_data["street_id"])
            if info:
                parts.append(info[2])
        
        # 번호
        number_type = addr_data.get("number_type", "none")
        if number_type == "single":
            parts.append(str(addr_data["number"]))
        elif number_type == "range":
            parts.append(f"{addr_data['number_start']}-{addr_data['number_end']}")
        elif number_type == "bit":
            # 비트에서 번호 복원
            bit = addr_data["number_bit"]
            for num, bit_val in self.NUMBER_BITS.items():
                if bit & bit_val:
                    parts.append(str(num))
                    break
        
        return " ".join(parts)
    
    def get_number_stats(self) -> dict:
        """번호 저장 통계"""
        stats = {
            "frequent_numbers": len(self.FREQUENT_NUMBERS),
            "frequent_coverage": 6.7,  # 상위 32개가 6.7% 차지
            "number_bits": len(self.NUMBER_BITS),
            "bit_efficiency": "32개 번호를 4바이트로 저장"
        }
        return stats
    
    def flush(self):
        """pending된 메타데이터 저장"""
        if not self.pending_inserts:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO meta_vocab 
                    (meta_id, domain, meta_type, meta_key, meta_value, display_value, parent_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, list(self.pending_inserts))
            
            if IDHelper.DEBUG_MODE:
                print(f"💾 메타 vocab 저장: {len(self.pending_inserts)}개")
            
            self.pending_inserts.clear()
            
        except Exception as e:
            print(f"❌ 메타 vocab 저장 실패: {e}")
    
    def get_meta_info(self, meta_id: int) -> Optional[Tuple[str, str, str]]:
        """meta_id로 메타정보 조회"""
        if meta_id in self.reverse_cache:
            return self.reverse_cache[meta_id]
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("""
                    SELECT domain, meta_type, display_value 
                    FROM meta_vocab WHERE meta_id = ?
                """, (meta_id,))
                row = cur.fetchone()
                if row:
                    self.reverse_cache[meta_id] = row
                    return row
        except Exception as e:
            print(f"⚠️ 메타 조회 실패: {e}")
        
        return None
    
    def get_children(self, parent_id: int) -> List[Tuple[int, str, str]]:
        """특정 ID의 하위 항목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("""
                    SELECT meta_id, meta_type, display_value 
                    FROM meta_vocab 
                    WHERE parent_id = ?
                    ORDER BY display_value
                """, (parent_id,))
                return cur.fetchall()
        except Exception as e:
            print(f"⚠️ 하위 항목 조회 실패: {e}")
            return []
    
    def format_id(self, meta_id: int) -> str:
        """디버그용 ID 포맷팅"""
        return IDHelper.format_id(meta_id, length=8)
    
    def get_stats(self) -> Dict:
        """통계 정보"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM meta_vocab").fetchone()[0]
                
                by_domain = conn.execute("""
                    SELECT domain, COUNT(*) FROM meta_vocab GROUP BY domain
                """).fetchall()
                
                by_type = conn.execute("""
                    SELECT meta_type, COUNT(*) FROM meta_vocab GROUP BY meta_type
                """).fetchall()
                
                return {
                    'total': total,
                    'by_domain': dict(by_domain),
                    'by_type': dict(by_type),
                    'number_stats': self.get_number_stats(),
                    'cache_size': len(self.cache),
                    'pending': len(self.pending_inserts)
                }
        except Exception as e:
            return {'error': str(e)}
    
    def close(self):
        """종료 시 저장"""
        self.flush()
        if IDHelper.DEBUG_MODE:
            stats = self.get_stats()
            print(f"📊 MetaVocab 종료: 총 {stats.get('total', 0)}개")