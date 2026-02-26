#!/usr/bin/env python3
"""
통합 메타데이터 Vocab 관리자
- 모든 도메인의 메타데이터(조리법, 식재료, 특수표시 등) 통합 관리
"""
import sqlite3
from typing import Optional, Dict, List, Tuple, Set
from core.id_generator import IDGenerator, IDHelper
from core.text_filter import TextFilter


class MetaVocabManager:
    """
    통합 메타데이터 vocab 관리자
    
    Example:
        meta = MetaVocabManager('global.db')
        meta_id = meta.get_or_create('meal', 'cooking_method', '매운맛')
    """
    
    def __init__(self, db_path: str, debug_mode: bool = False):
        self.db_path = db_path
        self.cache: Dict[Tuple[str, str, str], int] = {}  # (domain, type, value) -> meta_id
        self.reverse_cache: Dict[int, Tuple[str, str, str]] = {}  # meta_id -> (domain, type, value)
        self.pending_inserts: Set[Tuple[int, str, str, str, str]] = set()
        
        IDHelper.DEBUG_MODE = debug_mode
        
        self._init_table()
        self._load_cache()
    
    def _init_table(self):
        """통합 메타데이터 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            # WAL 모드 활성화
            conn.execute("PRAGMA journal_mode=WAL")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta_vocab (
                    meta_id     INTEGER PRIMARY KEY,
                    domain      TEXT NOT NULL,     -- 'meal', 'timetable', 'schedule'
                    meta_type   TEXT NOT NULL,     -- 'cooking_method', 'ingredient', 'special_mark'
                    meta_key    TEXT NOT NULL,     -- 정규화된 키 (UNIQUE)
                    meta_value  TEXT NOT NULL,     -- 원본 값
                    display_value TEXT NOT NULL,   -- 표시용 값
                    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(domain, meta_type, meta_key)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_domain ON meta_vocab(domain)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_type ON meta_vocab(meta_type)")
    
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
    
    def get_or_create(self, domain: str, meta_type: str, raw_value: str) -> int:
        """
        메타데이터 ID 조회/생성
        
        Args:
            domain: 도메인 ('meal', 'timetable', 'schedule')
            meta_type: 메타타입 ('cooking_method', 'ingredient', 'special_mark')
            raw_value: 원본 값 ('매운맛', '돼지고기', '★')
        
        Returns:
            meta_id (63비트 정수)
        """
        if not raw_value:
            return 0
        
        # 값 정규화
        meta_key = self._normalize_key(raw_value)
        if not meta_key:
            return 0
        
        key = (domain, meta_type, raw_value)
        
        # 1. 캐시 확인
        if key in self.cache:
            meta_id = self.cache[key]
            if IDHelper.DEBUG_MODE:
                print(f"🔍 메타 캐시 히트: {domain}.{meta_type}={raw_value} → {IDHelper.format_id(meta_id)}")
            return meta_id
        
        # 2. ID 생성
        meta_id = self._generate_meta_id(domain, meta_type, meta_key)
        display_value = TextFilter.normalize(raw_value)
        
        # 3. 배치 저장
        self.pending_inserts.add((meta_id, domain, meta_type, meta_key, raw_value, display_value))
        
        # 4. 100개 모이면 저장
        if len(self.pending_inserts) >= 100:
            self.flush()
        
        # 5. 캐시 업데이트
        self.cache[key] = meta_id
        self.reverse_cache[meta_id] = (domain, meta_type, display_value)
        
        if IDHelper.DEBUG_MODE:
            print(f"🆕 새 메타: {domain}.{meta_type}={raw_value} → {IDHelper.format_id(meta_id)}")
        
        return meta_id
    
    def flush(self):
        """pending된 메타데이터 저장"""
        if not self.pending_inserts:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO meta_vocab 
                    (meta_id, domain, meta_type, meta_key, meta_value, display_value) 
                    VALUES (?, ?, ?, ?, ?, ?)
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
    
    def search_by_type(self, domain: str, meta_type: str, keyword: str = "", limit: int = 100) -> List[Tuple[int, str]]:
        """특정 타입의 메타데이터 검색"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if keyword:
                    cur = conn.execute("""
                        SELECT meta_id, display_value FROM meta_vocab
                        WHERE domain = ? AND meta_type = ? 
                          AND (meta_value LIKE ? OR display_value LIKE ?)
                        ORDER BY display_value
                        LIMIT ?
                    """, (domain, meta_type, f'%{keyword}%', f'%{keyword}%', limit))
                else:
                    cur = conn.execute("""
                        SELECT meta_id, display_value FROM meta_vocab
                        WHERE domain = ? AND meta_type = ?
                        ORDER BY display_value
                        LIMIT ?
                    """, (domain, meta_type, limit))
                return cur.fetchall()
        except Exception as e:
            print(f"⚠️ 메타 검색 실패: {e}")
            return []
    
    def format_id(self, meta_id: int) -> str:
        """디버그용 ID 포맷팅"""
        return IDHelper.format_id(meta_id, length=8)
    
    def close(self):
        """종료 시 저장"""
        self.flush()
        