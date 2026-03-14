#!/usr/bin/env python3
"""
제네릭 Vocab 관리자
- 모든 도메인(meal, timetable, schedule)의 vocab 통합 관리
- 전략 패턴으로 정규화 로직 주입
- 메모리 캐시 + 배치 저장 최적화
"""
import sqlite3
import re
from typing import Optional, Dict, List, Tuple, Set, Callable
from core.id_generator import IDGenerator, IDHelper
from core.filters import TextFilter


class VocabManager:
    """
    제네릭 Vocab 관리자
    
    Example:
        # 급식용
        meal_vocab = VocabManager(
            'global.db', 'meal',
            normalize_func=lambda x: re.sub(r'\\([^)]*\\)', '', TextFilter.normalize_for_id(x))
        )
        
        # 시간표용
        timetable_vocab = VocabManager('global.db', 'timetable')
    """
    
    def __init__(self, db_path: str, domain: str, 
                 normalize_func: Optional[Callable[[str], str]] = None,
                 debug_mode: bool = False):
        """
        Args:
            db_path: SQLite DB 파일 경로
            domain: 도메인명 ('meal', 'timetable', 'schedule')
            normalize_func: 도메인별 정규화 함수 (None이면 기본값 사용)
            debug_mode: 디버그 모드 여부
        """
        self.db_path = db_path
        self.domain = domain
        self.table_name = f"vocab_{domain}"
        
        # 정규화 함수 설정 (전략 패턴)
        self.normalize = normalize_func or self._default_normalize
        
        # 캐시
        self.cache: Dict[str, int] = {}          # 원본명 -> ID
        self.reverse_cache: Dict[int, str] = {}  # ID -> 원본명
        self.pending_inserts: Set[Tuple[int, str, str, str]] = set()
        
        IDHelper.DEBUG_MODE = debug_mode
        
        self._init_table()
        self._load_cache()
    
    def _default_normalize(self, text: str) -> str:
        """기본 정규화 (공통 로직)"""
        if not text:
            return ""
        return TextFilter.normalize_for_id(text)
    
    def _init_table(self):
        """도메인별 vocab 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            # WAL 모드 활성화 (동시성 향상)
            conn.execute("PRAGMA journal_mode=WAL")
            
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    {self.domain}_id INTEGER PRIMARY KEY,
                    name_key TEXT NOT NULL UNIQUE,      -- 정규화된 키 (UNIQUE)
                    name TEXT NOT NULL,                  -- 원본 이름
                    display_name TEXT NOT NULL,          -- 표시용 이름
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.domain}_key ON {self.table_name}(name_key)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.domain}_name ON {self.table_name}(name)")
    
    def _load_cache(self):
        """시작 시 전체 캐시 로드"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute(f"SELECT {self.domain}_id, name FROM {self.table_name}")
                for item_id, name in cur:
                    self.cache[name] = item_id
                    self.reverse_cache[item_id] = name
            
            if IDHelper.DEBUG_MODE:
                print(f"✅ {self.domain} vocab 캐시 로드: {len(self.cache)}개")
                
        except Exception as e:
            print(f"⚠️ {self.domain} 캐시 로드 실패: {e}")
    
    def _generate_id(self, name_key: str) -> int:
        """도메인별 ID 생성 (해시 기반)"""
        return IDGenerator.text_to_int(name_key, namespace=self.domain, bits=63)
    
    def get_or_create(self, name: str) -> int:
        """
        이름으로 ID 조회/생성
        
        Args:
            name: 원본 이름
            
        Returns:
            item_id (63비트 정수)
        """
        if not name:
            return 0
        
        # 1. 캐시 확인
        if name in self.cache:
            item_id = self.cache[name]
            if IDHelper.DEBUG_MODE:
                print(f"🔍 {self.domain} 캐시 히트: {name[:20]} → {IDHelper.format_id(item_id)}")
            return item_id
        
        # 2. 정규화 (주입된 함수 사용)
        name_key = self.normalize(name)
        if not name_key:
            name_key = "empty"
        
        # 3. ID 생성
        item_id = self._generate_id(name_key)
        display_name = TextFilter.normalize(name, strip_html=True)
        
        # 4. 배치 저장을 위해 pending
        self.pending_inserts.add((item_id, name_key, name, display_name))
        
        # 5. 100개 모이면 일괄 저장
        if len(self.pending_inserts) >= 100:
            self.flush()
        
        # 6. 캐시 업데이트
        self.cache[name] = item_id
        self.reverse_cache[item_id] = name
        
        if IDHelper.DEBUG_MODE:
            print(f"🆕 새 {self.domain}: {name[:20]} → {IDHelper.format_id(item_id)}")
        
        return item_id
    
    def get_or_create_batch(self, names: List[str]) -> Dict[str, int]:
        """
        여러 이름을 한 번에 처리
        
        Args:
            names: 이름 리스트
            
        Returns:
            {name: item_id} 딕셔너리
        """
        result = {}
        need_create = []
        
        # 캐시에서 찾기
        for name in names:
            if name in self.cache:
                result[name] = self.cache[name]
            else:
                need_create.append(name)
        
        # 새로운 항목들 처리
        for name in need_create:
            name_key = self.normalize(name) or "empty"
            item_id = self._generate_id(name_key)
            display_name = TextFilter.normalize(name, strip_html=True)
            
            self.pending_inserts.add((item_id, name_key, name, display_name))
            self.cache[name] = item_id
            self.reverse_cache[item_id] = name
            result[name] = item_id
        
        if len(self.pending_inserts) >= 100:
            self.flush()
        
        return result
    
    def flush(self):
        """pending된 항목들을 DB에 일괄 저장"""
        if not self.pending_inserts:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(f"""
                    INSERT OR IGNORE INTO {self.table_name} 
                    ({self.domain}_id, name_key, name, display_name) 
                    VALUES (?, ?, ?, ?)
                """, list(self.pending_inserts))
            
            if IDHelper.DEBUG_MODE:
                print(f"💾 {self.domain} vocab 저장: {len(self.pending_inserts)}개")
            
            self.pending_inserts.clear()
            
        except sqlite3.IntegrityError as e:
            # name_key 중복 무시
            if IDHelper.DEBUG_MODE:
                print(f"⚠️ {self.domain} 일부 항목 이미 존재: {e}")
            self.pending_inserts.clear()
            
        except Exception as e:
            print(f"❌ {self.domain} vocab 저장 실패: {e}")
    
    def get_name(self, item_id: int) -> Optional[str]:
        """ID로 원본 이름 조회"""
        # 역캐시 확인
        if item_id in self.reverse_cache:
            return self.reverse_cache[item_id]
        
        # DB 조회
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute(f"""
                    SELECT name FROM {self.table_name} 
                    WHERE {self.domain}_id = ?
                """, (item_id,))
                row = cur.fetchone()
                if row:
                    self.reverse_cache[item_id] = row[0]
                    return row[0]
        except Exception as e:
            print(f"⚠️ {self.domain} 이름 조회 실패: {e}")
        
        return None
    
    def search(self, keyword: str, limit: int = 100) -> List[Tuple[int, str]]:
        """이름 검색"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute(f"""
                    SELECT {self.domain}_id, name FROM {self.table_name}
                    WHERE name LIKE ? OR display_name LIKE ?
                    ORDER BY name
                    LIMIT ?
                """, (f'%{keyword}%', f'%{keyword}%', limit))
                return cur.fetchall()
        except Exception as e:
            print(f"⚠️ {self.domain} 검색 실패: {e}")
            return []
    
    def get_stats(self) -> Dict:
        """통계 정보"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                total = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
                return {
                    'domain': self.domain,
                    'total': total,
                    'cache_size': len(self.cache),
                    'pending': len(self.pending_inserts)
                }
        except Exception as e:
            return {'error': str(e)}
    
    def format_id(self, item_id: int) -> str:
        """디버그용 ID 포맷팅"""
        return IDHelper.format_id(item_id, length=8)
    
    def close(self):
        """종료 시 저장"""
        self.flush()
        if IDHelper.DEBUG_MODE:
            stats = self.get_stats()
            print(f"📊 {self.domain} vocab 종료: 총 {stats.get('total', 0)}개")
            