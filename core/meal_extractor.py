#!/usr/bin/env python3
"""
급식 메뉴 메타데이터 추출기
- unknown_meta로 새로운 패턴 수집
- WAL 모드 활성화
- 알레르기 번호 필터링
- 배치 저장 최적화
"""
import re
import sqlite3
from typing import List, Tuple, Dict, Any, Optional
from core.kst_time import now_kst


class MealMetaExtractor:
    """급식 메뉴 메타데이터 추출"""
    
    # 메타데이터 타입 정의
    COOKING_METHOD = 'cooking_method'  # 조리법
    INGREDIENT = 'ingredient'           # 식재료
    SPECIAL_MARK = 'special_mark'       # 특수표시
    ORIGIN = 'origin'                   # 원산지 (추후 사용)
    
    # 알려진 패턴 (Phase 1에서는 최소한으로)
    KNOWN_MARKS = {'★', '☆', '♥', '❤️'}
    
    # 식재료 패턴 (간단한 키워드 매칭용)
    INGREDIENT_PATTERNS = [
        '고기', '돼지', '소고기', '닭고기', '생선', '오징어', 
        '새우', '조개', '계란', '두부', '콩', '감자', '고구마'
    ]
    
    def __init__(self, db_path: str = None, batch_size: int = 100):
        """
        Args:
            db_path: unknown_patterns 저장할 DB 경로 (없으면 미저장)
            batch_size: 배치 저장 크기
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.pending_unknowns = []  # 배치 저장용 큐
        
        if db_path:
            self._init_unknown_table()
    
    def _init_unknown_table(self):
        """unknown 패턴 저장 테이블 생성 (WAL 모드 활성화)"""
        with sqlite3.connect(self.db_path) as conn:
            # WAL 모드 활성화 (동시성 향상)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS unknown_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_text TEXT NOT NULL,
                    part_type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    context TEXT,
                    frequency INT DEFAULT 1,
                    first_seen TEXT,
                    last_seen TEXT,
                    is_reviewed BOOLEAN DEFAULT 0,
                    suggested_type TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_unknown_value ON unknown_patterns(value)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_unknown_freq ON unknown_patterns(frequency DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_unknown_reviewed ON unknown_patterns(is_reviewed)")
    
    def _is_noise(self, text: str) -> bool:
        """노이즈(알레르기 번호, 단순 숫자 등) 필터링"""
        if not text:
            return True
        
        # 1. 알레르기 번호 패턴 (1.5.6, 1-2-3, 1 2 3 등)
        if re.match(r'^[\d\s\.\-]+$', text):
            return True
        
        # 2. 너무 긴 텍스트 (50자 이상)
        if len(text) > 50:
            return True
        
        # 3. 너무 짧은 텍스트 (1글자)
        if len(text) < 2:
            return True
        
        # 4. 특수문자만 있는 경우
        if re.match(r'^[^\w\s가-힣]+$', text):
            return True
        
        return False
    
    def _is_known_pattern(self, text: str) -> bool:
        """알려진 패턴인지 확인 (Phase 1에서는 최소한으로)"""
        # 식재료 패턴
        if any(i in text for i in self.INGREDIENT_PATTERNS):
            return True
        
        # 조리법 패턴 (간단한 키워드)
        cooking = ['볶음', '찜', '튀김', '구이', '조림', '무침', '회', '전']
        if any(c in text for c in cooking):
            return True
        
        return False
    
    def _is_ingredient(self, text: str) -> bool:
        """식재료 여부 판별"""
        return any(i in text for i in self.INGREDIENT_PATTERNS)
    
    def _save_unknown(self, original: str, part_type: str, value: str, context: str = ""):
        """배치로 unknown 패턴 저장"""
        if not self.db_path:
            return
        
        # 노이즈 필터링 (한 번 더 체크)
        if self._is_noise(value):
            return
        
        # 배치에 추가
        self.pending_unknowns.append((original, part_type, value, context))
        
        # 배치 크기 도달 시 저장
        if len(self.pending_unknowns) >= self.batch_size:
            self.flush()
    
    def flush(self):
        """pending된 unknown 패턴 일괄 저장"""
        if not self.pending_unknowns:
            return
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                
                now = now_kst().isoformat()
                for original, part_type, value, context in self.pending_unknowns:
                    # 이미 있는 패턴인지 확인
                    cur = conn.execute("""
                        SELECT id, frequency FROM unknown_patterns
                        WHERE part_type = ? AND value = ?
                    """, (part_type, value))
                    row = cur.fetchone()
                    
                    if row:
                        # 기존 패턴 업데이트
                        conn.execute("""
                            UPDATE unknown_patterns 
                            SET frequency = frequency + 1,
                                last_seen = ?,
                                context = CASE 
                                    WHEN ? != '' AND context IS NULL THEN ? 
                                    ELSE context 
                                END
                            WHERE id = ?
                        """, (now, context, context, row[0]))
                    else:
                        # 새 패턴 저장
                        conn.execute("""
                            INSERT INTO unknown_patterns 
                            (original_text, part_type, value, context, first_seen, last_seen)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (original[:200], part_type, value, context, now, now))
            
            self.pending_unknowns.clear()
            
        except Exception as e:
            print(f"⚠️ unknown 패턴 저장 실패: {e}")
    
    def extract(self, menu_text: str) -> List[Tuple[str, str]]:
        """
        개별 메뉴 텍스트에서 메타데이터 추출
        
        Args:
            menu_text: 개별 메뉴 텍스트 (예: "김치찌개(돼지고기)★")
        
        Returns:
            [(meta_type, meta_value)] 리스트
        """
        results = []
        unknown_parts = []
        
        if not menu_text:
            return results
        
        # 1. 괄호 안 내용 추출 (조리법/식재료) - 괄호/대괄호 모두 처리
        bracket_matches = re.findall(r'[\(\[]([^)\]]+)[\)\]]', menu_text)
        for match in bracket_matches:
            # 쉼표/슬래시로 구분된 경우 분리
            parts = re.split(r'[,\s/]+', match)
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                
                # 노이즈 필터링
                if self._is_noise(part):
                    continue
                
                # 알려진 패턴인지 확인
                if self._is_known_pattern(part):
                    if self._is_ingredient(part):
                        results.append((self.INGREDIENT, part))
                    else:
                        results.append((self.COOKING_METHOD, part))
                else:
                    unknown_parts.append(('bracket', part, match))
        
        # 2. 특수표시 추출
        special_matches = re.findall(r'[★☆◆◇●○■□▲△♡♥❤️]', menu_text)
        for mark in special_matches:
            if mark in self.KNOWN_MARKS:
                results.append((self.SPECIAL_MARK, mark))
            else:
                unknown_parts.append(('mark', mark, ''))
        
        # 3. unknown 패턴 저장 (배치 처리)
        for part_type, value, context in unknown_parts:
            self._save_unknown(menu_text, part_type, value, context)
        
        return results
    
    def close(self):
        """종료 시 남은 데이터 저장"""
        self.flush()


# ========================================================
# unknown 패턴 분석 유틸리티 (Phase 2에서 사용)
# ========================================================
class UnknownPatternAnalyzer:
    """unknown 패턴 분석기 (Phase 2에서 사용)"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_top_patterns(self, limit: int = 50, min_frequency: int = 10) -> List[Dict[str, Any]]:
        """가장 많이 발견된 unknown 패턴 조회"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            
            cur = conn.execute("""
                SELECT 
                    id,
                    part_type,
                    value,
                    frequency,
                    first_seen,
                    last_seen,
                    COUNT(*) OVER() as total_count
                FROM unknown_patterns
                WHERE is_reviewed = 0
                  AND frequency >= ?
                ORDER BY frequency DESC
                LIMIT ?
            """, (min_frequency, limit))
            
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    
    def suggest_meta_type(self, pattern: Dict[str, Any]) -> str:
        """패턴 기반 메타타입 제안"""
        value = pattern['value']
        
        # 원산지 패턴
        if any(word in value for word in ['국내산', '수입산', '미국산', '호주산', '중국산']):
            return 'origin'
        
        # 맛/조리법 패턴
        taste_words = ['매운', '순한', '달콤한', '짠', '신', '고소한']
        if any(word in value for word in taste_words):
            return 'taste'
        
        # 특수표시 패턴
        if pattern['part_type'] == 'mark':
            return 'special_mark'
        
        # 식재료 패턴 (새로 발견된 재료)
        ingredient_words = ['삼겹살', '목살', '안심', '등심']
        if any(word in value for word in ingredient_words):
            return 'ingredient'
        
        return 'unknown'
    
    def mark_as_reviewed(self, pattern_id: int, suggested_type: str):
        """패턴 검토 완료 처리"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE unknown_patterns
                SET is_reviewed = 1, suggested_type = ?
                WHERE id = ?
            """, (suggested_type, pattern_id))
    
    def get_statistics(self) -> Dict[str, Any]:
        """unknown 패턴 통계"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            
            total = conn.execute("SELECT COUNT(*) FROM unknown_patterns").fetchone()[0]
            reviewed = conn.execute("SELECT COUNT(*) FROM unknown_patterns WHERE is_reviewed = 1").fetchone()[0]
            
            by_type = conn.execute("""
                SELECT part_type, COUNT(*) as cnt
                FROM unknown_patterns
                GROUP BY part_type
                ORDER BY cnt DESC
            """).fetchall()
            
            top_values = conn.execute("""
                SELECT value, frequency
                FROM unknown_patterns
                ORDER BY frequency DESC
                LIMIT 10
            """).fetchall()
            
            return {
                'total': total,
                'reviewed': reviewed,
                'pending': total - reviewed,
                'by_type': dict(by_type),
                'top_values': top_values
            }
            