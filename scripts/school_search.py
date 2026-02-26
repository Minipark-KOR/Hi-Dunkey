#!/usr/bin/env python3
"""
학교 검색 시스템 - 초경량 버전 with 통계 & health check
- 선택 후 바로 학사일정으로 이동
- core.search_stats로 성능 모니터링
- --health 옵션으로 시스템 건강 상태 즉시 확인
- --monitor 옵션으로 크론탭 자동 모니터링
"""
import os
import json
import sqlite3
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# 상위 디렉토리 import를 위한 경로 추가
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.search_stats import SearchStats


# ========================================================
# 초성 유틸리티
# ========================================================
def extract_chosung(text: str) -> str:
    """한글 텍스트에서 초성만 추출"""
    CHOSUNG = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 
               'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 
               'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    
    result = []
    for char in text:
        if '가' <= char <= '힣':
            code = ord(char) - ord('가')
            result.append(CHOSUNG[code // (21 * 28)])
        else:
            result.append(char.lower())
    return ''.join(result)


def is_chosung_query(query: str) -> bool:
    """초성 검색어 확인"""
    if not query:
        return False
    chosung_range = set('ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ')
    return all(c in chosung_range for c in query)


# ========================================================
# 초경량 검색 시스템
# ========================================================
class LightSchoolSearch:
    """초경량 학교 검색 with 성능 통계 & health check"""
    
    def __init__(self, index_path: str = "data/school_light.json", 
                 detail_db: str = "data/master/school_master.db"):
        """
        Args:
            index_path: 인덱스 파일 경로
            detail_db: 학교 상세정보 DB 경로
        """
        self.index_path = index_path
        self.detail_db = detail_db
        self.schools = []  # [{code, name, addr_short}]
        self.chosung_map = {}  # 초성 -> 학교 인덱스 리스트
        self.cache = {}  # 검색 결과 캐시
        
        # 통계 모듈 (core에서 import)
        self.stats = SearchStats("school")
        
        # 검색 예시
        self.example = "💡 예: '한국고', 'ㅎㄱㄱ', '서울'"
        
        self._load_index()
    
    def _load_index(self):
        """인덱스 로드 (없으면 생성)"""
        if os.path.exists(self.index_path):
            try:
                with open(self.index_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.schools = data['schools']
                    self.chosung_map = data['chosung']
                print(f"✅ 인덱스 로드: {len(self.schools)}개 학교")
            except:
                print("⚠️ 인덱스 파일 손상, 재생성합니다")
                self._build_index()
        else:
            self._build_index()
    
    def _build_index(self):
        """초경량 인덱스 생성"""
        print("🔨 학교 인덱스 생성 중...")
        
        if not os.path.exists(self.detail_db):
            print(f"❌ 학교 DB 없음: {self.detail_db}")
            return
        
        with sqlite3.connect(self.detail_db) as conn:
            cur = conn.execute("""
                SELECT sc_code, sc_name, 
                       substr(address, 1, 8) as short_addr
                FROM schools
                WHERE address IS NOT NULL AND address != ''
            """)
            
            self.schools = []
            self.chosung_map = {}
            
            for idx, (code, name, addr) in enumerate(cur):
                school = {
                    "code": code,
                    "name": name,
                    "addr": addr if addr else "주소 정보 없음"
                }
                self.schools.append(school)
                
                # 학교명 초성 인덱싱
                chosung = extract_chosung(name)
                if chosung not in self.chosung_map:
                    self.chosung_map[chosung] = []
                self.chosung_map[chosung].append(idx)
        
        # 저장
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        with open(self.index_path, 'w', encoding='utf-8') as f:
            json.dump({
                "schools": self.schools,
                "chosung": self.chosung_map
            }, f, ensure_ascii=False)
        
        print(f"✅ 인덱스 생성 완료: {len(self.schools)}개 학교")
    
    def _check_cache(self, query: str) -> Tuple[bool, Optional[List]]:
        """캐시 확인"""
        cache_key = f"search:{query}"
        if cache_key in self.cache:
            return True, self.cache[cache_key]
        return False, None
    
    def _do_search(self, query: str) -> List[Dict]:
        """실제 검색 로직"""
        query = query.strip().lower()
        is_chosung = is_chosung_query(query)
        results = []
        seen = set()
        
        if is_chosung:
            # 초성 검색
            for chosung, indices in self.chosung_map.items():
                if query in chosung:
                    for idx in indices:
                        school = self.schools[idx]
                        if school['code'] not in seen:
                            seen.add(school['code'])
                            results.append(school)
                            if len(results) >= 15:
                                break
        else:
            # 일반 검색
            for school in self.schools:
                if (query in school['name'].lower() or 
                    query in school['addr'].lower()):
                    if school['code'] not in seen:
                        seen.add(school['code'])
                        results.append(school)
                        if len(results) >= 15:
                            break
        
        return results
    
    def search(self, query: str) -> List[Dict]:
        """
        학교 검색 + 통계 기록
        
        Returns:
            [{"code": "B100001234", "name": "한국고", "addr": "서울 강남"}]
        """
        start_time = time.time()
        
        if len(query.strip()) < 2:
            return []
        
        original_query = query
        query = query.strip().lower()
        is_chosung = is_chosung_query(query)
        
        # 캐시 확인
        cache_hit, cached_results = self._check_cache(query)
        if cache_hit:
            response_time = (time.time() - start_time) * 1000
            # 통계 기록 (캐시 히트)
            self.stats.record_search(
                query=original_query,
                response_time=response_time,
                result_count=len(cached_results),
                is_chosung=is_chosung,
                cache_hit=True,
                index_covered=(len(cached_results) > 0)
            )
            return cached_results
        
        # 실제 검색
        results = self._do_search(query)
        
        # 캐시에 저장
        cache_key = f"search:{query}"
        self.cache[cache_key] = results
        
        # 통계 기록
        response_time = (time.time() - start_time) * 1000
        self.stats.record_search(
            query=original_query,
            response_time=response_time,
            result_count=len(results),
            is_chosung=is_chosung,
            cache_hit=False,
            index_covered=(len(results) > 0)
        )
        
        return results
    
    def get_school_code(self, query: str) -> Optional[str]:
        """검색 후 첫 번째 학교 코드 반환 (바로 이동용)"""
        results = self.search(query)
        return results[0]['code'] if results else None
    
    def get_stats_report(self):
        """통계 리포트 조회"""
        return self.stats.get_performance_report()
    
    def health_check(self) -> bool:
        """
        시스템 건강 상태 체크
        Returns: 정상이면 True, 문제 있으면 False
        """
        healthy, warnings = self.stats.check_health()
        
        if healthy:
            print("✅ 시스템 건강 상태: 정상")
            print(f"   - 캐시 효율: {self._get_cache_efficiency():.1f}%")
            print(f"   - 평균 응답: {self.stats.stats['avg_response_time']:.1f}ms")
            return True
        else:
            print("⚠️ 시스템 건강 상태: 주의 필요")
            for w in warnings:
                print(f"   - {w}")
            return False
    
    def _get_cache_efficiency(self) -> float:
        """캐시 효율 계산"""
        if self.stats.stats["total_searches"] == 0:
            return 0.0
        return (self.stats.stats["cache_hits"] / self.stats.stats["total_searches"]) * 100
    
    def print_stats(self):
        """통계 출력"""
        self.stats.print_performance_report()
    
    def reset_stats(self, confirm: bool = False):
        """통계 초기화 (관리자용)"""
        self.stats.reset(confirm)


# ========================================================
# 모니터링 함수 (크론탭용)
# ========================================================
def monitoring_example():
    """
    크론탭에 등록할 모니터링 스크립트
    0 9 * * * cd /path/to/project && python scripts/school_search.py --monitor
    """
    # 로그 디렉토리 생성
    os.makedirs("logs", exist_ok=True)
    
    search = LightSchoolSearch()
    healthy, warnings = search.stats.check_health()
    cache_efficiency = search._get_cache_efficiency()
    
    # 로그 메시지 생성
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if not healthy:
        log_msg = f"[{timestamp}] ⚠️ 시스템 주의 필요 (캐시효율: {cache_efficiency:.1f}%)\n"
        for w in warnings:
            log_msg += f"  - {w}\n"
        
        # 파일로 저장
        with open("logs/health_monitor.log", "a", encoding='utf-8') as f:
            f.write(log_msg)
        
        # 심각한 문제면 표준 출력에도 표시 (크론 메일로 전송됨)
        if len(warnings) >= 2 or cache_efficiency < 50:
            print(log_msg)
        
        return False
    
    # 정상이면 간단한 로그만 남김
    with open("logs/health_monitor.log", "a", encoding='utf-8') as f:
        f.write(f"[{timestamp}] ✅ 시스템 정상 (캐시효율: {cache_efficiency:.1f}%)\n")
    
    return True


# ========================================================
# CLI
# ========================================================
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="학교 검색 시스템")
    parser.add_argument("--search", "-s", help="검색어")
    parser.add_argument("--interactive", "-i", action="store_true", help="대화형 모드")
    parser.add_argument("--stats", action="store_true", help="성능 통계 보기")
    parser.add_argument("--health", action="store_true", help="건강 상태 체크")
    parser.add_argument("--monitor", action="store_true", help="모니터링 모드 (크론탭용)")
    parser.add_argument("--rebuild", action="store_true", help="인덱스 재구성")
    parser.add_argument("--reset-stats", action="store_true", help="통계 초기화")
    parser.add_argument("--code", help="학교 코드로 정보 조회")
    
    args = parser.parse_args()
    
    # 로그 디렉토리 생성
    os.makedirs("logs", exist_ok=True)
    
    search = LightSchoolSearch()
    
    if args.monitor:
        monitoring_example()
        return
    
    if args.rebuild:
        search._build_index()
        return
    
    if args.stats:
        search.print_stats()
        return
    
    if args.health:
        search.health_check()
        return
    
    if args.reset_stats:
        confirm = input("⚠️ 정말 통계를 초기화하시겠습니까? (yes/no): ")
        if confirm.lower() == 'yes':
            search.reset_stats(confirm=True)
        return
    
    if args.code:
        # 학교 코드로 상세 정보 조회
        for school in search.schools:
            if school['code'] == args.code:
                print(f"\n📄 {school['name']}")
                print(f"   코드: {school['code']}")
                print(f"   주소: {school['addr']}")
                return
        print(f"❌ 코드 {args.code}를 찾을 수 없습니다")
        return
    
    if args.interactive:
        print("\n🏫 학교 검색 시스템")
        print("=" * 60)
        print(search.example)
        print("※ 2글자 이상 입력, 번호 선택시 학사일정으로 이동")
        print("※ 'q' 입력시 종료")
        print("-" * 60)
        
        while True:
            try:
                query = input("\n🔍 검색: ").strip()
                
                if query.lower() in ['q', 'quit', 'exit']:
                    print("👋 종료합니다")
                    break
                
                results = search.search(query)
                
                if not results:
                    if len(query) >= 2:
                        print("  📭 검색 결과가 없습니다")
                    continue
                
                print(f"\n  📋 검색 결과 ({len(results)}개)")
                for i, s in enumerate(results, 1):
                    addr_display = s['addr'][:10] + "..." if len(s['addr']) > 10 else s['addr']
                    print(f"  {i:2d}. {s['name']} - {addr_display}")
                
                choice = input("\n  번호 선택 (0:다시검색): ").strip()
                
                if choice == '0':
                    continue
                
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(results):
                        selected = results[idx]
                        print(f"\n  ✅ {selected['name']} 선택")
                        print(f"  📅 학사일정 화면으로 이동 (school_code: {selected['code']})")
                        # TODO: 여기서 학사일정 화면 호출
                except ValueError:
                    print("  ⚠️ 숫자를 입력하세요")
                    
            except KeyboardInterrupt:
                print("\n👋 종료합니다")
                break
            except Exception as e:
                print(f"  ⚠️ 오류 발생: {e}")
    
    elif args.search:
        results = search.search(args.search)
        if results:
            # JSON 형식으로 출력 (다른 프로그램에서 파싱 쉽게)
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            print("[]")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()