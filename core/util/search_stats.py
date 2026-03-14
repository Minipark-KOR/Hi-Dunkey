#!/usr/bin/env python3
"""
검색 통계 공통 모듈 (GA4 보완용)
- 모든 검색 페이지에서 사용
- 시스템 성능 모니터링 (캐시 효율, 응답시간)
- 검색 패턴 분석 (초성비율, 실패율)
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple


class SearchStats:
    """
    검색 통계 관리자 (서버사이드)
    
    GA4와 중복되지 않는 시스템 성능 메트릭에 집중:
    - 캐시 효율성
    - 응답 시간
    - 인덱스 품질
    - 검색 엔진 최적화 지표
    """
    
    def __init__(self, domain: str, stats_dir: str = "data/stats"):
        """
        Args:
            domain: 도메인 ('school', 'meal', 'schedule')
            stats_dir: 통계 파일 저장 디렉토리
        """
        self.domain = domain
        self.stats_path = os.path.join(stats_dir, f"{domain}_stats.json")
        
        # 통계 데이터 구조 (시스템 성능 중심)
        self.stats = {
            "domain": domain,
            "total_searches": 0,
            "cache_hits": 0,
            "avg_response_time": 0.0,
            "max_response_time": 0.0,
            "min_response_time": float('inf'),
            
            "search_types": {
                "chosung": 0,      # 초성검색
                "general": 0,       # 일반검색
                "failed": 0         # 결과 없음
            },
            
            "index_quality": {
                "total_terms": 0,           # 전체 검색어 수
                "covered_terms": 0,          # 인덱스 커버리지
                "avg_results_per_search": 0.0
            },
            
            "performance_alerts": [],         # 성능 경고 로그
            "daily_stats": {},                 # 일별 통계
            "hourly_stats": {},                 # 시간별 통계
            
            "last_updated": None,
            "created_at": None
        }
        
        self._load_stats()
    
    def _load_stats(self):
        """통계 로드"""
        os.makedirs(os.path.dirname(self.stats_path), exist_ok=True)
        
        if os.path.exists(self.stats_path):
            try:
                with open(self.stats_path, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    # 버전 호환성 유지
                    for key in self.stats:
                        if key in loaded:
                            self.stats[key] = loaded[key]
            except:
                self.stats["created_at"] = datetime.now().isoformat()
        else:
            self.stats["created_at"] = datetime.now().isoformat()
            self._save_stats()
    
    def _save_stats(self):
        """통계 저장"""
        try:
            with open(self.stats_path, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 통계 저장 실패: {e}")
    
    def _update_time_stats(self):
        """시간대/일별 통계 업데이트"""
        now = datetime.now()
        hour_key = now.strftime("%Y-%m-%d %H:00")
        day_key = now.strftime("%Y-%m-%d")
        
        self.stats["hourly_stats"][hour_key] = self.stats["hourly_stats"].get(hour_key, 0) + 1
        self.stats["daily_stats"][day_key] = self.stats["daily_stats"].get(day_key, 0) + 1
        
        # 오래된 데이터 정리 (30일)
        self._cleanup_old_stats()
    
    def _cleanup_old_stats(self, days: int = 30):
        """30일 이상 된 통계 정리"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        self.stats["daily_stats"] = {
            k: v for k, v in self.stats["daily_stats"].items() 
            if k >= cutoff
        }
        
        cutoff_hour = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:00")
        self.stats["hourly_stats"] = {
            k: v for k, v in self.stats["hourly_stats"].items() 
            if k >= cutoff_hour
        }
    
    def record_search(self, query: str, response_time: float, 
                     result_count: int, is_chosung: bool, 
                     cache_hit: bool = False, index_covered: bool = True):
        """
        검색 기록 (시스템 성능 중심)
        
        Args:
            query: 검색어
            response_time: 응답시간 (ms)
            result_count: 결과 수
            is_chosung: 초성검색 여부
            cache_hit: 캐시 히트 여부
            index_covered: 인덱스 커버리지
        """
        self.stats["total_searches"] += 1
        
        # 캐시 효율
        if cache_hit:
            self.stats["cache_hits"] += 1
        
        # 응답시간 통계
        self.stats["avg_response_time"] = (
            (self.stats["avg_response_time"] * (self.stats["total_searches"] - 1) + response_time)
            / self.stats["total_searches"]
        )
        self.stats["max_response_time"] = max(self.stats["max_response_time"], response_time)
        self.stats["min_response_time"] = min(self.stats["min_response_time"], response_time)
        
        # 검색 타입
        if result_count == 0:
            self.stats["search_types"]["failed"] += 1
        elif is_chosung:
            self.stats["search_types"]["chosung"] += 1
        else:
            self.stats["search_types"]["general"] += 1
        
        # 인덱스 품질
        self.stats["index_quality"]["total_terms"] += 1
        if index_covered:
            self.stats["index_quality"]["covered_terms"] += 1
        
        # 평균 결과 수
        total = self.stats["total_searches"]
        current_avg = self.stats["index_quality"]["avg_results_per_search"]
        self.stats["index_quality"]["avg_results_per_search"] = (
            (current_avg * (total - 1) + result_count) / total
        )
        
        # 성능 경고 (응답시간 1초 이상)
        if response_time > 1000:
            self.stats["performance_alerts"].append({
                "time": datetime.now().isoformat(),
                "query": query[:20],
                "response_time": response_time,
                "result_count": result_count
            })
            # 최근 100개만 유지
            if len(self.stats["performance_alerts"]) > 100:
                self.stats["performance_alerts"] = self.stats["performance_alerts"][-100:]
        
        # 시간대 통계
        self._update_time_stats()
        
        self.stats["last_updated"] = datetime.now().isoformat()
        
        # 100회마다 저장
        if self.stats["total_searches"] % 100 == 0:
            self._save_stats()
    
    def get_performance_report(self) -> Dict:
        """성능 리포트 (시스템 관리자용)"""
        cache_efficiency = 0
        if self.stats["total_searches"] > 0:
            cache_efficiency = (self.stats["cache_hits"] / self.stats["total_searches"]) * 100
        
        index_coverage = 0
        if self.stats["index_quality"]["total_terms"] > 0:
            index_coverage = (self.stats["index_quality"]["covered_terms"] / 
                            self.stats["index_quality"]["total_terms"]) * 100
        
        return {
            "domain": self.stats["domain"],
            "period": {
                "total_searches": self.stats["total_searches"],
                "cache_efficiency": f"{cache_efficiency:.1f}%",
                "avg_response": f"{self.stats['avg_response_time']:.2f}ms",
                "max_response": f"{self.stats['max_response_time']:.2f}ms",
                "min_response": f"{self.stats['min_response_time']:.2f}ms"
            },
            "index_quality": {
                "coverage": f"{index_coverage:.1f}%",
                "avg_results": f"{self.stats['index_quality']['avg_results_per_search']:.1f}개",
                "total_terms": self.stats["index_quality"]["total_terms"]
            },
            "search_types": {
                "chosung": self.stats["search_types"]["chosung"],
                "general": self.stats["search_types"]["general"],
                "failed": self.stats["search_types"]["failed"]
            },
            "recent_alerts": self.stats["performance_alerts"][-10:],
            "last_updated": self.stats["last_updated"]
        }
    
    def check_health(self) -> Tuple[bool, List[str]]:
        """
        시스템 건강 상태 체크
        
        Returns:
            (정상여부, 경고메시지 리스트)
        """
        warnings = []
        
        # 캐시 효율 체크
        cache_rate = 0
        if self.stats["total_searches"] > 0:
            cache_rate = (self.stats["cache_hits"] / self.stats["total_searches"]) * 100
            if cache_rate < 70:
                warnings.append(f"캐시 효율 낮음: {cache_rate:.1f}% (기준 70%)")
        
        # 응답시간 체크
        if self.stats["avg_response_time"] > 500:
            warnings.append(f"평균 응답시간 높음: {self.stats['avg_response_time']:.1f}ms (기준 500ms)")
        
        if self.stats["max_response_time"] > 2000:
            warnings.append(f"최대 응답시간 초과: {self.stats['max_response_time']:.1f}ms (기준 2000ms)")
        
        # 실패율 체크
        fail_rate = 0
        if self.stats["total_searches"] > 0:
            fail_rate = (self.stats["search_types"]["failed"] / self.stats["total_searches"]) * 100
            if fail_rate > 20:
                warnings.append(f"검색 실패율 높음: {fail_rate:.1f}% (기준 20%)")
        
        # 인덱스 커버리지 체크
        coverage = 0
        if self.stats["index_quality"]["total_terms"] > 0:
            coverage = (self.stats["index_quality"]["covered_terms"] / 
                       self.stats["index_quality"]["total_terms"]) * 100
            if coverage < 90:
                warnings.append(f"인덱스 커버리지 낮음: {coverage:.1f}% (기준 90%)")
        
        return len(warnings) == 0, warnings
    
    def print_performance_report(self):
        """성능 리포트 출력"""
        report = self.get_performance_report()
        healthy, warnings = self.check_health()
        
        print(f"\n📊 [{report['domain']}] 검색 시스템 성능 리포트")
        print("=" * 70)
        print(f"📈 전체 검색: {report['period']['total_searches']}회")
        print(f"⚡ 응답시간: {report['period']['avg_response']} (최대: {report['period']['max_response']})")
        print(f"💾 캐시 효율: {report['period']['cache_efficiency']}")
        print("-" * 70)
        print(f"📚 인덱스 품질:")
        print(f"   - 커버리지: {report['index_quality']['coverage']}")
        print(f"   - 평균 결과: {report['index_quality']['avg_results']}")
        print(f"   - 전체 용어: {report['index_quality']['total_terms']}개")
        print("-" * 70)
        print(f"🎯 검색 유형: 초성 {report['search_types']['chosung']}회, "
              f"일반 {report['search_types']['general']}회, "
              f"실패 {report['search_types']['failed']}회")
        
        if warnings:
            print("-" * 70)
            print("⚠️ 경고:")
            for w in warnings:
                print(f"   {w}")
        
        if report['recent_alerts']:
            print("-" * 70)
            print("🚨 최근 성능 경고:")
            for alert in report['recent_alerts'][-3:]:
                print(f"   {alert['time'][:16]} | {alert['query']} | {alert['response_time']}ms")
    
    def reset(self, confirm: bool = False):
        """통계 초기화 (관리자용)"""
        if not confirm:
            print("⚠️ 정말 초기화하려면 confirm=True 하세요")
            return
        
        self.stats = {
            "domain": self.domain,
            "total_searches": 0,
            "cache_hits": 0,
            "avg_response_time": 0.0,
            "max_response_time": 0.0,
            "min_response_time": float('inf'),
            "search_types": {"chosung": 0, "general": 0, "failed": 0},
            "index_quality": {"total_terms": 0, "covered_terms": 0, "avg_results_per_search": 0.0},
            "performance_alerts": [],
            "daily_stats": {},
            "hourly_stats": {},
            "last_updated": None,
            "created_at": self.stats["created_at"]
        }
        self._save_stats()
        print("✅ 통계 초기화 완료")


# ========================================================
# 테스트
# ========================================================
if __name__ == "__main__":
    stats = SearchStats("school")
    
    # 가상 데이터
    for i in range(100):
        stats.record_search(
            query=f"검색어{i}",
            response_time=50 + (i % 100),  # 50-150ms
            result_count=(i % 5) + 1,
            is_chosung=(i % 3 == 0),
            cache_hit=(i % 2 == 0),
            index_covered=(i % 10 != 0)
        )
    
    stats.print_performance_report()
    