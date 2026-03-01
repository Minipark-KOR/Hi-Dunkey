#!/usr/bin/env python3
"""
위치정보 수집기 (Geo Collector)
- 학교, 행사, 급식 등 모든 도메인의 위치정보 통합 관리
- meta_vocab과 연동하여 일관된 메타데이터 체계 구축
- VWorld API 할당량 관리 및 캐싱 최적화
"""
import os
import hashlib
import time
import sqlite3
import requests  # ✅ 추가됨
from typing import Optional, Dict, List, Tuple
from urllib.parse import quote
from datetime import datetime

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import get_db_connection
from core.id_generator import IDGenerator
from core.meta_vocab import MetaVocabManager
from core.kst_time import now_kst
from constants.codes import VWORLD_API_KEY


class GeoCollector:
    """
    위치정보 수집기 - 통합 버전
    - 모든 도메인의 위치정보를 meta_vocab으로 통합 관리
    - 학교, 행사, 급식 등에서 공통으로 사용
    - VWorld API 할당량 관리 및 캐싱 최적화
    """
    
    # VWorld API 엔드포인트
    GEOCODE_URL = "https://api.vworld.kr/req/address"
    
    # API 할당량 (VWorld 일반 계정 기준)
    DAILY_API_LIMIT = 50000
    
    def __init__(self, global_db_path: str = "data/global_vocab.db", 
                 school_db_path: str = "data/master/school_master.db",
                 debug_mode: bool = False):
        """
        Args:
            global_db_path: 통합 vocab DB 경로
            school_db_path: 학교 마스터 DB 경로
            debug_mode: 디버그 모드
        """
        self.global_db_path = global_db_path
        self.school_db_path = school_db_path
        self.debug_mode = debug_mode
        
        # ✅ meta_vocab 통합 관리자
        self.meta_vocab = MetaVocabManager(global_db_path, debug_mode)
        
        # 캐시 (메모리)
        self.cache = {}  # address_hash -> (lon, lat)
        self.pending_inserts = []  # 배치 저장용
        
        # ✅ API 사용량 모니터링
        self.api_calls_today = 0
        self.api_limit = self.DAILY_API_LIMIT
        self._load_api_usage()
        
        self._init_tables()
        self._load_cache()
        
        if debug_mode:
            print(f"🗺️ GeoCollector 초기화 완료 (오늘 API 사용량: {self.api_calls_today}/{self.api_limit})")
    
    def _init_tables(self):
        """테이블 초기화"""
        with get_db_connection(self.global_db_path) as conn:
            # WAL 모드 활성화
            conn.execute("PRAGMA journal_mode=WAL")
            
            # 지오코딩 캐시 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    address_hash TEXT PRIMARY KEY,
                    original_address TEXT NOT NULL,
                    longitude REAL,
                    latitude REAL,
                    confidence TEXT,
                    last_queried TEXT,
                    query_count INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(original_address)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geo_coords ON geo_cache(longitude, latitude)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geo_queried ON geo_cache(last_queried)")
        
        # schools 테이블에 location_meta_ids 컬럼 추가
        if os.path.exists(self.school_db_path):
            with sqlite3.connect(self.school_db_path) as conn:
                try:
                    conn.execute("ALTER TABLE schools ADD COLUMN location_meta_ids TEXT")
                except sqlite3.OperationalError:
                    pass  # 이미 존재함
                
                try:
                    conn.execute("ALTER TABLE schools ADD COLUMN location_updated TEXT")
                except sqlite3.OperationalError:
                    pass
                
                try:
                    conn.execute("ALTER TABLE schools ADD COLUMN longitude REAL")
                    conn.execute("ALTER TABLE schools ADD COLUMN latitude REAL")
                except sqlite3.OperationalError:
                    pass
    
    def _load_api_usage(self):
        """오늘 API 사용량 로드"""
        try:
            today = now_kst().strftime("%Y-%m-%d")
            with get_db_connection(self.global_db_path) as conn:
                cur = conn.execute("""
                    SELECT SUM(query_count) FROM geo_cache 
                    WHERE last_queried LIKE ?
                """, (f"{today}%",))
                result = cur.fetchone()[0]
                self.api_calls_today = result if result else 0
        except Exception as e:
            if self.debug_mode:
                print(f"⚠️ API 사용량 로드 실패: {e}")
            self.api_calls_today = 0
    
    def _load_cache(self):
        """메모리 캐시 로드"""
        try:
            with get_db_connection(self.global_db_path) as conn:
                cur = conn.execute("""
                    SELECT original_address, longitude, latitude 
                    FROM geo_cache 
                    WHERE longitude IS NOT NULL
                """)
                for addr, lon, lat in cur:
                    addr_hash = self._hash_address(addr)
                    self.cache[addr_hash] = (lon, lat)
            
            if self.debug_mode:
                print(f"✅ 지오코딩 캐시 로드: {len(self.cache)}개")
                
        except Exception as e:
            if self.debug_mode:
                print(f"⚠️ 캐시 로드 실패: {e}")
    
    def _hash_address(self, address: str) -> str:
        """주소 해시 생성"""
        return hashlib.sha256(address.encode()).hexdigest()[:16]
    
    def _check_api_limit(self) -> bool:
        """API 할당량 확인"""
        if self.api_calls_today >= self.api_limit:
            remaining = self.api_limit - self.api_calls_today
            if self.debug_mode:
                print(f"⚠️ 오늘 API 할당량 초과 (사용: {self.api_calls_today}/{self.api_limit})")
            return False
        return True
    
    def _geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """
        VWorld API 호출 (캐시 적용, 할당량 관리)
        
        Returns:
            (longitude, latitude) or None
        """
        if not address or not VWORLD_API_KEY:
            return None
        
        # 캐시 확인
        addr_hash = self._hash_address(address)
        if addr_hash in self.cache:
            if self.debug_mode:
                print(f"🔍 캐시 히트: {address[:30]}...")
            return self.cache[addr_hash]
        
        # 할당량 확인
        if not self._check_api_limit():
            return None
        
        # API 호출
        for addr_type in ['road', 'jibun']:
            try:
                encoded_addr = quote(address)
                url = f"{self.GEOCODE_URL}?service=address&request=getcoord&version=2.0&crs=epsg:4326&address={encoded_addr}&refine=true&simple=false&format=json&type={addr_type}&key={VWORLD_API_KEY}"
                
                response = requests.get(url, timeout=10)
                
                # API 호출 카운트 증가
                self.api_calls_today += 1
                
                # 응답 상태 확인
                if response.status_code == 429:  # Too Many Requests
                    if self.debug_mode:
                        print("⚠️ API 요청 제한 초과 (429)")
                    time.sleep(5)
                    continue
                
                data = response.json()
                
                # API 응답 상태 확인
                response_status = data.get('response', {}).get('status')
                if response_status == 'OK':
                    result = data['response']['result']
                    point = result.get('point', {})
                    lon = float(point.get('x', 0))
                    lat = float(point.get('y', 0))
                    
                    if lon and lat:
                        # 캐시에 저장
                        self.cache[addr_hash] = (lon, lat)
                        self._save_to_cache(address, lon, lat, result.get('confidence', 'HIGH'))
                        
                        if self.debug_mode:
                            print(f"✅ API 성공: ({lon:.6f}, {lat:.6f})")
                        
                        return (lon, lat)
                
                elif response_status == 'LIMIT_EXCEEDED':
                    if self.debug_mode:
                        print("⚠️ API 할당량 초과 응답")
                    self.api_calls_today = self.api_limit  # 할당량 초과로 설정
                    return None
                
                time.sleep(0.1)
                
            except requests.exceptions.Timeout:
                if self.debug_mode:
                    print("⚠️ API 타임아웃")
                time.sleep(1)
            except Exception as e:
                if self.debug_mode:
                    print(f"⚠️ API 호출 실패: {e}")
                time.sleep(1)
        
        return None
    
    def _save_to_cache(self, address: str, lon: float, lat: float, confidence: str = 'HIGH'):
        """캐시에 저장 (배치 처리)"""
        self.pending_inserts.append((address, lon, lat, confidence))
        
        # 10개 모이면 저장
        if len(self.pending_inserts) >= 10:
            self.flush()
    
    def flush(self):
        """pending된 캐시 일괄 저장"""
        if not self.pending_inserts:
            return
        
        try:
            with get_db_connection(self.global_db_path) as conn:
                now = now_kst().isoformat()
                
                for address, lon, lat, confidence in self.pending_inserts:
                    addr_hash = self._hash_address(address)
                    
                    # 이미 있는지 확인
                    cur = conn.execute("""
                        SELECT query_count FROM geo_cache WHERE address_hash = ?
                    """, (addr_hash,))
                    row = cur.fetchone()
                    
                    if row:
                        # 기존 레코드 업데이트
                        conn.execute("""
                            UPDATE geo_cache 
                            SET query_count = query_count + 1,
                                last_queried = ?,
                                longitude = ?,
                                latitude = ?,
                                confidence = ?
                            WHERE address_hash = ?
                        """, (now, lon, lat, confidence, addr_hash))
                    else:
                        # 새 레코드 삽입
                        conn.execute("""
                            INSERT INTO geo_cache 
                            (address_hash, original_address, longitude, latitude, confidence, last_queried)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (addr_hash, address, lon, lat, confidence, now))
            
            if self.debug_mode:
                print(f"💾 지오코딩 캐시 저장: {len(self.pending_inserts)}개")
            
            self.pending_inserts.clear()
            
        except Exception as e:
            print(f"❌ 캐시 저장 실패: {e}")
    
    def save_location(self, domain: str, item_id: str, address: str) -> Dict:
        """
        위치 정보 저장 (실제 도메인에 저장)
        
        Args:
            domain: 실제 도메인 ('school', 'event', 'meal')
            item_id: 항목 ID
            address: 주소
        
        Returns:
            위치 정보 딕셔너리
        """
        # 좌표 조회
        coords = self._geocode(address)
        
        meta_ids = []
        result = {
            'domain': domain,
            'item_id': item_id,
            'address': address,
            'coords': coords,
            'meta_ids': []
        }
        
        # 주소를 meta_vocab에 저장
        if address:
            addr_id = self.meta_vocab.get_or_create(domain, 'address', address)
            meta_ids.append(addr_id)
            result['address_id'] = addr_id
        
        # 좌표를 meta_vocab에 저장
        if coords:
            lon, lat = coords
            
            # 위치 문자열 저장
            loc_str = f"{lon:.6f},{lat:.6f}"
            loc_id = self.meta_vocab.get_or_create(domain, 'location', loc_str)
            meta_ids.append(loc_id)
            result['location_id'] = loc_id
            result['longitude'] = lon
            result['latitude'] = lat
        
        result['meta_ids'] = meta_ids
        
        # 도메인별 테이블에 저장
        self._save_to_domain_table(domain, item_id, meta_ids, coords)
        
        return result
    
    def _save_to_domain_table(self, domain: str, item_id: str, 
                              meta_ids: List[int], coords: Optional[Tuple]):
        """도메인별 테이블에 위치 정보 저장"""
        
        if domain == 'school':
            with sqlite3.connect(self.school_db_path) as conn:
                meta_ids_str = ','.join(map(str, meta_ids)) if meta_ids else None
                
                conn.execute("""
                    UPDATE schools 
                    SET location_meta_ids = ?,
                        location_updated = ?
                    WHERE sc_code = ?
                """, (meta_ids_str, now_kst().isoformat(), item_id))
                
                if coords:
                    lon, lat = coords
                    try:
                        conn.execute("""
                            UPDATE schools 
                            SET longitude = ?, latitude = ?
                            WHERE sc_code = ?
                        """, (lon, lat, item_id))
                    except sqlite3.OperationalError:
                        pass  # 컬럼 없으면 무시
    
    def get_location(self, domain: str, item_id: str) -> Optional[Dict]:
        """
        위치 정보 조회
        
        Returns:
            {
                'address': 주소,
                'longitude': 경도,
                'latitude': 위도,
                'location_id': 위치 ID,
                'address_id': 주소 ID
            }
        """
        result = {}
        
        if domain == 'school':
            with sqlite3.connect(self.school_db_path) as conn:
                cur = conn.execute("""
                    SELECT location_meta_ids, longitude, latitude 
                    FROM schools 
                    WHERE sc_code = ?
                """, (item_id,))
                row = cur.fetchone()
                
                if row and row[0]:
                    meta_ids = list(map(int, row[0].split(',')))
                    
                    for meta_id in meta_ids:
                        info = self.meta_vocab.get_meta_info(meta_id)
                        if info:
                            _, meta_type, value = info
                            if meta_type == 'address':
                                result['address'] = value
                                result['address_id'] = meta_id
                            elif meta_type == 'location':
                                result['location_id'] = meta_id
                                if ',' in value:
                                    lon, lat = value.split(',')
                                    result['longitude'] = float(lon)
                                    result['latitude'] = float(lat)
                    
                    if row[1] and row[2]:
                        result['longitude'] = row[1]
                        result['latitude'] = row[2]
        
        return result if result else None
    
    def find_nearby(self, domain: str, lat: float, lon: float, 
                    radius_km: float = 5.0) -> List[Dict]:
        """
        반경 내 항목 검색 (Haversine 공식)
        
        Args:
            domain: 도메인 ('school')
            lat: 기준 위도
            lon: 기준 경도
            radius_km: 반경 (km)
        
        Returns:
            [{item_id, name, distance, ...}]
        """
        results = []
        
        if domain == 'school':
            with sqlite3.connect(self.school_db_path) as conn:
                try:
                    # ✅ WHERE 절에서 직접 계산 (HAVING보다 호환성 좋음)
                    cur = conn.execute("""
                        SELECT sc_code, sc_name, address,
                               longitude, latitude,
                               (6371 * acos(cos(radians(?)) * cos(radians(latitude)) 
                               * cos(radians(longitude) - radians(?)) 
                               + sin(radians(?)) * sin(radians(latitude)))) AS distance
                        FROM schools
                        WHERE longitude IS NOT NULL 
                          AND latitude IS NOT NULL
                          AND (6371 * acos(cos(radians(?)) * cos(radians(latitude)) 
                               * cos(radians(longitude) - radians(?)) 
                               + sin(radians(?)) * sin(radians(latitude)))) < ?
                        ORDER BY distance
                    """, (lat, lon, lat, lat, lon, lat, radius_km))
                    
                    for row in cur:
                        results.append({
                            'item_id': row[0],
                            'name': row[1],
                            'address': row[2],
                            'longitude': row[3],
                            'latitude': row[4],
                            'distance_km': round(row[5], 2)
                        })
                except sqlite3.OperationalError as e:
                    if self.debug_mode:
                        print(f"⚠️ 공간 검색 실패: {e}")
        
        return results
    
    def batch_update_schools(self, limit: int = 100) -> int:
        """
        학교 위치 일괄 업데이트
        
        Args:
            limit: 최대 변환할 학교 수
        
        Returns:
            성공한 학교 수
        """
        if not os.path.exists(self.school_db_path):
            print(f"❌ 학교 DB 없음: {self.school_db_path}")
            return 0
        
        # 할당량 확인
        if not self._check_api_limit():
            print(f"⚠️ 오늘 API 할당량 초과 ({self.api_calls_today}/{self.api_limit})")
            return 0
        
        with sqlite3.connect(self.school_db_path) as conn:
            schools = conn.execute("""
                SELECT sc_code, sc_name, address 
                FROM schools 
                WHERE address IS NOT NULL 
                  AND address != ''
                  AND (location_meta_ids IS NULL OR location_meta_ids = '')
                LIMIT ?
            """, (limit,)).fetchall()
        
        print(f"🔍 업데이트할 학교: {len(schools)}개 (오늘 사용량: {self.api_calls_today}/{self.api_limit})")
        
        success = 0
        for i, (sc_code, sc_name, address) in enumerate(schools, 1):
            # 진행 상황 표시
            remaining = self.api_limit - self.api_calls_today
            print(f"\r🔄 {i}/{len(schools)} 처리중... (API 잔여: {remaining})", end="")
            
            result = self.save_location('school', sc_code, address)
            
            if result.get('coords'):
                success += 1
            
            time.sleep(0.2)  # API 부하 분산
            
            # 중간 저장
            if i % 10 == 0:
                self.flush()
                self.meta_vocab.flush()
        
        print()  # 줄바꿈
        self.flush()
        self.meta_vocab.flush()
        
        print(f"\n📊 결과: 성공 {success}/{len(schools)}개 (오늘 누적: {self.api_calls_today}/{self.api_limit})")
        return success
    
    def get_api_usage(self) -> Dict:
        """API 사용량 조회"""
        self._load_api_usage()
        return {
            'used_today': self.api_calls_today,
            'limit': self.api_limit,
            'remaining': self.api_limit - self.api_calls_today,
            'percent': f"{(self.api_calls_today/self.api_limit*100):.1f}%"
        }
    
    def get_stats(self) -> Dict:
        """통계 정보"""
        stats = {
            'geo_cache': {'total': 0, 'cached': len(self.cache)},
            'schools': {'total': 0, 'with_location': 0},
            'api_usage': self.get_api_usage()
        }
        
        # geo_cache 통계
        try:
            with get_db_connection(self.global_db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM geo_cache").fetchone()[0]
                stats['geo_cache']['total'] = total
        except:
            pass
        
        # 학교 통계
        if os.path.exists(self.school_db_path):
            with sqlite3.connect(self.school_db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
                with_loc = conn.execute("""
                    SELECT COUNT(*) FROM schools 
                    WHERE location_meta_ids IS NOT NULL
                """).fetchone()[0]
                
                stats['schools']['total'] = total
                stats['schools']['with_location'] = with_loc
                stats['schools']['coverage'] = f"{(with_loc/total*100):.1f}%" if total else "0%"
        
        return stats
    
    def close(self):
        """종료 처리"""
        self.flush()
        self.meta_vocab.close()
        if self.debug_mode:
            stats = self.get_stats()
            usage = stats['api_usage']
            print(f"📊 GeoCollector 종료: 캐시 {stats['geo_cache']['cached']}개")
            print(f"   API 사용량: {usage['used_today']}/{usage['limit']} ({usage['percent']})")


# ========================================================
# MAIN
# ========================================================
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="위치정보 수집기")
    parser.add_argument("--schools", action="store_true", help="학교 위치 일괄 변환")
    parser.add_argument("--limit", type=int, default=100, help="변환할 학교 수 (기본: 100)")
    parser.add_argument("--code", help="특정 학교 코드")
    parser.add_argument("--address", help="변환할 주소")
    parser.add_argument("--domain", default="test", help="도메인 (기본: test)")
    parser.add_argument("--item", help="항목 ID")
    parser.add_argument("--nearby", action="store_true", help="주변 학교 검색")
    parser.add_argument("--lat", type=float, help="위도")
    parser.add_argument("--lon", type=float, help="경도")
    parser.add_argument("--radius", type=float, default=5.0, help="검색 반경 (km)")
    parser.add_argument("--stats", action="store_true", help="통계 조회")
    parser.add_argument("--usage", action="store_true", help="API 사용량 조회")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    
    args = parser.parse_args()
    
    if not VWORLD_API_KEY:
        print("❌ VWORLD_API_KEY 환경변수가 없습니다.")
        return
    
    geo = GeoCollector(debug_mode=args.debug)
    
    try:
        if args.stats:
            # 통계 출력
            stats = geo.get_stats()
            print(f"\n📊 위치정보 통계:")
            print(f"  지오코딩 캐시: {stats['geo_cache']['total']}개")
            print(f"  전체 학교: {stats['schools']['total']}개")
            print(f"  위치정보 있음: {stats['schools']['with_location']}개")
            print(f"  적용률: {stats['schools'].get('coverage', '0%')}")
        
        elif args.usage:
            # API 사용량 조회
            usage = geo.get_api_usage()
            print(f"\n📊 VWorld API 사용량:")
            print(f"  오늘 사용: {usage['used_today']}회")
            print(f"  일일 한도: {usage['limit']}회")
            print(f"  잔여: {usage['remaining']}회")
            print(f"  사용률: {usage['percent']}")
        
        elif args.schools:
            # 학교 일괄 변환 (할당량 고려)
            geo.batch_update_schools(limit=args.limit)
        
        elif args.code and args.address:
            # 특정 학교 변환
            result = geo.save_location('school', args.code, args.address)
            if result.get('coords'):
                lon, lat = result['coords']
                print(f"\n✅ 변환 완료:")
                print(f"  학교: {args.code}")
                print(f"  주소: {args.address}")
                print(f"  좌표: ({lon:.6f}, {lat:.6f})")
                print(f"  meta_ids: {result['meta_ids']}")
            else:
                print("❌ 변환 실패")
        
        elif args.address:
            # 주소만 변환 (저장 없음)
            coords = geo._geocode(args.address)
            if coords:
                lon, lat = coords
                print(f"\n📍 변환 결과:")
                print(f"  주소: {args.address}")
                print(f"  좌표: ({lon:.6f}, {lat:.6f})")
            else:
                print("❌ 변환 실패")
        
        elif args.nearby and args.lat and args.lon:
            # 주변 학교 검색
            nearby = geo.find_nearby('school', args.lat, args.lon, args.radius)
            print(f"\n📍 반경 {args.radius}km 내 학교 {len(nearby)}개")
            for school in nearby:
                print(f"  {school['distance_km']}km: {school['name']}")
        
        else:
            parser.print_help()
    
    finally:
        geo.close()


if __name__ == "__main__":
    main()