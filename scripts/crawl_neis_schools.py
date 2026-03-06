#!/usr/bin/env python3
"""
NEIS 학교 정보 크롤러 - 샤드 DB 생성
- 진행 상황 그래프 출력
- odd/even 샤드로 분할 저장
- 병합 스크립트와 연동 가능
"""
import os
import sys
import sqlite3
import requests
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

# 교육청 코드
REGIONS = [
    "B10", "C10", "D10", "E10", "F10", "G10", "H10", "I10",
    "J10", "K10", "M10", "N10", "P10", "Q10", "R10", "S10", "T10"
]

# API 키 (환경 변수에서 가져오기 권장)
API_KEY = os.getenv("NEIS_API_KEY", "917818905d7b46e4b0eb71d2a15d9187")

def print_progress_bar(current, total, current_region="", schools_collected=0, bar_length=40):
    """진행 상황 그래프 출력 (덮어쓰기 방식)"""
    percent = float(current) / total if total > 0 else 0
    filled = int(bar_length * percent)
    bar = "█" * filled + "░" * (bar_length - filled)
    
    # 한 줄로 출력 (덮어쓰기)
    sys.stdout.write(f"\r🕷️  [{bar}] {current}/{total} | {current_region}")
    sys.stdout.write(f"\n   📊 수집: {schools_collected:,}개 학교")
    sys.stdout.flush()
    
    if current == total:
        print()  # 완료 시 줄바꿈

def create_shard_db(db_path: str):
    """샤드 DB 스키마 생성"""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schools (
            sc_code TEXT PRIMARY KEY,
            school_id TEXT,
            sc_name TEXT,
            eng_name TEXT,
            sc_kind TEXT,
            atpt_code TEXT,
            address TEXT,
            address_hash TEXT,
            tel TEXT,
            homepage TEXT,
            status TEXT,
            last_seen TEXT,
            load_dt TEXT,
            latitude REAL,
            longitude REAL,
            city_id TEXT,
            district_id TEXT,
            street_id TEXT,
            number_bit INTEGER
        )
    """)
    conn.commit()
    return conn

def fetch_schools(region_code: str) -> list:
    """NEIS API 에서 학교 정보 조회"""
    url = "https://open.neis.go.kr/hub/schoolInfo"
    schools = []
    page = 1
    page_size = 1000
    
    while True:
        params = {
            "KEY": API_KEY,
            "Type": "json",
            "ATPT_OFCDC_SC_CODE": region_code,
            "pIndex": page,
            "pSize": page_size
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'schoolInfo' not in data:
                break
                
            rows = data['schoolInfo'][1].get('row', [])
            if not rows:
                break
            
            for row in rows:
                schools.append({
                    'sc_code': row.get('SD_SCHUL_CODE', ''),
                    'school_id': row.get('SCHUL_ID', ''),
                    'sc_name': row.get('SCHUL_NM', ''),
                    'eng_name': row.get('ENG_SCHUL_NM', ''),
                    'sc_kind': row.get('SCHUL_KND_SC_NM', ''),
                    'atpt_code': row.get('ATPT_OFCDC_SC_CODE', ''),
                    'address': row.get('LCTN_ADDR', ''),
                    'address_hash': '',
                    'tel': row.get('TELNO', ''),
                    'homepage': row.get('HMPG_ADDR', ''),
                    'status': 'active',
                    'last_seen': datetime.now().isoformat(),
                    'load_dt': datetime.now().isoformat(),
                    'latitude': float(row.get('LAT', 0) or 0),
                    'longitude': float(row.get('LON', 0) or 0),
                    'city_id': '',
                    'district_id': '',
                    'street_id': '',
                    'number_bit': 0
                })
            
            if len(rows) < page_size:
                break
            
            page += 1
            time.sleep(0.3)
            
        except Exception as e:
            print(f"\n⚠️ {region_code} 페이지 {page} 오류: {e}")
            break
    
    return schools

def crawl_all_regions():
    """모든 교육청 크롤링 + 샤드 분할 저장"""
    os.makedirs(MASTER_DIR, exist_ok=True)
    
    # 샤드 DB 경로
    odd_db = os.path.join(MASTER_DIR, "neis_info_odd.db")
    even_db = os.path.join(MASTER_DIR, "neis_info_even.db")
    
    # 기존 샤드 삭제
    for db_path in [odd_db, even_db]:
        if os.path.exists(db_path):
            os.remove(db_path)
    
    # 샤드 DB 생성
    odd_conn = create_shard_db(odd_db)
    even_conn = create_shard_db(even_db)
    
    total_regions = len(REGIONS)
    total_schools = 0
    odd_count = 0
    even_count = 0
    
    print("=" * 70)
    print("🕷️  NEIS 학교 정보 크롤링 시작")
    print("=" * 70)
    print(f"📂 저장 경로: {MASTER_DIR}")
    print(f" 대상: {total_regions}개 교육청")
    print("-" * 70)
    
    for i, region in enumerate(REGIONS, 1):
        # 진행 상황 출력
        region_name = get_region_name(region)
        print(f"\n[{i}/{total_regions}] {region} ({region_name}) 조회 중...")
        
        # 학교 정보 수집
        schools = fetch_schools(region)
        
        # 샤드에 저장
        for school in schools:
            code_hash = hash(school['sc_code'])
            
            if code_hash % 2 == 0:
                even_conn.execute("""
                    INSERT OR REPLACE INTO schools VALUES 
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, tuple(school.values()))
                even_count += 1
            else:
                odd_conn.execute("""
                    INSERT OR REPLACE INTO schools VALUES 
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, tuple(school.values()))
                odd_count += 1
        
        total_schools += len(schools)
        
        # 진행 바 업데이트
        print_progress_bar(
            current=i,
            total=total_regions,
            current_region=f"{region} ({region_name})",
            schools_collected=total_schools,
            bar_length=50
        )
        
        time.sleep(0.5)
    
    # 커밋 및 닫기
    odd_conn.commit()
    even_conn.commit()
    odd_conn.close()
    even_conn.close()
    
    # 최종 결과
    print("\n" + "=" * 70)
    print("✅ 크롤링 완료")
    print("=" * 70)
    print(f"📊 총 학교 수: {total_schools:,}개")
    print(f"📦 odd 샤드:  {odd_count:,}개")
    print(f"📦 even 샤드: {even_count:,}개")
    print(f"💾 저장 위치: {MASTER_DIR}")
    print("\n🔜 다음 단계:")
    print("   python3 scripts/merge_neis_info_dbs.py")
    print("=" * 70)

def get_region_name(code: str) -> str:
    """교육청 코드 → 이름 변환"""
    region_map = {
        "B10": "서울", "C10": "부산", "D10": "인천", "E10": "광주",
        "F10": "대전", "G10": "울산", "H10": "세종", "I10": "경기",
        "J10": "강원", "K10": "충북", "L10": "충남", "M10": "전북",
        "N10": "전남", "O10": "경북", "P10": "경남", "Q10": "제주",
        "R10": "국가", "S10": "해외", "T10": "기타"
    }
    return region_map.get(code, "Unknown")

if __name__ == "__main__":
    crawl_all_regions()
    