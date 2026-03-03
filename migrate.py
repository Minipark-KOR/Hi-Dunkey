#!/usr/bin/env python3
"""
DB 스키마 마이그레이션 (상호작용형)
- 실행 결과 상세 출력
- 사후 메뉴로 추가 작업 선택 가능
"""
import sqlite3
import sys
import os

def migrate(db_path: str, verbose: bool = True) -> dict:
    """
    DB 마이그레이션 실행
    
    Returns:
        dict: 마이그레이션 결과 통계
    """
    result = {
        'success': False,
        'added': 0,
        'skipped': 0,
        'total': 0,
        'indexes': 0,
        'errors': []
    }
    
    if not os.path.exists(db_path):
        print(f"❌ DB 파일 없음: {db_path}")
        result['errors'].append("DB 파일이 존재하지 않음")
        return result
    
    print(f"\n🔍 마이그레이션 시작: {db_path}")
    print("=" * 70)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            
            # 기존 컬럼 목록 조회
            cur = conn.execute("PRAGMA table_info(schools)")
            existing = {row[1] for row in cur.fetchall()}
            print(f"📋 기존 컬럼: {len(existing)}개")
            
            # 추가할 새 컬럼 정의
            new_columns = [
                ("cleaned_address", "TEXT"),
                ("geocode_attempts", "INTEGER DEFAULT 0"),
                ("last_error", "TEXT"),
                ("city_id", "INTEGER"),
                ("district_id", "INTEGER"),
                ("street_id", "INTEGER"),
                ("number_type", "TEXT"),
                ("number_value", "INTEGER"),
                ("number_start", "INTEGER"),
                ("number_end", "INTEGER"),
                ("number_bit", "INTEGER"),
            ]
            result['total'] = len(new_columns)
            
            added = 0
            skipped = 0
            
            for col, typ in new_columns:
                if col not in existing:
                    try:
                        conn.execute(f"ALTER TABLE schools ADD COLUMN {col} {typ}")
                        if verbose:
                            print(f"  ✅ 추가: {col} ({typ})")
                        added += 1
                    except sqlite3.OperationalError as e:
                        msg = f"실패: {col} - {e}"
                        if verbose:
                            print(f"  ⚠️  {msg}")
                        result['errors'].append(msg)
                else:
                    if verbose:
                        print(f"  ⏭️  존재: {col}")
                    skipped += 1
            
            result['added'] = added
            result['skipped'] = skipped
            
            # geocode_attempts 초기값 설정
            conn.execute("UPDATE schools SET geocode_attempts = 0 WHERE geocode_attempts IS NULL")
            
            # 인덱스 생성
            indexes = [
                ("idx_schools_missing", "ON schools(latitude) WHERE latitude IS NULL"),
                ("idx_schools_attempts", "ON schools(geocode_attempts) WHERE latitude IS NULL"),
            ]
            
            for idx_name, idx_def in indexes:
                try:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} {idx_def}")
                    if verbose:
                        print(f"  ✅ 인덱스: {idx_name}")
                    result['indexes'] += 1
                except sqlite3.OperationalError as e:
                    msg = f"인덱스 실패: {idx_name} - {e}"
                    if verbose:
                        print(f"  ⚠️  {msg}")
                    result['errors'].append(msg)
            
            print("=" * 70)
            print(f"📊 결과: 추가 {added}개, 기존 {skipped}개, 총 {len(new_columns)}개")
            result['success'] = True
            
    except Exception as e:
        print(f"❌ 마이그레이션 오류: {e}")
        result['errors'].append(str(e))
    
    return result


def show_menu(db_path: str):
    """사후 메뉴 표시"""
    while True:
        print("\n" + "=" * 70)
        print("📋 추가 작업 메뉴")
        print("=" * 70)
        print("  1. DB 스키마 확인 (컬럼 목록)")
        print("  2. 인덱스 목록 확인")
        print("  3. 학교 데이터 개수 확인")
        print("  4. 지오코딩 누락 학교 확인")
        print("  5. DB 파일 크기 확인")
        print("  6. 마이그레이션 재실행")
        print("  0. 종료")
        print("=" * 70)
        
        choice = input("번호를 선택하세요 (0-6): ").strip()
        
        if choice == '1':
            check_schema(db_path)
        elif choice == '2':
            check_indexes(db_path)
        elif choice == '3':
            check_school_count(db_path)
        elif choice == '4':
            check_missing_geocode(db_path)
        elif choice == '5':
            check_db_size(db_path)
        elif choice == '6':
            migrate(db_path, verbose=True)
        elif choice == '0':
            print("👋 종료합니다.")
            break
        else:
            print("❌ 잘못된 입력입니다. 다시 선택하세요.")


def check_schema(db_path: str):
    """DB 스키마 확인"""
    if not os.path.exists(db_path):
        print("❌ DB 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("PRAGMA table_info(schools)")
        columns = cur.fetchall()
        print(f"\n📋 schools 테이블 컬럼 ({len(columns)}개):")
        print("-" * 70)
        for col in columns:
            cid, name, typ, notnull, default, pk = col
            pk_mark = "🔑" if pk else "  "
            print(f"  {pk_mark} {cid:2d}. {name:<25} {typ:<15}")


def check_indexes(db_path: str):
    """인덱스 목록 확인"""
    if not os.path.exists(db_path):
        print("❌ DB 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='schools'")
        indexes = cur.fetchall()
        print(f"\n📋 schools 테이블 인덱스 ({len(indexes)}개):")
        print("-" * 70)
        for idx_name, idx_sql in indexes:
            if idx_name and not idx_name.startswith('sqlite_'):
                print(f"  • {idx_name}")


def check_school_count(db_path: str):
    """학교 데이터 개수 확인"""
    if not os.path.exists(db_path):
        print("❌ DB 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM schools")
        total = cur.fetchone()[0]
        
        cur = conn.execute("SELECT status, COUNT(*) FROM schools GROUP BY status")
        by_status = cur.fetchall()
        
        print(f"\n📊 학교 데이터 통계:")
        print("-" * 70)
        print(f"  총 계: {total:,}개")
        for status, count in by_status:
            print(f"  - {status}: {count:,}개")


def check_missing_geocode(db_path: str):
    """지오코딩 누락 학교 확인"""
    if not os.path.exists(db_path):
        print("❌ DB 파일이 없습니다.")
        return
    
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("""
            SELECT COUNT(*) FROM schools 
            WHERE (latitude IS NULL OR longitude IS NULL) 
            AND address IS NOT NULL AND address != ''
        """)
        missing = cur.fetchone()[0]
        
        print(f"\n📍 지오코딩 누락 학교:")
        print("-" * 70)
        print(f"  좌표 없음: {missing:,}개")
        
        if missing > 0 and missing <= 10:
            print("\n  상세 목록 (최대 10 개):")
            cur = conn.execute("""
                SELECT sc_code, sc_name, address 
                FROM schools 
                WHERE (latitude IS NULL OR longitude IS NULL) 
                AND address IS NOT NULL AND address != ''
                LIMIT 10
            """)
            for sc_code, sc_name, address in cur:
                print(f"    - {sc_code}: {sc_name} ({address[:40]}...)")


def check_db_size(db_path: str):
    """DB 파일 크기 확인"""
    if not os.path.exists(db_path):
        print("❌ DB 파일이 없습니다.")
        return
    
    size = os.path.getsize(db_path)
    size_mb = size / (1024 * 1024)
    
    # WAL 파일 크기 포함
    wal_path = db_path + "-wal"
    shm_path = db_path + "-shm"
    
    print(f"\n💾 DB 파일 크기:")
    print("-" * 70)
    print(f"  main: {size_mb:.2f} MB")
    
    if os.path.exists(wal_path):
        wal_size = os.path.getsize(wal_path) / (1024 * 1024)
        print(f"  wal:  {wal_size:.2f} MB")
    
    if os.path.exists(shm_path):
        shm_size = os.path.getsize(shm_path) / (1024 * 1024)
        print(f"  shm:  {shm_size:.2f} MB")
    
    total = size_mb
    if os.path.exists(wal_path):
        total += os.path.getsize(wal_path) / (1024 * 1024)
    if os.path.exists(shm_path):
        total += os.path.getsize(shm_path) / (1024 * 1024)
    
    print(f"  ----------------")
    print(f"  총계: {total:.2f} MB")


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="DB 스키마 마이그레이션")
    parser.add_argument("db_path", nargs="?", default="data/master/school_info.db", 
                        help="school_info.db 경로 (기본: data/master/school_info.db)")
    parser.add_argument("-q", "--quiet", action="store_true", help="간략 출력")
    parser.add_argument("-m", "--menu", action="store_true", help="실행 후 메뉴 표시")
    args = parser.parse_args()
    
    # 마이그레이션 실행
    result = migrate(args.db_path, verbose=not args.quiet)
    
    # 결과에 따라 종료 또는 메뉴 표시
    if args.menu or not result['success']:
        show_menu(args.db_path)
    
    # 종료 코드 설정
    sys.exit(0 if result['success'] else 1)


if __name__ == "__main__":
    main()
    