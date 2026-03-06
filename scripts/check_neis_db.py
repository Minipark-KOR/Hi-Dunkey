#!/usr/bin/env python3
"""
NEIS 학교 DB 상태 확인 및 백업 (병합 소스가 없을 때)
"""
import os
import sys
import sqlite3
import time
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER_DIR = os.path.join(BASE_DIR, "data", "master")

def check_and_backup():
    total_db_path = os.path.join(MASTER_DIR, "neis_info.db")
    
    print("=" * 60)
    print("📊 NEIS 학교 DB 상태 진단")
    print("=" * 60)
    print(f"📂 경로: {MASTER_DIR}\n")
    
    # 1. 샤드 파일 확인
    print("🔍 1. 샤드 DB 파일 확인")
    shard_files = ["neis_info_odd.db", "neis_info_even.db"]
    shards_found = []
    for fname in shard_files:
        fpath = os.path.join(MASTER_DIR, fname)
        if os.path.exists(fpath):
            size_mb = os.path.getsize(fpath) / 1024 / 1024
            print(f"   ✅ {fname} ({size_mb:.1f} MB)")
            shards_found.append(fpath)
        else:
            print(f"   ❌ {fname} 없음")
    
    if not shards_found:
        print("   ⚠️  샤드 파일이 없습니다. (병합 불가)")
    print()
    
    # 2. 통합 DB 확인
    print("🔍 2. 통합 DB 확인")
    if os.path.exists(total_db_path):
        size_mb = os.path.getsize(total_db_path) / 1024 / 1024
        print(f"   ✅ neis_info.db 존재 ({size_mb:.1f} MB)")
        
        try:
            conn = sqlite3.connect(total_db_path)
            cursor = conn.cursor()
            
            # 테이블 확인
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            print(f"   📋 테이블: {', '.join(tables)}")
            
            # 레코드 수 확인
            if 'schools' in tables:
                count = cursor.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
                print(f"   📊 schools 레코드: {count:,}건")
                
                # 샘플 데이터
                sample = cursor.execute("SELECT sc_code, sc_name FROM schools LIMIT 3").fetchall()
                print(f"   📝 샘플: {sample}")
            conn.close()
            
        except Exception as e:
            print(f"   ❌ DB 오류: {e}")
            return
        
        # 3. 백업 수행
        print()
        print("🔍 3. 백업 수행")
        backup_dir = os.path.join(MASTER_DIR, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        
        backup_path = os.path.join(
            backup_dir, 
            f"neis_info_backup_{time.strftime('%Y%m%d_%H%M%S')}.db"
        )
        shutil.copy2(total_db_path, backup_path)
        print(f"   💾 백업 완료: backup/{os.path.basename(backup_path)}")
        
    else:
        print("   ❌ neis_info.db 없음")
        print("   ⚠️  샤드 파일 병합이 필요합니다.")
    
    print()
    print("=" * 60)
    print("✅ 진단 완료")
    print("=" * 60)

if __name__ == "__main__":
    check_and_backup()
    