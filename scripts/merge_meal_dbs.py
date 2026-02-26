#!/usr/bin/env python3
"""
최적화된 급식 DB 병합 스크립트
- ATTACH DATABASE 사용
- 인덱스 드랍/생성으로 성능 최적화
- 컬럼명 명시로 안정성 확보
- vocab 통합 기능 추가
"""
import os
import sqlite3
import glob
import shutil
import time
import argparse
from typing import List, Tuple

BASE_DIR = "data/active/meal"
GLOBAL_VOCAB_PATH = "data/active/global_vocab.db"


def get_indexes(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str]]:
    """테이블의 인덱스 정보 조회"""
    return conn.execute("""
        SELECT name, sql FROM sqlite_master 
        WHERE type='index' AND tbl_name=?
    """, (table,)).fetchall()


def drop_indexes(conn: sqlite3.Connection, indexes: List[Tuple[str, str]]):
    """인덱스 삭제"""
    for idx_name, _ in indexes:
        conn.execute(f"DROP INDEX IF EXISTS {idx_name}")


def recreate_indexes(conn: sqlite3.Connection, indexes: List[Tuple[str, str]]):
    """인덱스 재생성"""
    for _, idx_sql in indexes:
        if idx_sql:
            conn.execute(idx_sql)


def consolidate_vocab(shard_dbs: List[str]):
    """모든 샤드의 vocab을 통합"""
    
    conn_global = sqlite3.connect(GLOBAL_VOCAB_PATH)
    conn_global.execute("PRAGMA journal_mode=WAL")
    
    total_menus = 0
    total_meta = 0
    
    for shard_db in shard_dbs:
        conn_shard = sqlite3.connect(shard_db)
        
        # vocab_meal 통합
        try:
            menus = conn_shard.execute("""
                SELECT meal_id, name_key, name, display_name 
                FROM vocab_meal
            """).fetchall()
            
            for menu_id, name_key, name, display_name in menus:
                conn_global.execute("""
                    INSERT OR IGNORE INTO vocab_meal 
                    (meal_id, name_key, name, display_name)
                    VALUES (?, ?, ?, ?)
                """, (menu_id, name_key, name, display_name))
            
            total_menus += len(menus)
        except sqlite3.OperationalError:
            # 테이블이 없으면 스킵
            pass
        
        # meta_vocab 통합
        try:
            metas = conn_shard.execute("""
                SELECT meta_id, domain, meta_type, meta_key, meta_value, display_value
                FROM meta_vocab
            """).fetchall()
            
            for meta_id, domain, meta_type, meta_key, meta_value, display_value in metas:
                conn_global.execute("""
                    INSERT OR IGNORE INTO meta_vocab 
                    (meta_id, domain, meta_type, meta_key, meta_value, display_value)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (meta_id, domain, meta_type, meta_key, meta_value, display_value))
            
            total_meta += len(metas)
        except sqlite3.OperationalError:
            pass
        
        conn_shard.close()
    
    conn_global.commit()
    conn_global.close()
    
    print(f"✅ Vocab 통합 완료:")
    print(f"  - meal vocab: {total_menus}개")
    print(f"  - meta vocab: {total_meta}개")


def merge_databases(consolidate_vocab: bool = False):
    """최적화된 병합"""
    start_time = time.time()
    
    total_db_path = os.path.join(BASE_DIR, "meal_total.db")
    
    # 모든 샤드 DB 찾기
    shard_dbs = glob.glob(os.path.join(BASE_DIR, "meal_*.db"))
    shard_dbs = [db for db in shard_dbs if "total" not in db]
    
    print(f"🔍 발견된 샤드 DB: {len(shard_dbs)}개")
    
    if not shard_dbs:
        print("❌ 병합할 DB 없음")
        return
    
    # vocab 통합 (선택사항)
    if consolidate_vocab and shard_dbs:
        print("\n📚 Vocab 통합 중...")
        consolidate_vocab(shard_dbs)
    
    # 기존 total DB 백업
    if os.path.exists(total_db_path):
        backup_path = total_db_path.replace('.db', f"_backup_{time.strftime('%Y%m%d_%H%M%S')}.db")
        shutil.copy2(total_db_path, backup_path)
        print(f"💾 기존 DB 백업: {os.path.basename(backup_path)}")
        os.remove(total_db_path)
    
    # 새 total DB 생성
    conn_total = sqlite3.connect(total_db_path)
    
    # WAL 모드 활성화
    conn_total.execute("PRAGMA journal_mode=WAL")
    conn_total.execute("PRAGMA synchronous=NORMAL")
    
    # 스키마 생성
    conn_total.execute("""
        CREATE TABLE meal (
            school_id    INTEGER NOT NULL,
            meal_date    INTEGER NOT NULL,
            meal_type    INTEGER NOT NULL,
            menu_id      INTEGER NOT NULL,
            allergy_info TEXT,
            original_menu TEXT,
            cal_info     TEXT,
            ntr_info     TEXT,
            load_dt      TEXT,
            PRIMARY KEY (school_id, meal_date, meal_type, menu_id)
        ) WITHOUT ROWID
    """)
    
    conn_total.execute("""
        CREATE TABLE meal_meta (
            school_id  INTEGER NOT NULL,
            meal_date  INTEGER NOT NULL,
            meal_type  INTEGER NOT NULL,
            menu_id    INTEGER NOT NULL,
            meta_id    INTEGER NOT NULL,
            PRIMARY KEY (school_id, meal_date, meal_type, menu_id, meta_id)
        )
    """)
    
    conn_total.commit()
    
    # 인덱스 정보 백업
    meal_indexes = get_indexes(conn_total, 'meal')
    meta_indexes = get_indexes(conn_total, 'meal_meta')
    
    # 인덱스 삭제 (삽입 속도 향상)
    drop_indexes(conn_total, meal_indexes)
    drop_indexes(conn_total, meta_indexes)
    
    # 데이터 병합
    total_rows = 0
    total_meta = 0
    
    for shard_db in shard_dbs:
        print(f"📦 병합 중: {os.path.basename(shard_db)}")
        
        # ATTACH로 다른 DB 연결
        conn_total.execute(f"ATTACH DATABASE '{shard_db}' AS shard")
        
        # meal 테이블 병합 (컬럼명 명시!)
        cursor = conn_total.execute("""
            INSERT OR REPLACE INTO meal 
            (school_id, meal_date, meal_type, menu_id, 
             allergy_info, original_menu, cal_info, ntr_info, load_dt)
            SELECT school_id, meal_date, meal_type, menu_id,
                   allergy_info, original_menu, cal_info, ntr_info, load_dt
            FROM shard.meal
        """)
        total_rows += cursor.rowcount
        
        # meal_meta 테이블 병합
        cursor = conn_total.execute("""
            INSERT OR REPLACE INTO meal_meta 
            (school_id, meal_date, meal_type, menu_id, meta_id)
            SELECT school_id, meal_date, meal_type, menu_id, meta_id
            FROM shard.meal_meta
        """)
        total_meta += cursor.rowcount
        
        conn_total.execute("DETACH DATABASE shard")
        
        print(f"  ✅ meal: {cursor.rowcount}개 추가")
    
    # 인덱스 재생성
    print("🔨 인덱스 생성 중...")
    recreate_indexes(conn_total, meal_indexes)
    recreate_indexes(conn_total, meta_indexes)
    
    conn_total.commit()
    conn_total.close()
    
    elapsed = time.time() - start_time
    
    print(f"\n✨ 병합 완료!")
    print(f"  - meal 테이블: {total_rows:,}개")
    print(f"  - meal_meta 테이블: {total_meta:,}개")
    print(f"  - 소요 시간: {elapsed:.1f}초")
    print(f"  - 결과 파일: {total_db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consolidate-vocab", action="store_true", 
                       help="모든 샤드의 vocab을 통합")
    args = parser.parse_args()
    
    merge_databases(consolidate_vocab=args.consolidate_vocab)