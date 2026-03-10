#!/usr/bin/env python3
"""
급식 DB 병합 스크립트
"""
import os
import sys
import sqlite3
import glob
import time
import shutil
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from constants.paths import ACTIVE_DIR, GLOBAL_VOCAB_DB_PATH

MEAL_DIR = os.path.join(ACTIVE_DIR, "meal")  # meal 하위 디렉토리?

def consolidate_vocab(shard_dbs: List[str]):
    conn_global = sqlite3.connect(GLOBAL_VOCAB_DB_PATH)
    conn_global.execute("PRAGMA journal_mode=WAL")
    conn_global.execute("""
        CREATE TABLE IF NOT EXISTS vocab_meal (
            meal_id      INTEGER PRIMARY KEY,
            name_key     TEXT UNIQUE,
            name         TEXT,
            display_name TEXT
        )
    """)
    conn_global.execute("""
        CREATE TABLE IF NOT EXISTS meta_vocab (
            meta_id       INTEGER PRIMARY KEY,
            domain        TEXT,
            meta_type     TEXT,
            meta_key      TEXT,
            meta_value    TEXT,
            display_value TEXT,
            UNIQUE(domain, meta_type, meta_key)
        )
    """)
    total_menus = total_meta = 0
    for shard_db in shard_dbs:
        conn_shard = sqlite3.connect(shard_db)
        try:
            menus = conn_shard.execute(
                "SELECT meal_id, name_key, name, display_name FROM vocab_meal"
            ).fetchall()
            for row in menus:
                conn_global.execute(
                    "INSERT OR IGNORE INTO vocab_meal VALUES (?,?,?,?)", row
                )
            total_menus += len(menus)
            metas = conn_shard.execute(
                "SELECT meta_id, domain, meta_type, meta_key, meta_value, display_value FROM meta_vocab"
            ).fetchall()
            for row in metas:
                conn_global.execute(
                    "INSERT OR IGNORE INTO meta_vocab VALUES (?,?,?,?,?,?)", row
                )
            total_meta += len(metas)
        except sqlite3.OperationalError:
            pass
        finally:
            conn_shard.close()
    conn_global.commit()
    conn_global.close()
    print(f"✅ Vocab 통합 완료: meal {total_menus}건, meta {total_meta}건")

def merge_databases(do_consolidate_vocab: bool = False):
    start_time = time.time()
    total_db_path = os.path.join(ACTIVE_DIR, "meal.db")
    # meal 하위 디렉토리에 샤드 파일이 있다면 경로 수정
    shard_dbs = [db for db in glob.glob(os.path.join(ACTIVE_DIR, "meal_*.db")) if "total" not in db]
    print(f"🔍 발견된 샤드 DB: {len(shard_dbs)}개")
    if not shard_dbs:
        print("❌ 병합할 샤드 데이터 없음")
        return
    if do_consolidate_vocab:
        print("\n📚 Vocab 통합 중...")
        consolidate_vocab(shard_dbs)
    if os.path.exists(total_db_path):
        backup_path = total_db_path.replace(".db", f"_backup_{time.strftime('%Y%m%d_%H%M%S')}.db")
        shutil.copy2(total_db_path, backup_path)
        print(f"💾 기존 DB 백업: {os.path.basename(backup_path)}")
        os.remove(total_db_path)
    conn = sqlite3.connect(total_db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE meal (
            school_id     INTEGER NOT NULL,
            meal_date     INTEGER NOT NULL,
            meal_type     INTEGER NOT NULL,
            menu_id       INTEGER NOT NULL,
            allergy_info  TEXT,
            original_menu TEXT,
            cal_info      TEXT,
            ntr_info      TEXT,
            load_dt       TEXT,
            PRIMARY KEY (school_id, meal_date, meal_type, menu_id)
        ) WITHOUT ROWID
    """)
    conn.execute("""
        CREATE TABLE meal_meta (
            school_id INTEGER NOT NULL,
            meal_date INTEGER NOT NULL,
            meal_type INTEGER NOT NULL,
            menu_id   INTEGER NOT NULL,
            meta_id   INTEGER NOT NULL,
            PRIMARY KEY (school_id, meal_date, meal_type, menu_id, meta_id)
        )
    """)
    conn.commit()
    total_rows = total_meta = 0
    for shard_db in shard_dbs:
        print(f"📦 병합 중: {os.path.basename(shard_db)}")
        conn.execute(f"ATTACH DATABASE '{shard_db}' AS shard")
        c1 = conn.execute("INSERT OR REPLACE INTO meal SELECT * FROM shard.meal")
        total_rows += c1.rowcount
        c2 = conn.execute("INSERT OR REPLACE INTO meal_meta SELECT * FROM shard.meal_meta")
        total_meta += c2.rowcount
        conn.execute("DETACH DATABASE shard")
        print(f"  ✅ meal: {c1.rowcount}건, meal_meta: {c2.rowcount}건")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_date ON meal(meal_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_school ON meal(school_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_meal_meta ON meal_meta(meta_id)")
    conn.commit()
    conn.execute("PRAGMA optimize")
    conn.close()
    elapsed = time.time() - start_time
    size = os.path.getsize(total_db_path) / 1024 / 1024
    print(f"\n✅ 병합 완료: meal {total_rows:,}건 | {size:.1f} MB | {elapsed:.1f}초")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--consolidate-vocab", action="store_true")
    args = parser.parse_args()
    merge_databases(do_consolidate_vocab=args.consolidate_vocab)
    