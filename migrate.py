#!/usr/bin/env python3
# migrate.py
"""
통합 마이그레이션 스크립트 (전체 일괄 처리 지원)

사용법:
    python migrate.py                     # 대화형 메뉴로 스키마 선택
    python migrate.py <schema_name>        # 해당 스키마의 모든 DB 파일 처리
    python migrate.py --all                # 모든 스키마 일괄 처리 (✅ 신규)
    python migrate.py --all --recreate     # 모든 스키마 테이블 재생성 (PRIMARY KEY 변경 등)
    python migrate.py <schema_name> --db <db_path>   # 특정 DB 파일만 처리
    python migrate.py <schema_name> --recreate        # 단일 스키마 테이블 재생성
"""
import sqlite3
import sys
import os
import argparse
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Any
from collections import defaultdict

# ANSI 색상
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent))

from constants.paths import MASTER_DIR
from constants.schema import SCHEMAS
from core.util.manage_log import build_domain_logger

# 로그 설정: data/logs/migrate.log
logger = build_domain_logger("migrate")

# 마이그레이션 결과 저장용: (스키마, db_path, 성공여부, 메시지)
migration_results: List[Tuple[str, str, bool, str]] = []

def print_header():
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📦 통합 DB 마이그레이션 (전체 일괄 처리 지원){RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

def select_schema() -> str:
    """사용 가능한 스키마 목록을 보여주고 선택받기"""
    schemas = list(SCHEMAS.keys())
    print(f"\n{YELLOW}마이그레이션할 스키마를 선택하세요:{RESET}")
    for i, name in enumerate(schemas, 1):
        table_name = SCHEMAS[name].get("table_name", "unknown")
        print(f"  {i}) {name} (테이블: {table_name})")
    print("\n  0) 종료")

    while True:
        choice = input("선택: ").strip()
        if choice == "0":
            sys.exit(0)
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(schemas):
                return schemas[idx]
        print(f"{RED}잘못된 선택입니다.{RESET}")

def backup_db(db_path: str) -> str:
    """마이그레이션 전 백업 생성"""
    if not os.path.exists(db_path):
        return None

    backup_path = db_path.replace('.db', f'_pre_migrate_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
    try:
        shutil.copy2(db_path, backup_path)
        logger.info(f"📦 백업 생성: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"백업 실패: {e}")
        return None

def recreate_table(conn: sqlite3.Connection, schema_name: str, schema: Dict) -> bool:
    """테이블 재생성 (데이터 복사 포함) - PRIMARY KEY 변경 등 ALTER로 불가능한 경우 사용"""
    old_table = schema["table_name"]
    temp_table = f"{old_table}_new"

    try:
        # 1. 새 테이블 생성 (primary_key 정보를 컬럼 정의에 반영)
        columns = schema["columns"]
        primary_key = schema.get("primary_key", [])
        indexes = schema.get("indexes", [])

        col_defs = []
        for col, typ, constraint in columns:
            col = col.strip()
            typ = typ.strip()
            constraint = constraint.strip() if constraint else ""
            col_def = f"{col} {typ}"
            if constraint:
                col_def += f" {constraint}"
            if col in primary_key:
                col_def += " PRIMARY KEY"
            col_defs.append(col_def)

        create_sql = f"CREATE TABLE {temp_table} ({', '.join(col_defs)})"
        logger.info(f"🆕 새 테이블 생성: {create_sql[:100]}...")
        conn.execute(create_sql)

        # 2. 기존 테이블에서 데이터 복사 (공통 컬럼만)
        cur = conn.execute(f"PRAGMA table_info({old_table})")
        existing_cols = {row[1].strip() for row in cur.fetchall()}
        new_cols = [col[0].strip() for col in columns]
        common_cols = [c for c in new_cols if c in existing_cols]

        if common_cols:
            col_list = ', '.join(common_cols)
            insert_sql = f"INSERT INTO {temp_table} ({col_list}) SELECT {col_list} FROM {old_table}"
            logger.info(f"📋 데이터 복사: {len(common_cols)}개 컬럼")
            conn.execute(insert_sql)
            copied = conn.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
            logger.info(f"✅ {copied}행 복사됨")
        else:
            logger.warning("⚠️ 공통 컬럼이 없어 데이터를 복사하지 않습니다.")

        # 3. 인덱스 생성
        for idx_def in indexes:
            if len(idx_def) == 2:
                idx_name, col = idx_def
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name.strip()} ON {temp_table}({col.strip()})"
            else:
                idx_name, col, condition = idx_def
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name.strip()} ON {temp_table}({col.strip()}) {condition.strip()}"
            conn.execute(sql)
            logger.info(f"  ✅ 인덱스: {idx_name.strip()}")

        # 4. 舊 테이블 삭제, 새 테이블 리네임
        conn.execute(f"DROP TABLE {old_table}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {old_table}")

        logger.info(f"✅ 테이블 재생성 완료: {old_table}")
        return True

    except Exception as e:
        logger.error(f"❌ 테이블 재생성 실패: {e}", exc_info=True)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        return False

def migrate_db(db_path: str, schema_name: str, schema: Dict, recreate_tables: bool = False) -> bool:
    """하나의 DB 파일에 대해 마이그레이션 실행"""
    if not os.path.exists(db_path):
        msg = f"❌ 파일 없음: {db_path}"
        logger.error(msg)
        migration_results.append((schema_name, db_path, False, msg))
        return False

    logger.info(f"\n🔍 마이그레이션: {db_path} (스키마: {schema_name})")
    logger.info("=" * 70)

    backup_path = backup_db(db_path)  # 자동 백업 생성

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")

            table_name = schema["table_name"]

            if recreate_tables:
                logger.info("🔄 테이블 재생성 모드 실행")
                success = recreate_table(conn, schema_name, schema)
                if success:
                    conn.commit()
                    logger.info("✅ 커밋 완료")
                    migration_results.append((schema_name, db_path, True, "테이블 재생성 완료"))
                else:
                    conn.rollback()
                    logger.info("🔄 롤백 완료")
                    migration_results.append((schema_name, db_path, False, "테이블 재생성 실패"))
                return success
            else:
                # 일반 모드: 누락된 컬럼 추가, 인덱스 생성
                cur = conn.execute(f"PRAGMA table_info({table_name})")
                existing = {row[1].strip() for row in cur.fetchall()}
                logger.info(f"📋 기존 컬럼: {len(existing)}개")

                added = 0
                for col, typ, constraint in schema["columns"]:
                    col = col.strip()
                    if col not in existing:
                        sql = f"ALTER TABLE {table_name} ADD COLUMN {col} {typ}"
                        if constraint:
                            sql += f" {constraint}"
                        try:
                            conn.execute(sql)
                            logger.info(f"  ✅ 추가: {col}")
                            added += 1
                        except sqlite3.OperationalError as e:
                            logger.error(f"  ⚠️ 실패: {col} - {e}")
                    else:
                        logger.info(f"  ⏭️ 존재: {col}")

                for idx_def in schema.get("indexes", []):
                    if len(idx_def) == 2:
                        idx_name, col = idx_def
                        sql = f"CREATE INDEX IF NOT EXISTS {idx_name.strip()} ON {table_name}({col.strip()})"
                    else:
                        idx_name, col, condition = idx_def
                        sql = f"CREATE INDEX IF NOT EXISTS {idx_name.strip()} ON {table_name}({col.strip()}) {condition.strip()}"
                    try:
                        conn.execute(sql)
                        logger.info(f"  ✅ 인덱스: {idx_name.strip()}")
                    except Exception as e:
                        logger.error(f"  ⚠️ 인덱스 실패: {idx_name} - {e}")

                conn.commit()
                logger.info(f"📊 결과: 추가된 컬럼 {added}개")
                migration_results.append((schema_name, db_path, True, f"컬럼 {added}개 추가"))
                return True

    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {e}", exc_info=True)
        migration_results.append((schema_name, db_path, False, str(e)))
        return False

def show_summary():
    """마이그레이션 결과 요약 출력"""
    print(f"\n{BLUE}📊 마이그레이션 결과 요약{RESET}")
    print("=" * 80)

    # 스키마별 그룹화
    by_schema = defaultdict(list)
    for schema_name, db_path, success, msg in migration_results:
        by_schema[schema_name].append((db_path, success, msg))

    total_success = sum(1 for r in migration_results if r[2])
    total_fail = len(migration_results) - total_success

    print(f"총 처리 파일: {len(migration_results)}개 (성공: {total_success}, 실패: {total_fail})\n")

    for schema_name, results in by_schema.items():
        schema_success = sum(1 for r in results if r[1])
        print(f"{CYAN}📁 [{schema_name}]{RESET} - 성공: {schema_success}/{len(results)}")
        for db_path, success, msg in results:
            status = f"{GREEN}성공{RESET}" if success else f"{RED}실패{RESET}"
            print(f"   • {os.path.basename(db_path)} : {status} - {msg}")
        print()

    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(description="통합 DB 마이그레이션 (전체 일괄 처리 지원)")
    parser.add_argument("schema", nargs="?", help="적용할 스키마 이름 (생략하면 대화형 메뉴)")
    parser.add_argument("--all", action="store_true", help="모든 스키마 일괄 처리")
    parser.add_argument("--db", help="특정 DB 파일 경로 (지정하지 않으면 MASTER_DIR 내의 모든 해당 패턴 파일 처리)")
    parser.add_argument("--recreate", action="store_true", help="테이블 재생성 모드 (PRIMARY KEY 변경 등 ALTER로 불가능한 경우)")
    parser.add_argument("--no-backup", action="store_true", help="백업 없이 실행 (위험)")
    args = parser.parse_args()

    global migration_results
    migration_results = []

    # 모든 스키마 일괄 처리 모드
    if args.all:
        print_header()
        print(f"\n{YELLOW}⚠️  경고: 모든 스키마를 일괄 처리합니다.{RESET}")
        print(f"   - 처리할 스키마: {len(SCHEMAS)}개 ({', '.join(SCHEMAS.keys())})")
        if not args.no_backup:
            confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
            if confirm != 'y':
                print("취소되었습니다.")
                sys.exit(0)

        for schema_name, schema in SCHEMAS.items():
            pattern = f"{schema_name}*.db"
            db_files = list(MASTER_DIR.glob(pattern))
            if not db_files:
                logger.warning(f"⚠️ {schema_name}: 해당 DB 파일 없음")
                continue

            print(f"\n{CYAN}📁 [{schema_name}] 처리 시작 ({len(db_files)}개 파일){RESET}")
            for db_path in db_files:
                migrate_db(str(db_path), schema_name, schema, args.recreate)

    # 단일 스키마 처리 모드
    else:
        if args.schema is None:
            print_header()
            args.schema = select_schema()

        if args.schema not in SCHEMAS:
            print(f"{RED}❌ 알 수 없는 스키마: {args.schema}{RESET}")
            sys.exit(1)

        schema = SCHEMAS[args.schema]

        if args.recreate and not args.no_backup:
            print(f"\n{YELLOW}⚠️  경고: 테이블 재생성 모드는 데이터를 복사합니다.{RESET}")
            confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
            if confirm != 'y':
                print("취소되었습니다.")
                sys.exit(0)

        if args.db:
            # 단일 파일 처리
            if not args.no_backup:
                backup_db(args.db)
            migrate_db(args.db, args.schema, schema, args.recreate)
        else:
            # 해당 스키마의 모든 DB 파일 처리
            pattern = f"{args.schema}*.db"
            db_files = list(MASTER_DIR.glob(pattern))
            if not db_files:
                print(f"{RED}❌ {MASTER_DIR} 내에 '{pattern}' 패턴의 파일이 없습니다.{RESET}")
                sys.exit(1)

            print(f"\n{YELLOW}처리할 파일: {len(db_files)}개{RESET}")
            for db_path in db_files:
                migrate_db(str(db_path), args.schema, schema, args.recreate)

    print(f"\n{GREEN}✅ 마이그레이션 완료!{RESET}")
    show_summary()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}👋 사용자 종료{RESET}")
        sys.exit(0)
