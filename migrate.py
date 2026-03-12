#!/usr/bin/env python3
"""
통합 마이그레이션 스크립트 (대화형 메뉴 및 후속 작업 지원)

사용법:
    python migrate.py                     # 대화형 메뉴로 스키마 선택
    python migrate.py <schema_name>        # 해당 스키마의 모든 DB 파일 처리
    python migrate.py <schema_name> --db <db_path>   # 특정 DB 파일만 처리
"""
import sqlite3
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# ANSI 색상
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent))

from constants.paths import MASTER_DIR, LOG_DIR
from constants.schema import SCHEMAS

# 로그 설정
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOG_DIR / f"migrate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 마이그레이션 결과 저장용
migration_results: List[Tuple[str, bool, str]] = []  # (db_path, 성공여부, 메시지)

def print_header():
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}📦 통합 DB 마이그레이션{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

def select_schema() -> str:
    """사용 가능한 스키마 목록을 보여주고 선택받기"""
    schemas = list(SCHEMAS.keys())
    print(f"\n{YELLOW}마이그레이션할 스키마를 선택하세요:{RESET}")
    for i, name in enumerate(schemas, 1):
        print(f"  {i}) {name}")
    print()
    print("  0) 종료")

    while True:
        choice = input("선택: ").strip()
        if choice == "0":
            sys.exit(0)
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(schemas):
                return schemas[idx]
        print(f"{RED}잘못된 선택입니다.{RESET}")

def migrate_db(db_path: str, schema_name: str, table_name: str, columns: list, indexes: list) -> bool:
    """하나의 DB 파일에 대해 마이그레이션 실행"""
    if not os.path.exists(db_path):
        msg = f"❌ 파일 없음: {db_path}"
        logger.error(msg)
        migration_results.append((db_path, False, msg))
        return False

    logger.info(f"\n🔍 마이그레이션: {db_path}")
    logger.info("=" * 70)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")

            # 기존 컬럼 목록 조회
            cur = conn.execute(f"PRAGMA table_info({table_name})")
            existing = {row[1] for row in cur.fetchall()}
            logger.info(f"📋 기존 컬럼: {len(existing)}개")

            added = 0
            for col, typ, constraint in columns:
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

            # 인덱스 생성
            for idx_def in indexes:
                if len(idx_def) == 2:
                    idx_name, col_name = idx_def
                    condition = ""
                else:
                    idx_name, col_name, condition = idx_def
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({col_name}) {condition}".strip()
                try:
                    conn.execute(sql)
                    logger.info(f"  ✅ 인덱스: {idx_name}")
                except sqlite3.OperationalError as e:
                    logger.error(f"  ⚠️ 인덱스 실패: {idx_name} - {e}")

            logger.info("=" * 70)
            logger.info(f"📊 결과: 추가된 컬럼 {added}개")
            migration_results.append((db_path, True, f"추가된 컬럼 {added}개"))
            return True
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {e}", exc_info=True)
        migration_results.append((db_path, False, str(e)))
        return False

def show_summary():
    """마이그레이션 결과 요약 출력"""
    print(f"\n{BLUE}📊 마이그레이션 결과 요약{RESET}")
    print("=" * 60)
    success_count = sum(1 for r in migration_results if r[1])
    fail_count = len(migration_results) - success_count
    print(f"총 처리 파일: {len(migration_results)}개 (성공: {success_count}, 실패: {fail_count})")
    for db_path, success, msg in migration_results:
        status = f"{GREEN}성공{RESET}" if success else f"{RED}실패{RESET}"
        print(f"  • {db_path} : {status} - {msg}")
    print("=" * 60)

def view_log():
    """로그 파일 내용 보여주기 (최근 50줄)"""
    log_file = log_filename
    if log_file.exists():
        print(f"\n{BLUE}📄 로그 파일: {log_file}{RESET}")
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(''.join(lines[-50:]))
        except Exception as e:
            print(f"{RED}로그 읽기 실패: {e}{RESET}")
    else:
        print(f"{YELLOW}⚠️ 로그 파일이 없습니다.{RESET}")

def post_migration_menu():
    """마이그레이션 후 후속 작업 메뉴"""
    while True:
        print(f"\n{YELLOW}후속 작업을 선택하세요.{RESET}")
        print("  1) 결과 요약 보기")
        print("  2) 로그 확인 (마지막 50줄)")
        print("  3) 다른 스키마 마이그레이션 (처음으로)")
        print("  4) 종료")
        print()
        choice = input("선택: ").strip()

        if choice == "1":
            show_summary()
        elif choice == "2":
            view_log()
        elif choice == "3":
            return True  # 다른 스키마로 다시 시작
        elif choice == "4":
            print(f"{GREEN}👋 종료합니다.{RESET}")
            sys.exit(0)
        else:
            print(f"{RED}잘못된 선택입니다.{RESET}")

def main():
    parser = argparse.ArgumentParser(description="통합 DB 마이그레이션 (대화형 메뉴 및 후속 작업 지원)")
    parser.add_argument("schema", nargs="?", help="적용할 스키마 이름 (생략하면 대화형 메뉴)")
    parser.add_argument("--db", help="특정 DB 파일 경로 (지정하지 않으면 MASTER_DIR 내의 모든 해당 패턴 파일 처리)")
    args = parser.parse_args()

    # 스키마가 인자로 주어지지 않으면 대화형 메뉴 실행
    if args.schema is None:
        print_header()
        args.schema = select_schema()

    if args.schema not in SCHEMAS:
        print(f"{RED}❌ 알 수 없는 스키마: {args.schema}{RESET}")
        sys.exit(1)

    schema_info = SCHEMAS[args.schema]
    table_name = schema_info["table_name"]
    columns = schema_info["columns"]
    indexes = schema_info.get("indexes", [])

    # 마이그레이션 실행 전 결과 초기화
    global migration_results
    migration_results = []

    if args.db:
        # 단일 파일 처리
        migrate_db(args.db, args.schema, table_name, columns, indexes)
    else:
        # 패턴으로 모든 파일 처리 (예: school_info*.db)
        pattern = f"{args.schema}*.db"
        db_files = list(MASTER_DIR.glob(pattern))
        if not db_files:
            print(f"{RED}❌ {MASTER_DIR} 내에 '{pattern}' 패턴의 파일이 없습니다.{RESET}")
            sys.exit(1)
        for db_path in db_files:
            migrate_db(str(db_path), args.schema, table_name, columns, indexes)

    # 마이그레이션 완료 후 후속 메뉴
    print(f"\n{GREEN}✅ 마이그레이션 완료!{RESET}")
    post_migration_menu()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}👋 사용자 종료{RESET}")
        sys.exit(0)
        