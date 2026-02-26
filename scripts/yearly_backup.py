#!/usr/bin/env python3
"""
학년도 전체 백업 스크립트 (자동 복구 포함)
- 급식(meal)과 동일한 패턴
- 학년도 마지막에 전체 수집 후 백업
- 3단계 검증 + 자동 복구
- 상세 로깅
"""
import os
import sys
import sqlite3
import shutil
import time
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kst_time import now_kst
from core.backup import vacuum_into

# Collector들 import
from collectors.school_master_collector import SchoolMasterCollector
from collectors.meal_collector import MealCollector
from collectors.timetable_collector import TimetableCollector
from collectors.schedule_collector import ScheduleCollector


class YearlyBackup:
    """
    학년도 전체 백업 + 자동 복구
    
    사용 예시:
        python yearly_backup.py --year 2026 --full
        python yearly_backup.py --year 2026 --verify
        python yearly_backup.py --list
        python yearly_backup.py --year 2026 --auto-repair
    """
    
    # Collector 정의
    COLLECTORS = {
        "school": {
            "class": SchoolMasterCollector,
            "description": "학교 기본정보",
            "db_file": "school_master.db",
            "backup_file": "school_master_{year}1231.db",
            "table": "schools",
            "enabled": True
        },
        "meal": {
            "class": MealCollector,
            "description": "급식 정보",
            "db_file": "meal_total.db",
            "backup_file": "meal_total_{year}1231.db",
            "table": "meal",
            "enabled": True
        },
        "timetable": {
            "class": TimetableCollector,
            "description": "시간표 정보",
            "db_file": "timetable_total.db",
            "backup_file": "timetable_{year}1231.db",
            "table": "timetable",
            "enabled": True
        },
        "schedule": {
            "class": ScheduleCollector,
            "description": "학사일정 정보",
            "db_file": "schedule_total.db",
            "backup_file": "schedule_{year}1231.db",
            "table": "schedule",
            "enabled": True
        }
    }
    
    # 글로벌 DB
    GLOBAL_DBS = [
        {"name": "global_vocab.db", "table": "meta_vocab"},
        {"name": "unknown_patterns.db", "table": "unknown_patterns"}
    ]
    
    def __init__(self, base_dir: str = "data", backup_dir: str = "data/backup", 
                 log_dir: str = "logs"):
        """
        Args:
            base_dir: 데이터 기본 디렉토리
            backup_dir: 백업 디렉토리
            log_dir: 로그 디렉토리
        """
        self.base_dir = base_dir
        self.backup_dir = backup_dir
        self.log_dir = log_dir
        
        # 디렉토리 생성
        for d in [base_dir, backup_dir, log_dir]:
            os.makedirs(d, exist_ok=True)
        
        # 로거 설정
        self._setup_logger()
        
        # 최대 재시도 횟수
        self.max_retries = 3
    
    def _setup_logger(self):
        """로거 설정"""
        self.logger = logging.getLogger("yearly_backup")
        self.logger.setLevel(logging.INFO)
        
        # 파일 핸들러
        fh = logging.FileHandler(f"{self.log_dir}/yearly_backup.log", encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # 콘솔 핸들러
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 포맷터
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    # ========================================================
    # 1. 백업 실행
    # ========================================================
    
    def run_full_backup(self, year: int, collectors: List[str] = None) -> bool:
        """
        학년도 전체 백업 수행 (자동 복구 포함)
        
        Args:
            year: 학년도
            collectors: 백업할 Collector 목록
        
        Returns:
            성공 여부
        """
        self.logger.info(f"📅 학년도 {year} 전체 백업 시작 (최대 {self.max_retries}회 시도)")
        
        for attempt in range(self.max_retries):
            self.logger.info(f"🔄 시도 {attempt + 1}/{self.max_retries}")
            
            try:
                # 1. Collector 실행
                if not self._run_collectors(year, collectors):
                    raise Exception("Collector 실행 실패")
                
                # 2. 통합 DB 생성
                self._create_total_dbs(year)
                
                # 3. 백업 생성
                backup_dir = self._create_backup_dir(year)
                self._backup_databases(year, backup_dir)
                
                # 4. 검증
                issues = self.verify_backup(year)
                
                if not issues:
                    self.logger.info(f"✅ 학년도 {year} 백업 성공 (attempt {attempt + 1})")
                    
                    # 성공 로그 저장
                    self._save_success_log(year)
                    return True
                
                # 문제 발견! 자동 복구
                self.logger.warning(f"⚠️ {len(issues)}개 문제 발견, 자동 복구 시작...")
                self._auto_repair(year, issues)
                
                # 다음 시도를 위해 대기
                if attempt < self.max_retries - 1:
                    self.logger.info("⏳ 10초 후 재시도...")
                    time.sleep(10)
                
            except Exception as e:
                self.logger.error(f"❌ 백업 실패: {e}")
                
                if attempt < self.max_retries - 1:
                    self.logger.info("⏳ 10초 후 재시도...")
                    time.sleep(10)
        
        self.logger.critical(f"❌ {self.max_retries}회 시도 실패 - 수동 개입 필요")
        return False
    
    def _run_collectors(self, year: int, collectors: List[str] = None) -> bool:
        """Collector 실행"""
        if collectors:
            target_collectors = [c for c in collectors if c in self.COLLECTORS]
        else:
            target_collectors = [name for name, info in self.COLLECTORS.items() 
                                if info["enabled"]]
        
        for name in target_collectors:
            self.logger.info(f"🚀 {name} 수집 시작")
            
            try:
                if name == "school":
                    collector = self.COLLECTORS[name]["class"](shard="none", full=True)
                elif name == "meal":
                    collector = self.COLLECTORS[name]["class"](shard="none", full=True)
                elif name == "timetable":
                    collector = self.COLLECTORS[name]["class"](shard="none", full=True)
                elif name == "schedule":
                    collector = self.COLLECTORS[name]["class"](shard="none", full=True)
                
                # 전체 지역 수집
                from constants.codes import ALL_REGIONS
                for region in ALL_REGIONS:
                    if name == "timetable":
                        collector.fetch_region(region, year, 1)
                        collector.fetch_region(region, year, 2)
                    elif name == "schedule":
                        collector.fetch_region(region, year)
                    else:
                        collector.fetch_region(region)
                
                collector.close()
                self.logger.info(f"✅ {name} 수집 완료")
                
            except Exception as e:
                self.logger.error(f"❌ {name} 수집 실패: {e}")
                return False
        
        return True
    
    def _create_total_dbs(self, year: int):
        """통합 DB 생성"""
        self.logger.info("🔗 통합 DB 생성 중...")
        
        # meal_total.db
        try:
            from scripts.merge_meal_dbs import merge_databases
            merge_databases(consolidate_vocab=True)
            self.logger.info("  ✅ meal_total.db 생성 완료")
        except Exception as e:
            self.logger.error(f"  ❌ meal_total.db 생성 실패: {e}")
            raise
    
    def _create_backup_dir(self, year: int) -> str:
        """백업 디렉토리 생성"""
        backup_dir = os.path.join(self.backup_dir, str(year))
        os.makedirs(backup_dir, exist_ok=True)
        return backup_dir
    
    def _backup_databases(self, year: int, backup_dir: str):
        """DB 파일 백업"""
        self.logger.info("💾 DB 파일 백업 중...")
        
        # Collector DB 백업
        for name, info in self.COLLECTORS.items():
            if info.get("db_file"):
                src = os.path.join(self.base_dir, "active", info["db_file"])
                if os.path.exists(src):
                    dst = os.path.join(backup_dir, info["backup_file"].format(year=year))
                    
                    try:
                        vacuum_into(src, dst)
                        size = os.path.getsize(dst) / 1024 / 1024
                        self.logger.info(f"  ✅ {name} 백업: {dst} ({size:.1f}MB)")
                    except Exception as e:
                        self.logger.error(f"  ❌ {name} 백업 실패: {e}")
                        raise
        
        # 글로벌 DB 백업
        for db in self.GLOBAL_DBS:
            src = os.path.join(self.base_dir, db["name"])
            if os.path.exists(src):
                dst = os.path.join(backup_dir, f"{year}_{db['name']}")
                try:
                    vacuum_into(src, dst)
                    size = os.path.getsize(dst) / 1024 / 1024
                    self.logger.info(f"  ✅ 글로벌 백업: {dst} ({size:.1f}MB)")
                except Exception as e:
                    self.logger.error(f"  ❌ 글로벌 백업 실패: {e}")
    
    # ========================================================
    # 2. 검증 (3단계)
    # ========================================================
    
    def verify_backup(self, year: int) -> List[Dict]:
        """
        3단계 백업 검증
        
        Returns:
            문제 리스트 (비어있으면 성공)
        """
        self.logger.info(f"🔍 학년도 {year} 백업 검증 시작")
        
        backup_dir = os.path.join(self.backup_dir, str(year))
        if not os.path.exists(backup_dir):
            self.logger.error(f"❌ 백업 디렉토리 없음: {backup_dir}")
            return [{"type": "no_backup", "year": year}]
        
        issues = []
        
        for name, info in self.COLLECTORS.items():
            if not info.get("db_file"):
                continue
            
            src = os.path.join(self.base_dir, "active", info["db_file"])
            dst = os.path.join(backup_dir, info["backup_file"].format(year=year))
            
            if not os.path.exists(dst):
                self.logger.error(f"  ❌ {name} 백업 파일 없음")
                issues.append({"type": "missing", "collector": name})
                continue
            
            self.logger.info(f"  📋 {name} 검증 중...")
            
            # STEP 1: 개수 검증
            step1_ok = self._verify_count(src, dst, info["table"])
            if not step1_ok:
                issues.append({
                    "type": "count_mismatch",
                    "collector": name,
                    "src": src,
                    "dst": dst,
                    "table": info["table"]
                })
                continue
            
            # STEP 2: 무결성 검증
            step2_ok = self._verify_integrity(dst)
            if not step2_ok:
                issues.append({
                    "type": "corruption",
                    "collector": name,
                    "dst": dst
                })
                continue
            
            # STEP 3: 샘플 검증
            step3_ok = self._verify_samples(src, dst, info["table"])
            if not step3_ok:
                issues.append({
                    "type": "sample_mismatch",
                    "collector": name,
                    "src": src,
                    "dst": dst,
                    "table": info["table"]
                })
                continue
            
            self.logger.info(f"  ✅ {name} 3단계 검증 완료")
        
        # 글로벌 DB 검증
        for db in self.GLOBAL_DBS:
            src = os.path.join(self.base_dir, db["name"])
            dst = os.path.join(backup_dir, f"{year}_{db['name']}")
            
            if os.path.exists(dst):
                if self._verify_count(src, dst, db["table"]):
                    self.logger.info(f"  ✅ {db['name']} 검증 완료")
                else:
                    issues.append({
                        "type": "global_count_mismatch",
                        "db": db["name"]
                    })
        
        if not issues:
            self.logger.info(f"✅ 학년도 {year} 백업 검증 완료 (모두 정상)")
        
        return issues
    
    def _verify_count(self, src: str, dst: str, table: str) -> bool:
        """STEP 1: 개수 검증"""
        try:
            with sqlite3.connect(src) as s_conn:
                s_count = s_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            
            with sqlite3.connect(dst) as d_conn:
                d_count = d_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            
            if s_count != d_count:
                self.logger.error(f"    STEP 1 실패: 개수 불일치 ({s_count} vs {d_count})")
                return False
            
            self.logger.info(f"    ✅ STEP 1: {s_count}개 일치")
            return True
            
        except Exception as e:
            self.logger.error(f"    STEP 1 오류: {e}")
            return False
    
    def _verify_integrity(self, db_path: str) -> bool:
        """STEP 2: 무결성 검증"""
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.execute("PRAGMA integrity_check")
                result = cur.fetchone()[0]
                
                if result != "ok":
                    self.logger.error(f"    STEP 2 실패: {result}")
                    return False
                
                self.logger.info(f"    ✅ STEP 2: 무결성 정상")
                return True
                
        except Exception as e:
            self.logger.error(f"    STEP 2 오류: {e}")
            return False
    
    def _verify_samples(self, src: str, dst: str, table: str, sample_size: int = 10) -> bool:
        """STEP 3: 샘플 검증"""
        try:
            with sqlite3.connect(src) as s_conn:
                # ID 컬럼 찾기
                cur = s_conn.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cur.fetchall()]
                id_col = columns[0]  # 보통 첫 번째 컬럼이 PK
                
                # 랜덤 샘플 추출
                samples = s_conn.execute(f"""
                    SELECT * FROM {table} 
                    ORDER BY RANDOM() 
                    LIMIT {sample_size}
                """).fetchall()
            
            with sqlite3.connect(dst) as d_conn:
                for sample in samples:
                    sample_id = sample[0]
                    d_data = d_conn.execute(
                        f"SELECT * FROM {table} WHERE {id_col} = ?",
                        (sample_id,)
                    ).fetchone()
                    
                    if not d_data or str(sample) != str(d_data):
                        self.logger.error(f"    STEP 3 실패: ID {sample_id} 불일치")
                        return False
            
            self.logger.info(f"    ✅ STEP 3: {sample_size}개 샘플 일치")
            return True
            
        except Exception as e:
            self.logger.error(f"    STEP 3 오류: {e}")
            return False
    
    # ========================================================
    # 3. 자동 복구
    # ========================================================
    
    def _auto_repair(self, year: int, issues: List[Dict]):
        """문제 자동 복구"""
        self.logger.info("🔧 자동 복구 시작")
        
        for issue in issues:
            if issue["type"] == "count_mismatch":
                self._repair_count_mismatch(issue)
            elif issue["type"] == "corruption":
                self._repair_corruption(issue)
            elif issue["type"] == "sample_mismatch":
                self._repair_sample_mismatch(issue)
            elif issue["type"] == "missing":
                self._repair_missing_backup(issue, year)
        
        self.logger.info("✅ 자동 복구 완료")
    
    def _repair_count_mismatch(self, issue: Dict):
        """개수 불일치 복구"""
        collector = issue["collector"]
        self.logger.info(f"  🔧 {collector} 개수 불일치 복구 중...")
        
        try:
            # 해당 Collector만 다시 실행
            if collector == "school":
                c = SchoolMasterCollector(shard="none", full=True)
            elif collector == "meal":
                c = MealCollector(shard="none", full=True)
            elif collector == "timetable":
                c = TimetableCollector(shard="none", full=True)
            elif collector == "schedule":
                c = ScheduleCollector(shard="none", full=True)
            
            from constants.codes import ALL_REGIONS
            for region in ALL_REGIONS:
                if collector == "timetable":
                    c.fetch_region(region, year, 1)
                    c.fetch_region(region, year, 2)
                else:
                    c.fetch_region(region)
            
            c.close()
            
            # 다시 백업
            backup_dir = os.path.join(self.backup_dir, str(year))
            src = os.path.join(self.base_dir, "active", self.COLLECTORS[collector]["db_file"])
            dst = os.path.join(backup_dir, self.COLLECTORS[collector]["backup_file"].format(year=year))
            
            vacuum_into(src, dst)
            self.logger.info(f"  ✅ {collector} 복구 완료")
            
        except Exception as e:
            self.logger.error(f"  ❌ {collector} 복구 실패: {e}")
    
    def _repair_corruption(self, issue: Dict):
        """손상된 DB 복구"""
        dst = issue["dst"]
        self.logger.info(f"  🔧 손상된 DB 복구 중: {dst}")
        
        try:
            # 손상된 파일 백업
            corrupt_backup = dst + ".corrupt"
            os.rename(dst, corrupt_backup)
            
            # 원본에서 다시 백업
            collector = issue["collector"]
            src = os.path.join(self.base_dir, "active", self.COLLECTORS[collector]["db_file"])
            vacuum_into(src, dst)
            
            self.logger.info(f"  ✅ 복구 완료 (손상본: {corrupt_backup})")
            
        except Exception as e:
            self.logger.error(f"  ❌ 복구 실패: {e}")
    
    def _repair_sample_mismatch(self, issue: Dict):
        """샘플 불일치 복구"""
        self.logger.info(f"  🔧 {issue['collector']} 샘플 불일치 복구 중...")
        # 개수 불일치와 동일하게 처리
        self._repair_count_mismatch(issue)
    
    def _repair_missing_backup(self, issue: Dict, year: int):
        """누락된 백업 복구"""
        collector = issue["collector"]
        self.logger.info(f"  🔧 {collector} 백업 누락 복구 중...")
        
        try:
            src = os.path.join(self.base_dir, "active", self.COLLECTORS[collector]["db_file"])
            backup_dir = os.path.join(self.backup_dir, str(year))
            dst = os.path.join(backup_dir, self.COLLECTORS[collector]["backup_file"].format(year=year))
            
            vacuum_into(src, dst)
            self.logger.info(f"  ✅ {collector} 백업 생성 완료")
            
        except Exception as e:
            self.logger.error(f"  ❌ {collector} 백업 실패: {e}")
    
    # ========================================================
    # 4. 복원 및 관리
    # ========================================================
    
    def restore_backup(self, year: int, collectors: List[str] = None) -> bool:
        """백업 복원"""
        backup_dir = os.path.join(self.backup_dir, str(year))
        if not os.path.exists(backup_dir):
            self.logger.error(f"❌ 백업 없음: {backup_dir}")
            return False
        
        self.logger.info(f"🔄 학년도 {year} 복원 시작")
        
        if collectors:
            target_collectors = collectors
        else:
            target_collectors = list(self.COLLECTORS.keys())
        
        for name in target_collectors:
            info = self.COLLECTORS[name]
            backup_file = os.path.join(backup_dir, info["backup_file"].format(year=year))
            
            if os.path.exists(backup_file):
                dst = os.path.join(self.base_dir, "active", info["db_file"])
                
                # 현재 DB 백업 (혹시 모를 복구용)
                if os.path.exists(dst):
                    current_backup = dst + ".current"
                    shutil.copy2(dst, current_backup)
                
                shutil.copy2(backup_file, dst)
                self.logger.info(f"  ✅ {name} 복원: {dst}")
        
        self.logger.info(f"✅ 학년도 {year} 복원 완료")
        return True
    
    def list_backups(self):
        """백업 목록 조회"""
        if not os.path.exists(self.backup_dir):
            print("📁 백업 디렉토리 없음")
            return
        
        print("\n📚 학년도 백업 목록")
        print("=" * 70)
        
        for year_dir in sorted(os.listdir(self.backup_dir)):
            if not year_dir.isdigit():
                continue
            
            year_path = os.path.join(self.backup_dir, year_dir)
            if os.path.isdir(year_path):
                files = os.listdir(year_path)
                total_size = sum(os.path.getsize(os.path.join(year_path, f)) 
                               for f in files if os.path.isfile(os.path.join(year_path, f)))
                
                print(f"\n📅 {year_dir}학년도")
                print(f"  📦 파일: {len(files)}개")
                print(f"  💾 용량: {total_size / 1024 / 1024:.1f} MB")
                
                # 최근 검증 로그 확인
                log_file = os.path.join(year_path, "backup_success.log")
                if os.path.exists(log_file):
                    with open(log_file, 'r') as f:
                        last_success = f.read().strip()
                    print(f"  ✅ 마지막 성공: {last_success}")
    
    def _save_success_log(self, year: int):
        """성공 로그 저장"""
        backup_dir = os.path.join(self.backup_dir, str(year))
        log_file = os.path.join(backup_dir, "backup_success.log")
        
        with open(log_file, 'w') as f:
            f.write(now_kst().isoformat())
    
    def cleanup_old_backups(self, keep_years: int = 5):
        """오래된 백업 정리"""
        self.logger.info(f"🗑️ {keep_years}년 이상 된 백업 정리 중...")
        
        backups = [d for d in os.listdir(self.backup_dir) if d.isdigit()]
        backups.sort()
        
        if len(backups) <= keep_years:
            self.logger.info("✅ 정리할 백업 없음")
            return
        
        to_delete = backups[:-keep_years]
        for year in to_delete:
            path = os.path.join(self.backup_dir, year)
            shutil.rmtree(path)
            self.logger.info(f"  ✅ {year}학년도 백업 삭제")


# ========================================================
# CLI
# ========================================================
def main():
    parser = argparse.ArgumentParser(description="학년도 전체 백업 (자동 복구 포함)")
    
    # 기본 옵션
    parser.add_argument("--year", type=int, help="학년도 (예: 2026)")
    
    # 실행 모드
    parser.add_argument("--full", action="store_true", help="전체 백업 수행")
    parser.add_argument("--verify", action="store_true", help="백업 검증")
    parser.add_argument("--restore", action="store_true", help="백업 복원")
    parser.add_argument("--list", action="store_true", help="백업 목록 조회")
    parser.add_argument("--cleanup", action="store_true", help="오래된 백업 정리")
    
    # 옵션
    parser.add_argument("--collectors", "-c", help="대상 Collector (콤마 구분)")
    parser.add_argument("--keep", type=int, default=5, help="보관할 학년도 수 (기본: 5)")
    parser.add_argument("--auto-repair", action="store_true", help="자동 복구 활성화")
    
    args = parser.parse_args()
    
    backup = YearlyBackup()
    
    if args.list:
        backup.list_backups()
        return
    
    if args.cleanup:
        backup.cleanup_old_backups(args.keep)
        return
    
    if not args.year:
        parser.print_help()
        return
    
    collectors = args.collectors.split(",") if args.collectors else None
    
    if args.verify:
        issues = backup.verify_backup(args.year)
        if issues:
            print(f"\n⚠️ {len(issues)}개 문제 발견:")
            for issue in issues:
                print(f"  - {issue}")
        return
    
    if args.restore:
        backup.restore_backup(args.year, collectors)
        return
    
    if args.full:
        success = backup.run_full_backup(args.year, collectors)
        if success:
            print(f"\n✅ 학년도 {args.year} 백업 완료!")
        else:
            print(f"\n❌ 학년도 {args.year} 백업 실패 - 로그 확인 필요")
        return
    
    parser.print_help()


if __name__ == "__main__":
    main()
    