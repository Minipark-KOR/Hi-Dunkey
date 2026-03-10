#!/usr/bin/env python3
"""
학년도 전체 백업 스크립트 (자동 복구 + 메트릭 수집)
- 백업 파일명: {yyyymmdd}_{domain}.db
- 생성날짜 기준 3년 이상 된 파일은 archive로 자동 이동
- 검증 실패 시 auto repair (최대 3회 재시도)
- 메트릭은 core/metrics.py 공통 모듈 사용
"""
import os
import sys
import sqlite3
import shutil
import time
import glob
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from core.backup import vacuum_into
    from core.school_year import get_current_school_year
except ImportError:
    def vacuum_into(src, dst):
        shutil.copy2(src, dst)
        with sqlite3.connect(dst) as c:
            c.execute("VACUUM")

    def get_current_school_year(dt):
        return dt.year if dt.month >= 3 else dt.year - 1

from core.kst_time import now_kst
from core.metrics import generate_and_save_metrics, cleanup_old_metrics
from constants.domains import DOMAIN_CONFIG, GLOBAL_DBS
from constants.paths import DATA_DIR, BACKUP_DIR, ARCHIVE_DIR, LOG_DIR, METRICS_DIR
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ✅ 올바른 collector 클래스 임포트
from collectors.neis_info_collector import NeisInfoCollector
from collectors.meal_collector import MealCollector
from collectors.timetable_collector import AnnualFullTimetableCollector as TimetableCollector
from collectors.schedule_collector import AnnualFullScheduleCollector as ScheduleCollector
from constants.codes import ALL_REGIONS

COLLECTOR_CLASSES = {
    "school":    NeisInfoCollector,
    "meal":      MealCollector,
    "timetable": TimetableCollector,
    "schedule":  ScheduleCollector,
}

class YearlyBackup:
    # ... (클래스 내용은 동일, 변경 없음)
    def __init__(self, base_dir: str = str(DATA_DIR), backup_dir: str = str(BACKUP_DIR),
                archive_dir: str = str(ARCHIVE_DIR), log_dir: str = str(LOG_DIR)):
        self.base_dir    = base_dir
        self.backup_dir  = backup_dir
        self.archive_dir = archive_dir
        self.log_dir     = log_dir
        self.max_retries = 3

        for d in [base_dir, backup_dir, archive_dir, log_dir, METRICS_DIR]:
            os.makedirs(d, exist_ok=True)

        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger("yearly_backup")
        self.logger.setLevel(logging.INFO)
        fh  = logging.FileHandler(LOG_DIR/"yearly_backup.log", encoding="utf-8")
        ch  = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    # ──────────────────────────────────────────
    # Public API (변경 없음)
    # ──────────────────────────────────────────

    def run_full_backup(self, year: Optional[int] = None,
                        collectors: Optional[List[str]] = None) -> bool:
        if year is None:
            year = get_current_school_year(now_kst())
            self.logger.info(f"📆 학년도 자동 설정: {year}")

        self.logger.info(f"📅 학년도 {year} 전체 백업 시작 (최대 {self.max_retries}회)")

        for attempt in range(self.max_retries):
            self.logger.info(f"🔄 시도 {attempt + 1}/{self.max_retries}")
            try:
                if not self._run_collectors(year, collectors):
                    raise RuntimeError("Collector 실행 실패")

                self._create_total_dbs(year)
                self._write_backups(year)

                issues = self.verify_backup(year, self.backup_dir)
                if not issues:
                    self.logger.info(f"✅ 학년도 {year} 백업 성공")
                    self._save_success_log(year)
                    self._save_metrics(year)
                    self.move_old_backups_to_archive()
                    cleanup_old_metrics(METRICS_DIR, keep=12)
                    return True

                self.logger.warning(f"⚠️ {len(issues)}개 문제 발견 → 자동 복구 시작")
                self._auto_repair(year, issues)
                if attempt < self.max_retries - 1:
                    time.sleep(10)

            except Exception as e:
                self.logger.error(f"❌ 백업 실패: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(10)

        self.logger.critical(f"❌ {self.max_retries}회 시도 모두 실패")
        return False

    def verify_backup(self, year: int, backup_dir: str) -> List[Dict]:
        self.logger.info(f"🔍 학년도 {year} 백업 검증 시작")
        if not os.path.exists(backup_dir):
            return [{"type": "no_backup_dir"}]

        issues = []
        for name, info in DOMAIN_CONFIG.items():
            src = os.path.join(BASE_DIR, info["db_path"])
            if not os.path.exists(src):
                continue
            files = glob.glob(os.path.join(backup_dir, f"*_{name}.db"))
            if not files:
                issues.append({"type": "missing", "collector": name})
                continue
            latest = sorted(files, reverse=True)[0]
            table  = info["table"]
            if not self._verify_integrity(latest):
                issues.append({"type": "corruption", "collector": name, "dst": latest})
            elif not self._verify_count(src, latest, table):
                issues.append({"type": "count_mismatch", "collector": name,
                                "src": src, "dst": latest, "table": table})
            elif not self._verify_samples(src, latest, table):
                issues.append({"type": "sample_mismatch", "collector": name,
                                "src": src, "dst": latest, "table": table})
            else:
                self.logger.info(f"  ✅ {name} 검증 완료")

        for db in GLOBAL_DBS:
            src = os.path.join(BASE_DIR, db["path"])
            if not os.path.exists(src):
                continue
            files = glob.glob(os.path.join(backup_dir, f"*_{db['name']}"))
            if not files:
                issues.append({"type": "missing_global", "db": db["name"]})
                continue
            latest = sorted(files, reverse=True)[0]
            if not self._verify_count(src, latest, db["table"]):
                issues.append({"type": "global_count_mismatch", "db": db["name"]})
            else:
                self.logger.info(f"  ✅ {db['name']} 검증 완료")

        return issues

    def restore_backup(self, year: int,
                       collectors: Optional[List[str]] = None) -> bool:
        self.logger.info(f"🔄 학년도 {year} 복원 시작")
        if not os.path.exists(self.backup_dir):
            self.logger.error("❌ 백업 디렉토리 없음")
            return False

        targets = collectors if collectors else list(DOMAIN_CONFIG.keys())
        for name in targets:
            files = glob.glob(os.path.join(self.backup_dir, f"*_{name}.db"))
            if not files:
                self.logger.warning(f"  ⚠️ {name} 백업 없음")
                continue
            latest = sorted(files, reverse=True)[0]
            dst    = os.path.join(BASE_DIR, DOMAIN_CONFIG[name]["db_path"])
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if os.path.exists(dst):
                shutil.copy2(dst, dst + ".current")
            shutil.copy2(latest, dst)
            self.logger.info(f"  ✅ {name} 복원: {latest}")

        for db in GLOBAL_DBS:
            files = glob.glob(os.path.join(self.backup_dir, f"*_{db['name']}"))
            if files:
                latest = sorted(files, reverse=True)[0]
                dst    = os.path.join(BASE_DIR, db["path"])
                shutil.copy2(latest, dst)
                self.logger.info(f"  ✅ {db['name']} 복원")
        return True

    def list_backups(self):
        if not os.path.exists(self.backup_dir):
            print("📁 백업 디렉토리 없음")
            return
        files = sorted(
            [f for f in os.listdir(self.backup_dir) if f.endswith(".db")],
            reverse=True
        )
        by_date: Dict[str, List[str]] = {}
        for f in files:
            d = f.split("_")[0]
            by_date.setdefault(d, []).append(f)
        for d, fl in sorted(by_date.items(), reverse=True):
            total = sum(
                os.path.getsize(os.path.join(self.backup_dir, f)) for f in fl
            )
            print(f"\n📅 {d}  ({len(fl)}개, {total/1024/1024:.1f} MB)")
            for f in sorted(fl):
                sz = os.path.getsize(os.path.join(self.backup_dir, f)) / 1024
                print(f"    - {f} ({sz:.1f} KB)")

    def move_old_backups_to_archive(self, cutoff_years: int = 3):
        self.logger.info(f"📦 {cutoff_years}년 이상 된 백업 → archive 이동")
        os.makedirs(self.archive_dir, exist_ok=True)

        if not os.path.exists(self.backup_dir):
            self.logger.warning(f"⚠️ 백업 디렉토리 없음: {self.backup_dir}")
            return

        cutoff = now_kst().date() - timedelta(days=365 * cutoff_years)
        moved  = 0

        for fname in os.listdir(self.backup_dir):
            if not fname.endswith(".db"):
                continue
            try:
                fdate = datetime.strptime(fname.split("_")[0], "%Y%m%d").date()
            except (ValueError, IndexError):
                continue
            if fdate >= cutoff:
                continue
            src = os.path.join(self.backup_dir, fname)
            dst = os.path.join(self.archive_dir, fname)
            if os.path.exists(dst):
                base, ext = os.path.splitext(fname)
                cnt = 1
                while os.path.exists(
                    os.path.join(self.archive_dir, f"{base}_{cnt}{ext}")
                ):
                    cnt += 1
                dst = os.path.join(self.archive_dir, f"{base}_{cnt}{ext}")
            shutil.move(src, dst)
            self.logger.info(f"  📦 이동: {fname} → {os.path.basename(dst)}")
            moved += 1

        self.logger.info(f"✅ {moved}개 파일 이동 완료 (기준일: {cutoff})")

    def cleanup_old_backups(self, keep: int = 5):
        self.logger.info(f"🗑️ 최근 {keep}개만 남기고 삭제")
        dated = []
        for f in os.listdir(self.backup_dir):
            if not f.endswith(".db"):
                continue
            try:
                d = datetime.strptime(f.split("_")[0], "%Y%m%d").date()
                dated.append((d, f))
            except (ValueError, IndexError):
                continue
        dated.sort()
        for _, f in dated[:-keep]:
            os.remove(os.path.join(self.backup_dir, f))
            self.logger.info(f"  🗑️ 삭제: {f}")

    # ──────────────────────────────────────────
    # Internal — metrics
    # ──────────────────────────────────────────

    def _save_metrics(self, year: int):
        backup_date = now_kst().strftime("%Y%m%d")
        result = generate_and_save_metrics(
            backup_date=backup_date,
            base_dir=str(BASE_DIR),
            metrics_dir=METRICS_DIR,
            domain_config=DOMAIN_CONFIG,
            global_dbs=GLOBAL_DBS,
            include_geo=True,
            include_global_tables=True,
            print_to_stdout=True,
        )
        self.logger.info(f"📄 요약 저장: {result['summary_path']}")
        self.logger.info(f"📊 메트릭 저장: {result['metrics_path']}")

    # ──────────────────────────────────────────
    # Internal — collect & merge
    # ──────────────────────────────────────────

    def _run_collectors(self, year: int,
                        collectors: Optional[List[str]] = None) -> bool:
        targets = collectors if collectors else [
            n for n, i in DOMAIN_CONFIG.items() if i["enabled"]
        ]
        for name in targets:
            self.logger.info(f"🚀 {name} 수집 시작")
            try:
                cls       = COLLECTOR_CLASSES[name]
                collector = cls(shard="none", full=True)
                for region in ALL_REGIONS:
                    args = DOMAIN_CONFIG[name]["fetch_args"](region, year)
                    if isinstance(args, list):
                        for a in args:
                            collector.fetch_region(**a)
                    else:
                        collector.fetch_region(**args)
                collector.close()
                self.logger.info(f"  ✅ {name} 수집 완료")
            except Exception as e:
                self.logger.error(f"  ❌ {name} 수집 실패: {e}")
                return False
        return True

    def _create_total_dbs(self, year: int):
        self.logger.info("🔗 통합 DB 생성 중...")
        for name, cfg in DOMAIN_CONFIG.items():
            if not cfg["enabled"]:
                continue
            try:
                module   = __import__(
                    f"scripts.{cfg['merge_script']}",
                    fromlist=["merge_databases"]
                )
                merge_fn = getattr(module, "merge_databases")
                if name in ("meal", "timetable", "schedule"):
                    merge_fn(do_consolidate_vocab=True)
                else:
                    merge_fn()
                self.logger.info(f"  ✅ {name} 통합 DB 생성 완료")
            except Exception as e:
                self.logger.error(f"  ❌ {name} 통합 DB 생성 실패: {e}")
                raise

    def _write_backups(self, year: int):
        backup_date = now_kst().strftime("%Y%m%d")
        for name, cfg in DOMAIN_CONFIG.items():
            src = os.path.join(BASE_DIR, cfg["db_path"])
            if not os.path.exists(src):
                self.logger.warning(f"  ⚠️ {name} DB 없음: {src}")
                continue
            dst  = os.path.join(self.backup_dir, f"{backup_date}_{name}.db")
            vacuum_into(src, dst)
            size = os.path.getsize(dst) / 1024 / 1024
            self.logger.info(
                f"  ✅ {name} 백업: {os.path.basename(dst)} ({size:.1f} MB)"
            )
        for db in GLOBAL_DBS:
            src = os.path.join(BASE_DIR, db["path"])
            if not os.path.exists(src):
                continue
            dst  = os.path.join(self.backup_dir, f"{backup_date}_{db['name']}")
            vacuum_into(src, dst)
            size = os.path.getsize(dst) / 1024 / 1024
            self.logger.info(f"  ✅ {db['name']} 백업 ({size:.1f} MB)")

    # ──────────────────────────────────────────
    # Internal — verify
    # ──────────────────────────────────────────

    def _verify_integrity(self, db_path: str) -> bool:
        try:
            with sqlite3.connect(db_path) as conn:
                return conn.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0] == "ok"
        except Exception:
            return False

    def _verify_count(self, src: str, dst: str, table: str) -> bool:
        try:
            with sqlite3.connect(src) as s:
                sc = s.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            with sqlite3.connect(dst) as d:
                dc = d.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            return sc == dc
        except Exception:
            return False

    def _verify_samples(self, src: str, dst: str,
                        table: str, sample_size: int = 10) -> bool:
        try:
            with sqlite3.connect(src) as s:
                cols    = [c[1] for c in s.execute(f"PRAGMA table_info({table})")]
                id_col  = cols[0]
                samples = s.execute(
                    f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT {sample_size}"
                ).fetchall()
            with sqlite3.connect(dst) as d:
                for row in samples:
                    drow = d.execute(
                        f"SELECT * FROM {table} WHERE {id_col} = ?", (row[0],)
                    ).fetchone()
                    if drow is None or str(row) != str(drow):
                        return False
            return True
        except Exception:
            return False

    # ──────────────────────────────────────────
    # Internal — auto repair
    # ──────────────────────────────────────────

    def _auto_repair(self, year: int, issues: List[Dict]):
        self.logger.info("🔧 자동 복구 시작")
        for issue in issues:
            t = issue["type"]
            if t in ("count_mismatch", "sample_mismatch"):
                self._repair_recollect(issue, year)
            elif t == "corruption":
                self._repair_corruption(issue, year)
            elif t == "missing":
                self._repair_missing(issue, year)
            elif t in ("missing_global", "global_count_mismatch"):
                self.logger.warning(
                    f"  ⚠️ 글로벌 DB 문제: {issue.get('db')} (수동 확인 필요)"
                )
        self.logger.info("✅ 자동 복구 완료")

    def _repair_recollect(self, issue: Dict, year: int):
        name = issue["collector"]
        self.logger.info(f"  🔧 {name} 재수집 시작")
        try:
            cls       = COLLECTOR_CLASSES[name]
            collector = cls(shard="none", full=True)
            for region in ALL_REGIONS:
                args = DOMAIN_CONFIG[name]["fetch_args"](region, year)
                if isinstance(args, list):
                    for a in args:
                        collector.fetch_region(**a)
                else:
                    collector.fetch_region(**args)
            collector.close()
            module   = __import__(
                f"scripts.{DOMAIN_CONFIG[name]['merge_script']}",
                fromlist=["merge_databases"]
            )
            merge_fn = getattr(module, "merge_databases")
            if name in ("meal", "timetable", "schedule"):
                merge_fn(do_consolidate_vocab=True)
            else:
                merge_fn()
            backup_date = now_kst().strftime("%Y%m%d")
            src = os.path.join(BASE_DIR, DOMAIN_CONFIG[name]["db_path"])
            dst = os.path.join(self.backup_dir, f"{backup_date}_{name}.db")
            vacuum_into(src, dst)
            self.logger.info(f"  ✅ {name} 복구 완료")
        except Exception as e:
            self.logger.error(f"  ❌ {name} 복구 실패: {e}")

    def _repair_corruption(self, issue: Dict, year: int):
        dst  = issue["dst"]
        name = issue["collector"]
        self.logger.info(f"  🔧 손상 파일 복구: {os.path.basename(dst)}")
        try:
            os.rename(dst, dst + ".corrupt")
            src         = os.path.join(BASE_DIR, DOMAIN_CONFIG[name]["db_path"])
            backup_date = now_kst().strftime("%Y%m%d")
            new_dst     = os.path.join(self.backup_dir, f"{backup_date}_{name}.db")
            vacuum_into(src, new_dst)
            self.logger.info(f"  ✅ {name} 백업 재생성 완료")
        except Exception as e:
            self.logger.error(f"  ❌ {name} 손상 복구 실패: {e}")

    def _repair_missing(self, issue: Dict, year: int):
        name = issue["collector"]
        self.logger.info(f"  🔧 {name} 백업 누락 복구")
        try:
            src = os.path.join(BASE_DIR, DOMAIN_CONFIG[name]["db_path"])
            if not os.path.exists(src):
                self.logger.warning(f"  ⚠️ 원본 DB도 없음 → 재수집")
                self._repair_recollect(issue, year)
                return
            backup_date = now_kst().strftime("%Y%m%d")
            dst         = os.path.join(self.backup_dir, f"{backup_date}_{name}.db")
            vacuum_into(src, dst)
            self.logger.info(f"  ✅ {name} 누락 백업 생성 완료")
        except Exception as e:
            self.logger.error(f"  ❌ {name} 누락 복구 실패: {e}")

    def _save_success_log(self, year: int):
        path = os.path.join(self.backup_dir, f"backup_success_{year}.log")
        with open(path, "w") as f:
            f.write(now_kst().isoformat())


# ──────────────────────────────────────────
# CLI (변경 없음)
# ──────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="학년도 전체 백업")
    parser.add_argument("--year",       type=int,            help="학년도 (미지정 시 자동)")
    parser.add_argument("--full",       action="store_true", help="전체 백업 수행")
    parser.add_argument("--verify",     action="store_true", help="백업 검증")
    parser.add_argument("--restore",    action="store_true", help="백업 복원")
    parser.add_argument("--list",       action="store_true", help="백업 목록 조회")
    parser.add_argument("--cleanup",    action="store_true", help="오래된 백업 삭제")
    parser.add_argument("--collectors", "-c",                help="콤마 구분 (예: meal,school)")
    parser.add_argument("--keep",       type=int, default=5, help="보관 개수 (기본 5)")
    args = parser.parse_args()

    backup = YearlyBackup()
    cols   = args.collectors.split(",") if args.collectors else None

    if args.list:
        backup.list_backups()
    elif args.cleanup:
        backup.cleanup_old_backups(args.keep)
    elif args.verify:
        if not args.year:
            print("❌ --year 필요")
            return
        issues = backup.verify_backup(args.year, backup.backup_dir)
        if issues:
            print(f"\n⚠️ {len(issues)}개 문제:")
            for i in issues:
                print(f"  - {i}")
        else:
            print("✅ 이상 없음")
    elif args.restore:
        if not args.year:
            print("❌ --year 필요")
            return
        backup.restore_backup(args.year, cols)
    elif args.full:
        success = backup.run_full_backup(args.year, cols)
        sys.exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
    