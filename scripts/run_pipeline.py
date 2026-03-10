#!/usr/bin/env python3
# scripts/run_pipeline.py
# 개발 가이드: docs/developer_guide.md 참조
"""
병렬 샤드 실행기 + 자동 병합 + 연도 프롬프트 + 로그 정리 (collector_cli.py 기반)
여러 region에 대해 odd/even 샤드를 병렬로 실행하며, rich를 사용한 실시간 멀티바 진행률을 표시합니다.
"""
import sys
import subprocess
import threading
import time
import argparse
import signal
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# 프로젝트 루트를 sys.path에 추가 (constants 모듈 임포트 전에)
sys.path.append(str(Path(__file__).parent.parent))

# 이제 프로젝트 모듈 임포트 가능
from constants.paths import LOG_DIR
from core.kst_time import KST, now_kst

try:
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, SpinnerColumn
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    # 폴백: 간단한 텍스트 진행률
    def simple_progress(completed, total):
        print(f"\r📊 진행: {completed}/{total} ({(completed/total*100):.1f}%)", end='', flush=True)


log_dir = LOG_DIR

# 도메인별 병합 스크립트 매핑
MERGE_SCRIPT_MAP = {
    "neis_info": "merge_neis_info_dbs.py",
    "school_info": "merge_school_info_dbs.py",
    "meal": "merge_meal_dbs.py",
    "schedule": "merge_schedule_dbs.py",
    "timetable": "merge_timetable_dbs.py",
}

def get_merge_script(collector: str) -> str:
    return MERGE_SCRIPT_MAP.get(collector, f"merge_{collector}_dbs.py")

def get_current_school_year() -> int:
    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1

def validate_year(year: int) -> bool:
    current = get_current_school_year()
    return 2000 <= year <= current

def prompt_year(default_year: int) -> int:
    if not sys.stdin.isatty():
        print(f"ℹ️ 비대화형 환경이므로 기본 연도({default_year})를 사용합니다.")
        return default_year

    while True:
        try:
            user_input = input(f"수집할 학년도를 입력하세요 (기본값: {default_year}): ").strip()
            if user_input == "":
                return default_year
            year = int(user_input)
            if validate_year(year):
                return year
            current = get_current_school_year()
            print(f"⚠️ 2000~{current} 사이의 연도를 입력하세요.")
        except ValueError:
            print("⚠️ 숫자를 입력해주세요.")
        except (EOFError, KeyboardInterrupt):
            print("\n⚠️ 사용자에 의해 중단되었습니다.")
            sys.exit(1)

def parse_regions(regions_arg: str) -> List[str]:
    """지역 코드 파싱 (필요시 constants.codes에서 ALL_REGIONS 임포트)"""
    if regions_arg.upper() == "ALL":
        from constants.codes import ALL_REGIONS
        return ALL_REGIONS
    return [r.strip() for r in regions_arg.split(",") if r.strip()]

def make_env() -> dict:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f".{os.pathsep}{existing}" if existing else "."
    return env

def cleanup_old_logs(log_dir: Path, collector: str, days: int = 7):
    """오래된 로그 파일 정리 (KST 타임존 기준)"""
    cutoff = KST.localize(datetime.now().replace(tzinfo=None)) - timedelta(days=days)
    for log_file in log_dir.glob(f"{collector}_*.log"):
        try:
            mtime = KST.localize(datetime.fromtimestamp(log_file.stat().st_mtime))
            if mtime < cutoff:
                log_file.unlink()
                print(f"🧹 오래된 로그 삭제: {log_file.name}")
        except Exception:
            pass

# 전역 상태 관리
results = {}  # (region, shard) -> returncode
lock = threading.Lock()
processes = []  # 실행 중인 Popen 객체 리스트

def signal_handler(sig, frame):
    signal_names = {
        signal.SIGINT: 'SIGINT',
        signal.SIGTERM: 'SIGTERM',
        getattr(signal, 'SIGHUP', None): 'SIGHUP',
    }
    sig_name = signal_names.get(sig, f'SIG{sig}')

    print(f"\n[{sig_name}] 종료 신호 수신. 자식 프로세스 정리 중...")
    with lock:
        procs = processes[:]
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                p.kill()
                p.wait()
            except Exception:
                pass
    print("👋 모든 작업이 중단되었습니다.")
    os._exit(1)

def run_merge(collector: str, year: int, log_dir: Path, timeout: Optional[int] = None) -> bool:
    merge_script_path = Path("scripts") / get_merge_script(collector)
    if not merge_script_path.exists():
        print(f"⚠️ 병합 스크립트 없음: {merge_script_path}")
        return False

    merge_cmd = [sys.executable, str(merge_script_path), "--year", str(year)]
    merge_log = log_dir / f"{collector}_merge.log"
    print(f"\n🔄 병합 시작: {' '.join(merge_cmd)} (로그: {merge_log})")

    proc = None
    try:
        with open(merge_log, "w", encoding="utf-8") as f:
            proc = subprocess.Popen(
                merge_cmd,
                env=make_env(),
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )
            with lock:
                processes.append(proc)

            if timeout is not None:
                try:
                    proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    print(f"⏰ 병합 타임아웃({timeout}초) 초과, 강제 종료 중...")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
            else:
                proc.wait()

            return proc.returncode == 0
    except Exception as e:
        print(f"❌ 병합 예외 발생: {e}")
        return False
    finally:
        if proc is not None:
            with lock:
                if proc in processes:
                    processes.remove(proc)

def run_shard(region: str, shard: str, collector: str, extra_args: List[str], year: int, timeout: Optional[int] = None) -> Tuple[str, str, int]:
    """collector_cli.py를 통해 단일 지역-샤드 실행"""
    log_file = LOG_DIR / f"{collector}_{region}_{shard}.log"
    cmd = [
        sys.executable, "collector_cli.py", collector,
        "--regions", region,
        "--shard", shard,
        "--year", str(year)
    ] + extra_args

    proc = None
    ret_code = 1

    try:
        with open(log_file, "w", encoding="utf-8") as f:
            proc = subprocess.Popen(
                cmd,
                env=make_env(),
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )
            with lock:
                processes.append(proc)

            if timeout is not None:
                try:
                    ret_code = proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    print(f"⏰ {region} {shard} 타임아웃({timeout}초) 초과, 강제 종료 중...")
                    proc.terminate()
                    try:
                        ret_code = proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        ret_code = proc.wait()
            else:
                ret_code = proc.wait()
    except Exception as e:
        print(f"❌ {region} {shard} 예외: {e}")
        ret_code = 1
    finally:
        if proc is not None:
            with lock:
                if proc in processes:
                    processes.remove(proc)

    return region, shard, ret_code

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="병렬 샤드 실행기 + 자동 병합 (collector_cli.py 기반)")
    parser.add_argument("collector", help="콜렉터 이름 (neis_info, school_info, meal, timetable, schedule 등)")
    parser.add_argument("--year", type=int, help="학년도 (미지정 시 프롬프트)")
    parser.add_argument("--timeout", type=int, default=None, help="각 샤드/병합의 최대 실행 시간(초)")
    parser.add_argument("--regions", default="ALL", help="교육청 코드 (쉼표 구분, 기본: ALL)")
    parser.add_argument("--quiet", action="store_true", help="출력 최소화")
    args, remaining = parser.parse_known_args()
    extra_args = list(remaining)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    # GitHub Actions 환경에서는 자동으로 quiet 모드
    if os.getenv('GITHUB_ACTIONS') == 'true' and not args.quiet:
        args.quiet = True

    # extra_args 에서 --year, --shard, --regions 등이 포함되어 있다면 제거 (중복 방지)
    filtered_args = []
    skip_next = False
    for i, arg in enumerate(extra_args):
        if skip_next:
            skip_next = False
            continue
        if arg in ("--year", "-y", "--shard", "-s", "--regions", "-r"):
            if i + 1 < len(extra_args) and not extra_args[i+1].startswith("-"):
                skip_next = True
            continue
        filtered_args.append(arg)
    extra_args = filtered_args

    if args.year is not None:
        if not validate_year(args.year):
            current = get_current_school_year()
            print(f"❌ --year 값이 유효하지 않습니다. (2000~{current} 사이여야 함)")
            sys.exit(1)
        year = args.year
    else:
        year = prompt_year(get_current_school_year())

    regions = parse_regions(args.regions)
    if not args.quiet:
        print(f"📌 대상 지역: {regions} (총 {len(regions)}개)")

    # collector_cli.py 존재 여부 확인
    if not Path("collector_cli.py").exists():
        print("❌ collector_cli.py 파일을 찾을 수 없습니다. (프로젝트 루트에 있어야 함)")
        sys.exit(1)

    log_dir = LOG_DIR
    log_dir.mkdir(exist_ok=True)
    cleanup_old_logs(log_dir, args.collector, days=7)

    # 모든 지역-샤드 조합 생성
    tasks = []
    for region in regions:
        for shard in ("odd", "even"):
            tasks.append((region, shard))

    total_tasks = len(tasks)
    if not args.quiet:
        print(f"🚀 총 {total_tasks}개 작업을 병렬로 실행합니다.")

    # Rich progress bar 설정
    if RICH_AVAILABLE and not args.quiet:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=Console(),
            transient=False,
        )
        progress.start()
        task_ids = {}
        for region, shard in tasks:
            desc = f"{region}-{shard}"
            task_ids[(region, shard)] = progress.add_task(desc, total=100)
    else:
        progress = None
        task_ids = None

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
        future_to_task = {
            executor.submit(run_shard, region, shard, args.collector, extra_args, year, args.timeout): (region, shard)
            for region, shard in tasks
        }
        completed = 0
        for future in as_completed(future_to_task):
            region, shard, ret_code = future.result()
            completed += 1
            if progress and task_ids:
                progress.update(task_ids[(region, shard)], completed=100)
            elif not args.quiet and not RICH_AVAILABLE:
                simple_progress(completed, total_tasks)
            with lock:
                results[(region, shard)] = ret_code

    if progress:
        progress.stop()
    elif not args.quiet and not RICH_AVAILABLE:
        print()

    # 결과 집계
    failed = [(r, s) for (r, s), code in results.items() if code != 0]
    success_count = len(results) - len(failed)

    print("\n" + "=" * 50)
    print(f"📊 결과 요약: 성공 {success_count}/{len(tasks)}, 실패 {len(failed)}")
    if failed:
        print("❌ 실패한 작업:")
        for r, s in failed:
            print(f"  - {r} {s} (로그: logs/{args.collector}_{r}_{s}.log)")

    if len(failed) == 0:
        print("🎉 모든 지역/샤드 수집 완료!")

        if run_merge(args.collector, year, log_dir, timeout=args.timeout):
            print("\n✅ 전체 파이프라인(수집 + 병합) 성공적으로 완료되었습니다.")
            sys.exit(0)
        else:
            print(f"\n⚠️ 수집은 성공했으나 병합에 실패했습니다.")
            print(f"👉 병합 로그: logs/{args.collector}_merge.log")
            sys.exit(1)
    else:
        print(f"\n⚠️ 일부 작업이 실패했습니다. 병합을 건너뜁니다.")
        sys.exit(1)

if __name__ == "__main__":
    main()
    