#!/usr/bin/env python3
# 개발 가이드: docs/developer_guide.md 참조
"""
병렬 샤드 실행기 + 자동 병합 + 연도 프롬프트 + 로그 정리 (collector_cli.py 기반)
사용법:
    python scripts/run_pipeline.py <collector_name> [--year YYYY] [--timeout 초] [추가 인자...]
예:
    python scripts/run_pipeline.py neis_info --regions ALL
    python scripts/run_pipeline.py neis_info --year 2025 --regions ALL --debug
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
from typing import Optional, List

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

def make_env() -> dict:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f".{os.pathsep}{existing}" if existing else "."
    return env

def cleanup_old_logs(log_dir: Path, collector: str, days: int = 7):
    cutoff = datetime.now() - timedelta(days=days)
    for log_file in log_dir.glob(f"{collector}_*.log"):
        try:
            if datetime.fromtimestamp(log_file.stat().st_mtime) < cutoff:
                log_file.unlink()
                print(f"🧹 오래된 로그 삭제: {log_file.name}")
        except Exception:
            pass

# 전역 상태 관리
running = {"odd": True, "even": True}
lock = threading.Lock()
results = {"odd": None, "even": None}
processes = []  # 실행 중인 Popen 객체 리스트

def signal_handler(sig, frame):
    print(f"\n[{signal.Signals(sig).name}] 종료 신호 수신. 자식 프로세스 정리 중...")
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

def run_shard(shard: str, collector: str, extra_args: List[str], year: int, timeout: Optional[int] = None):
    """collector_cli.py를 통해 단일 샤드 실행"""
    log_file = Path("logs") / f"{collector}_{shard}.log"
    # collector_cli.py에 전달할 기본 인자
    cmd = [
        sys.executable, "collector_cli.py", collector,
        "--shard", shard,
        "--year", str(year)
    ] + extra_args

    print(f"🚀 {shard} 시작 (로그: {log_file})")
    start_time = time.time()
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
                    print(f"⏰ {shard} 타임아웃({timeout}초) 초과, 강제 종료 중...")
                    proc.terminate()
                    try:
                        ret_code = proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        ret_code = proc.wait()
            else:
                ret_code = proc.wait()
    except Exception as e:
        print(f"❌ {shard} 예외: {e}")
        ret_code = 1
    finally:
        if proc is not None:
            with lock:
                if proc in processes:
                    processes.remove(proc)

    elapsed = time.time() - start_time
    with lock:
        results[shard] = ret_code
        running[shard] = False
        other = "even" if shard == "odd" else "odd"
        if ret_code == 0:
            status = f" — {other} 진행 중" if running[other] else ""
            print(f"✅ {shard} 수집완료 ({elapsed:.1f}초){status}")
        else:
            if ret_code >= 0:
                print(f"❌ {shard} 실패 (코드 {ret_code}) - 로그: {log_file}")

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="병렬 샤드 실행기 + 자동 병합 (collector_cli.py 기반)")
    parser.add_argument("collector", help="콜렉터 이름 (neis_info, school_info, meal, timetable, schedule 등)")
    parser.add_argument("--year", type=int, help="학년도 (미지정 시 프롬프트)")
    parser.add_argument("--timeout", type=int, default=None, help="각 샤드/병합의 최대 실행 시간(초)")
    args, remaining = parser.parse_known_args()
    extra_args = list(remaining)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    # extra_args 에서 --year 또는 --shard 가 포함되어 있다면 제거 (중복 방지)
    filtered_args = []
    skip_next = False
    for i, arg in enumerate(extra_args):
        if skip_next:
            skip_next = False
            continue
        if arg in ("--year", "-y", "--shard", "-s"):
            # 다음 인자도 건너뜀 (값이 있다면)
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

    # collector_cli.py 존재 여부 확인 (선택 사항)
    if not Path("collector_cli.py").exists():
        print("❌ collector_cli.py 파일을 찾을 수 없습니다. (프로젝트 루트에 있어야 함)")
        sys.exit(1)

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    cleanup_old_logs(log_dir, args.collector, days=7)

    threads = []
    for shard in ("odd", "even"):
        t = threading.Thread(target=run_shard, args=(shard, args.collector, extra_args, year, args.timeout))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    odd_ok = results.get("odd") == 0
    even_ok = results.get("even") == 0

    print("\n" + "=" * 50)
    if odd_ok and even_ok:
        print("🎉 모든 샤드 수집 완료! (odd ✅, even ✅)")
        print(f"👉 수집 로그: logs/{args.collector}_odd.log, logs/{args.collector}_even.log")

        if run_merge(args.collector, year, log_dir, timeout=args.timeout):
            print("\n✅ 전체 파이프라인(수집 + 병합) 성공적으로 완료되었습니다.")
            sys.exit(0)
        else:
            print(f"\n⚠️ 수집은 성공했으나 병합에 실패했습니다.")
            print(f"👉 병합 로그: logs/{args.collector}_merge.log")
            sys.exit(1)
    else:
        print("⚠️ 일부 샤드가 실패했거나 중단되었습니다.")
        print(f"👉 로그 확인: logs/{args.collector}_odd.log, logs/{args.collector}_even.log")
        sys.exit(1)

if __name__ == "__main__":
    main()
    