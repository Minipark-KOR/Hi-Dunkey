#!/usr/bin/env python3
"""
병렬 처리 관련 공통 유틸리티
- 적정 worker 수 계산 (CPU 코어, API rate limit, 절대 상한)
- 안전한 Pool 생성 (maxtasksperchild 적용)
"""
import os
import multiprocessing
from typing import Optional

def optimal_worker_count(total_jobs: int,
                         max_by_api: int = 10,
                         cpu_factor: float = 1.0,
                         absolute_max: int = 16) -> int:
    """
    적정 worker 수 계산
    - total_jobs: 전체 작업 수
    - max_by_api: API rate limit에 따른 최대 worker 수 (기본 10)
    - cpu_factor: CPU 코어 수에 곱할 계수 (기본 1.0)
    - absolute_max: 절대 상한 (기본 16, SQLite 락 경합 고려)
    
    NEIS API 공식 제한: 초당 50회, 각 프로세스 요청 간격 0.05~0.1초 가정 시
    적정 worker 수는 8~12 범위 (네트워크 환경에 따라 유동적)
    """
    cpu_cores = os.cpu_count() or 4
    cpu_limit = max(1, int(cpu_cores * cpu_factor))
    
    # 작업 수, CPU 제한, API 제한, 절대 상한 중 최소값 (최소 1 보장)
    return max(1, min(total_jobs, cpu_limit, max_by_api, absolute_max))

def setup_worker_pool(total_jobs: int, max_workers: Optional[int] = None):
    """
    worker pool 생성
    - total_jobs: 전체 작업 수
    - max_workers: 직접 지정한 worker 수 (None이면 자동 계산)
    
    반환: multiprocessing.Pool 객체 (with 문 사용 권장)
    """
    workers = max_workers if max_workers is not None else optimal_worker_count(total_jobs)
    
    print(f"🚀 [Parallel] {workers}개 프로세스로 병렬 실행 (총 {total_jobs}개 작업)")
    
    # maxtasksperchild: 프로세스당 20개 작업 후 재생성 → 메모리 누수 방지
    return multiprocessing.Pool(processes=workers, maxtasksperchild=20)
    