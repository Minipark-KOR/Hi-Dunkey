#!/usr/bin/env python3
"""
병렬 처리 관련 공통 유틸리티
"""
import os
import multiprocessing
from typing import Optional

def optimal_worker_count(total_jobs: int,
                         max_by_api: int = 10,
                         cpu_factor: float = 1.0,
                         absolute_max: int = 16) -> int:
    cpu_cores = os.cpu_count() or 4
    cpu_limit = max(1, int(cpu_cores * cpu_factor))
    return max(1, min(total_jobs, cpu_limit, max_by_api, absolute_max))

def setup_worker_pool(total_jobs: int, max_workers: Optional[int] = None):
    workers = max_workers if max_workers is not None else optimal_worker_count(total_jobs)
    print(f"🚀 [Parallel] {workers}개 프로세스로 병렬 실행 (총 {total_jobs}개 작업)")
    return multiprocessing.Pool(processes=workers, maxtasksperchild=20)