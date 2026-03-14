#!/usr/bin/env python3
# core/collector_stats.py
# 배치 처리 메트릭스 수집 및 요약 (min 값 안전 처리)

import time
import threading
from typing import Dict, Any, Optional
import logging


class CollectorStats:
    """수집기 배치 단위 통계 (배치 크기, 처리 시간, 성공/실패)"""

    def __init__(self, collector_name: str, logger: Optional[logging.Logger] = None):
        self.name = collector_name
        self.logger = logger or logging.getLogger(collector_name)

        self.lock = threading.Lock()
        self.total_batches = 0
        self.total_rows = 0
        self.failed_batches = 0
        self.total_time = 0.0
        self.max_batch_size = 0
        self.min_batch_size = float('inf')
        self.max_batch_time = 0.0
        self.min_batch_time = float('inf')
        self.start_time = time.time()

    def update(self, row_count: int, elapsed: float, success: bool):
        """배치 저장 후 호출"""
        with self.lock:
            self.total_batches += 1
            self.total_rows += row_count
            self.total_time += elapsed
            if not success:
                self.failed_batches += 1

            if row_count > self.max_batch_size:
                self.max_batch_size = row_count
            if row_count < self.min_batch_size:
                self.min_batch_size = row_count

            if elapsed > self.max_batch_time:
                self.max_batch_time = elapsed
            if elapsed < self.min_batch_time:
                self.min_batch_time = elapsed

    def get_summary(self) -> Dict[str, Any]:
        """현재까지 누적된 통계 반환"""
        with self.lock:
            elapsed_total = time.time() - self.start_time
            has_batches = self.total_batches > 0

            avg_batch_size = self.total_rows / self.total_batches if has_batches else 0
            avg_time_per_batch = self.total_time / self.total_batches if has_batches else 0
            rows_per_second = self.total_rows / elapsed_total if elapsed_total > 0 else 0

            return {
                "collector": self.name,
                "elapsed_seconds": round(elapsed_total, 2),
                "total_batches": self.total_batches,
                "total_rows": self.total_rows,
                "failed_batches": self.failed_batches,
                "success_rate": round((self.total_batches - self.failed_batches) / self.total_batches * 100, 2) if has_batches else 0,
                "avg_batch_size": round(avg_batch_size, 2),
                "min_batch_size": self.min_batch_size if has_batches and self.min_batch_size != float('inf') else 0,
                "max_batch_size": self.max_batch_size,
                "avg_time_per_batch": round(avg_time_per_batch, 3),
                "min_batch_time": round(self.min_batch_time, 3) if has_batches and self.min_batch_time != float('inf') else 0,
                "max_batch_time": round(self.max_batch_time, 3),
                "rows_per_second": round(rows_per_second, 2),
            }

    def log_summary(self, level=logging.INFO):
        """통계를 로그로 출력"""
        summary = self.get_summary()
        lines = [
            f"📊 [{self.name}] 배치 처리 통계",
            f"   ⏱️  경과 시간: {summary['elapsed_seconds']}초",
            f"   📦 배치 수: {summary['total_batches']} (실패: {summary['failed_batches']}, 성공률: {summary['success_rate']}%)",
            f"   📋 처리 행: {summary['total_rows']:,}행",
            f"   📏 평균 배치 크기: {summary['avg_batch_size']} (최소 {summary['min_batch_size']}, 최대 {summary['max_batch_size']})",
            f"   ⏱️  평균 배치 시간: {summary['avg_time_per_batch']}초 (최소 {summary['min_batch_time']}초, 최대 {summary['max_batch_time']}초)",
            f"   🚀 처리 속도: {summary['rows_per_second']}행/초",
        ]
        for line in lines:
            self.logger.log(level, line)
            