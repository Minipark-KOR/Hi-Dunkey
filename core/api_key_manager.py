#!/usr/bin/env python3
"""
멀티 API 키 관리 (로드밸런싱, 페일오버, 통계)
"""
import time
import threading
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

@dataclass
class APIKey:
    key: str
    rate_limit_per_sec: float = 1000.0
    daily_limit: Optional[int] = None
    used_today: int = 0
    last_reset: float = field(default_factory=time.time)
    failed_count: int = 0
    is_active: bool = True
    expiry_date: Optional[str] = None

    def reset_if_needed(self):
        now = time.time()
        if now - self.last_reset > 86400:
            self.used_today = 0
            self.last_reset = now

    def can_use(self) -> bool:
        self.reset_if_needed()
        return self.is_active and (self.daily_limit is None or self.used_today < self.daily_limit)


class APIKeyManager:
    def __init__(self, keys: List[str], rate_limits: Optional[List[float]] = None,
                 daily_limits: Optional[List[int]] = None):
        self.keys = []
        for i, key in enumerate(keys):
            rate = rate_limits[i] if rate_limits and i < len(rate_limits) else 1000.0
            daily = daily_limits[i] if daily_limits and i < len(daily_limits) else None
            self.keys.append(APIKey(key=key, rate_limit_per_sec=rate, daily_limit=daily))
        self.counter = 0
        self._lock = threading.Lock()

    def get_key(self) -> Optional[APIKey]:
        with self._lock:
            start = self.counter
            for _ in range(len(self.keys)):
                idx = self.counter % len(self.keys)
                self.counter += 1
                key = self.keys[idx]
                if key.can_use():
                    return key
                if self.counter - start >= len(self.keys):
                    break
            return None

    def report_success(self, key: APIKey):
        key.used_today += 1
        key.failed_count = 0

    def report_failure(self, key: APIKey):
        key.failed_count += 1
        if key.failed_count >= 5:
            key.is_active = False

    def get_stats(self) -> List[Dict[str, Any]]:
        return [
            {
                "key_prefix": k.key[:6] + "...",
                "used_today": k.used_today,
                "failed_count": k.failed_count,
                "is_active": k.is_active,
                "daily_limit": k.daily_limit,
            }
            for k in self.keys
        ]

    def log_stats(self, logger=None):
        if logger is None:
            import logging
            logger = logging.getLogger("key_manager")
        for stat in self.get_stats():
            logger.info(f"Key {stat['key_prefix']}: used={stat['used_today']}, fails={stat['failed_count']}, active={stat['is_active']}")
            