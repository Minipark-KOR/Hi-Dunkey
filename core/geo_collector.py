#!/usr/bin/env python3
"""
collectors/geo_collector.py

위치정보 수집기 (Geo Collector)
- VWorld geocode + 캐시 + schools 좌표 업데이트
- AddressFilter 기반 정제 레벨(1~3) 재시도
- 실패 시 RetryManager에 등록 (재시도 시스템 활용)
"""
import os
import sys
import time
import hashlib
import sqlite3
import argparse
from typing import Optional, Dict, Tuple
from datetime import datetime, time as dt_time

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kst_time import now_kst
from core.meta_vocab import MetaVocabManager
from core.filters import AddressFilter
from core.retry import RetryManager

try:
    from core.database import get_db_connection
except ImportError:
    def get_db_connection(path: str):
        return sqlite3.connect(path, timeout=30)

def _get_vworld_key() -> str:
    key = os.environ.get("VWORLD_API_KEY", "").strip()
    if key:
        return key
    try:
        from constants.codes import VWORLD_API_KEY as CONST_KEY
        return (CONST_KEY or "").strip()
    except Exception:
        return ""


class GeoCollector:
    GEOCODE_URL = "https://api.vworld.kr/req/address"
    DAILY_API_LIMIT = 50000

    def __init__(
        self,
        global_db_path: str = "data/global_vocab.db",
        school_db_path: str = "data/master/school_info.db",
        failures_db_path: str = "data/failures.db",
        debug_mode: bool = False,
        api_limit: Optional[int] = None,
    ):
        self.global_db_path = global_db_path
        self.school_db_path = school_db_path
        self.debug_mode = debug_mode

        self.vworld_key = _get_vworld_key()
        self.api_limit = api_limit if api_limit is not None else self.DAILY_API_LIMIT

        self.meta_vocab = MetaVocabManager(global_db_path, debug_mode)
        self.retry_mgr = RetryManager(db_path=failures_db_path)

        self.cache: Dict[str, Tuple[float, float]] = {}
        self.pending_inserts = []

        self.api_calls_today = 0
        self._usage_dirty = 0
        self._usage_date = now_kst().strftime("%Y-%m-%d")

        self._init_tables()
        self._load_cache()
        self._load_api_usage()

        if self.debug_mode:
            print(f"[GeoCollector] init: api_calls_today={self.api_calls_today}/{self.api_limit}")

    def _init_tables(self):
        os.makedirs(os.path.dirname(self.global_db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.school_db_path), exist_ok=True)

        with get_db_connection(self.global_db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_cache (
                    address_hash TEXT PRIMARY KEY,
                    original_address TEXT NOT NULL,
                    longitude REAL,
                    latitude REAL,
                    confidence TEXT,
                    last_queried TEXT,
                    query_count INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geo_coords ON geo_cache(longitude, latitude)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_geo_queried ON geo_cache(last_queried)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS geo_api_usage (
                    date TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # schools 테이블에 필요한 컬럼들 (migrate에서 이미 했다면 생략 가능)
        if os.path.exists(self.school_db_path):
            with sqlite3.connect(self.school_db_path, timeout=30) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout=30000;")
                cur = conn.execute("PRAGMA table_info(schools)")
                existing = [row[1] for row in cur.fetchall()]

                new_columns = [
                    ("cleaned_address", "TEXT"),
                    ("geocode_attempts", "INTEGER DEFAULT 0"),
                    ("last_error", "TEXT"),
                    ("city_id", "INTEGER"),
                    ("district_id", "INTEGER"),
                    ("street_id", "INTEGER"),
                    ("number_type", "TEXT"),
                    ("number_value", "INTEGER"),
                    ("number_start", "INTEGER"),
                    ("number_end", "INTEGER"),
                    ("number_bit", "INTEGER"),
                ]
                for col, typ in new_columns:
                    if col not in existing:
                        try:
                            conn.execute(f"ALTER TABLE schools ADD COLUMN {col} {typ}")
                        except sqlite3.OperationalError:
                            pass

                conn.execute("UPDATE schools SET geocode_attempts = 0 WHERE geocode_attempts IS NULL")
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_schools_missing "
                    "ON schools(latitude) WHERE latitude IS NULL"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_schools_attempts "
                    "ON schools(geocode_attempts) WHERE latitude IS NULL"
                )

    def _hash_address(self, address: str) -> str:
        return hashlib.sha256(address.encode("utf-8")).hexdigest()[:16]

    def _load_cache(self):
        try:
            with get_db_connection(self.global_db_path) as conn:
                cur = conn.execute(
                    "SELECT original_address, longitude, latitude FROM geo_cache "
                    "WHERE longitude IS NOT NULL AND latitude IS NOT NULL"
                )
                for addr, lon, lat in cur:
                    self.cache[self._hash_address(addr)] = (float(lon), float(lat))
            if self.debug_mode:
                print(f"[GeoCollector] cache loaded: {len(self.cache)}")
        except Exception as e:
            if self.debug_mode:
                print(f"[GeoCollector] cache load failed: {e}")

    def _load_api_usage(self):
        today = now_kst().strftime("%Y-%m-%d")
        self._usage_date = today
        try:
            with get_db_connection(self.global_db_path) as conn:
                cur = conn.execute("SELECT count FROM geo_api_usage WHERE date = ?", (today,))
                row = cur.fetchone()
                self.api_calls_today = int(row[0]) if row else 0
        except Exception as e:
            if self.debug_mode:
                print(f"[GeoCollector] api usage load failed: {e}")
            self.api_calls_today = 0

    def _persist_usage_if_needed(self, force: bool = False):
        if self._usage_dirty <= 0 and not force:
            return
        today = now_kst().strftime("%Y-%m-%d")
        if today != self._usage_date:
            self._usage_date = today
            self._usage_dirty = 0
            self._load_api_usage()
            return
        try:
            with get_db_connection(self.global_db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO geo_api_usage(date, count, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(date) DO UPDATE SET
                        count = count + excluded.count,
                        updated_at = excluded.updated_at
                    """,
                    (today, int(self._usage_dirty), now_kst().isoformat()),
                )
            self._usage_dirty = 0
        except Exception as e:
            if self.debug_mode:
                print(f"[GeoCollector] api usage persist failed: {e}")

    def _bump_api_usage(self, n: int = 1):
        self.api_calls_today += n
        self._usage_dirty += n
        if self._usage_dirty >= 100:
            self._persist_usage_if_needed(force=True)

    def _check_api_limit(self) -> bool:
        if self.api_calls_today >= self.api_limit:
            if self.debug_mode:
                print(f"[GeoCollector] API limit exceeded: {self.api_calls_today}/{self.api_limit}")
            return False
        return True

    def _geocode(self, address: str) -> Optional[Tuple[float, float]]:
        if not address or not self.vworld_key:
            return None

        addr_hash = self._hash_address(address)
        if addr_hash in self.cache:
            return self.cache[addr_hash]

        if not self._check_api_limit():
            return None

        kind = AddressFilter.classify(address)
        if kind == "road":
            type_order = ["ROAD", "PARCEL", "JIBUN"]
        elif kind == "jibun":
            type_order = ["PARCEL", "JIBUN", "ROAD"]
        else:
            type_order = ["ROAD", "PARCEL", "JIBUN"]

        for addr_type in type_order:
            if not self._check_api_limit():
                break
            params = {
                "service": "address",
                "request": "getcoord",
                "version": "2.0",
                "crs": "epsg:4326",
                "address": address,
                "refine": "true",
                "simple": "false",
                "format": "json",
                "type": addr_type,
                "key": self.vworld_key,
            }
            try:
                resp = requests.get(self.GEOCODE_URL, params=params, timeout=10)
                self._bump_api_usage(1)

                if resp.status_code == 429:
                    time.sleep(2)
                    continue

                data = resp.json()
                status = data.get("response", {}).get("status")
                if status == "OK":
                    point = data["response"]["result"]["point"]
                    lon = float(point["x"])
                    lat = float(point["y"])
                    self.cache[addr_hash] = (lon, lat)
                    confidence = data["response"]["result"].get("confidence", "UNKNOWN")
                    self._save_to_cache(address, lon, lat, confidence)
                    return (lon, lat)
                if status == "LIMIT_EXCEEDED":
                    self.api_calls_today = self.api_limit
                    self._persist_usage_if_needed(force=True)
                    return None
                time.sleep(0.05)
            except Exception as e:
                if self.debug_mode:
                    print(f"[GeoCollector] geocode error({addr_type}): {e}")
                time.sleep(0.2)
        return None

    def _save_to_cache(self, address: str, lon: float, lat: float, confidence: str = "UNKNOWN"):
        self.pending_inserts.append((address, lon, lat, confidence))
        if len(self.pending_inserts) >= 10:
            self.flush()

    def flush(self):
        if not self.pending_inserts:
            self._persist_usage_if_needed(force=False)
            return
        try:
            with get_db_connection(self.global_db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout=30000;")
                now = now_kst().isoformat()
                for address, lon, lat, confidence in self.pending_inserts:
                    addr_hash = self._hash_address(address)
                    cur = conn.execute("SELECT query_count FROM geo_cache WHERE address_hash = ?", (addr_hash,))
                    row = cur.fetchone()
                    if row:
                        conn.execute(
                            """
                            UPDATE geo_cache
                            SET query_count = query_count + 1,
                                last_queried = ?,
                                longitude = ?,
                                latitude = ?,
                                confidence = ?
                            WHERE address_hash = ?
                            """,
                            (now, lon, lat, confidence, addr_hash),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO geo_cache (address_hash, original_address, longitude, latitude, confidence, last_queried)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (addr_hash, address, lon, lat, confidence, now),
                        )
            self.pending_inserts.clear()
            self._persist_usage_if_needed(force=False)
        except Exception as e:
            print(f"[GeoCollector] cache flush failed: {e}")

    def _update_school_coords(self, sc_code: str, lon: float, lat: float, cleaned: str, addr_components: dict):
        """좌표 성공 시 schools 테이블 업데이트 (재시도 핸들러에서도 사용)"""
        with sqlite3.connect(self.school_db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute(
                """
                UPDATE schools
                SET longitude = ?, latitude = ?,
                    cleaned_address = ?,
                    geocode_attempts = 0,
                    last_error = NULL,
                    city_id = ?, district_id = ?, street_id = ?,
                    number_type = ?, number_value = ?, number_start = ?, number_end = ?, number_bit = ?
                WHERE sc_code = ?
                """,
                (
                    lon, lat, cleaned,
                    addr_components.get("city_id", 0),
                    addr_components.get("district_id", 0),
                    addr_components.get("street_id", 0),
                    addr_components.get("number_type"),
                    addr_components.get("number"),
                    addr_components.get("number_start"),
                    addr_components.get("number_end"),
                    addr_components.get("number_bit"),
                    sc_code,
                ),
            )

    def save_location(self, domain: str, item_id: str, address: str) -> Dict:
        """
        학교 위치 정보 저장 (실패 시 RetryManager에 등록)
        """
        if domain != "school" or not address:
            return {"error": "Invalid domain or empty address"}

        if not os.path.exists(self.school_db_path):
            return {"error": f"School DB not found: {self.school_db_path}"}

        # 현재 좌표 확인
        with sqlite3.connect(self.school_db_path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT longitude, latitude FROM schools WHERE sc_code = ?",
                (item_id,),
            ).fetchone()
            if row and row["longitude"] is not None and row["latitude"] is not None:
                return {"coords": (row["longitude"], row["latitude"]), "status": "existing"}

        # 정제
        cleaned = AddressFilter.clean(address, level=1)  # 기본 정제, 실제 재시도 핸들러에서 레벨 조정 가능
        coords = None
        error_msg = None
        try:
            coords = self._geocode(cleaned)
        except Exception as e:
            error_msg = str(e)[:200]

        addr_components = self.meta_vocab.save_address(cleaned)

        if coords:
            lon, lat = coords
            self._update_school_coords(item_id, lon, lat, cleaned, addr_components)
            status = "success"
        else:
            # 실패 시 RetryManager에 등록 (오늘 15시 데드라인)
            deadline = datetime.combine(now_kst().date(), dt_time(15, 0))
            self.retry_mgr.record_failure(
                domain="school",
                task_type="geocode",
                sc_code=item_id,
                address=address,
                error=error_msg or "geocode failed",
                deadline=deadline,
            )
            status = "failed"

        return {
            "coords": coords,
            "status": status,
            "error": error_msg,
        }

    def batch_update_schools(self, limit: int = 100):
        """기존 방식으로 일괄 처리 (초기 누락 학교 처리용)"""
        with sqlite3.connect(self.school_db_path) as conn:
            schools = conn.execute(
                """
                SELECT sc_code, sc_name, address
                FROM schools
                WHERE address IS NOT NULL AND address != ''
                  AND (latitude IS NULL OR longitude IS NULL)
                ORDER BY RANDOM()
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        print(f"처리할 학교: {len(schools)}개")
        success = 0
        for i, (sc_code, sc_name, address) in enumerate(schools, 1):
            result = self.save_location("school", sc_code, address)
            if result.get("coords"):
                success += 1
            print(f"\r{i}/{len(schools)} 성공:{success}", end="")
            time.sleep(0.2)
            if i % 10 == 0:
                self.flush()
                self.meta_vocab.flush()
        print()
        self.flush()
        self.meta_vocab.flush()
        return success


def main():
    parser = argparse.ArgumentParser(description="GeoCollector CLI")
    parser.add_argument("--schools", action="store_true", help="학교 좌표 배치 업데이트 실행")
    parser.add_argument("--limit", type=int, default=100, help="배치 처리 개수")
    parser.add_argument("--debug", action="store_true", help="디버그 로그")
    parser.add_argument("--global-db", default="data/global_vocab.db")
    parser.add_argument("--school-db", default="data/master/school_info.db")
    parser.add_argument("--failures-db", default="data/failures.db")
    args = parser.parse_args()

    gc = GeoCollector(
        global_db_path=args.global_db,
        school_db_path=args.school_db,
        failures_db_path=args.failures_db,
        debug_mode=args.debug,
    )

    if args.schools:
        gc.batch_update_schools(limit=args.limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
    