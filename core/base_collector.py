#!/usr/bin/env python3
"""
학교 기본정보 수집기 (Diff 기반 좌표 갱신)
- GeoCollector 통합으로 캐시 및 API 사용량 추적
- 기본 모드에서도 지역별 좌표 현황 출력
- 전체 수집 완료 후 누적 통계 표시 (성공률 포함)
- GitHub Actions 환경에서는 자동으로 quiet 모드
- 진행률에 [LIMIT:xx] 표시 추가
- 지번 주소 (jibun_address) 추출 및 저장
"""
import os
import sys
import time
import sqlite3
import argparse
from typing import List, Dict, Tuple, Optional
from datetime import timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from core.base_collector import BaseCollector
from core.database import get_db_connection, get_db_reader
from core.school_id import create_school_id
from core.meta_vocab import MetaVocabManager
from core.filters import AddressFilter
from core.kst_time import now_kst
from constants.codes import NEIS_ENDPOINTS, ALL_REGIONS, REGION_NAMES
from constants.paths import MASTER_DB_PATH as MASTER_DB, MASTER_DIR
from collectors.geo_collector import GeoCollector

BASE_DIR = str(MASTER_DIR)
GLOBAL_VOCAB_PATH = str(MASTER_DIR.parent / "active" / "global_vocab.db")
NEIS_URL = NEIS_ENDPOINTS['school']

# ✅ ANSI 색상 코드
GREEN, RED, YELLOW, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[0m"


class NeisInfoCollector(BaseCollector):
    LEVEL_GEOCODING = 3
    LEVEL_FINAL = 4

    def __init__(
        self,
        shard: str = "none",
        school_range=None,
        incremental: bool = False,
        full: bool = False,
        compare: bool = False,
        debug_mode: bool = False,
        quiet_mode: bool = False
    ):
        # ✅ 수정: collector name을 "neis_info"로, base_dir을 MASTER_DIR로 변경
        super().__init__("neis_info", str(MASTER_DIR), shard, school_range)
        self.db_path = str(MASTER_DB)          # 통합 DB 경로는 그대로 유지
        self.api_context = 'school'
        self.incremental = incremental
        self.full = full
        self.compare = compare
        self.debug_mode = debug_mode
        self.quiet_mode = quiet_mode
        self.run_date = now_kst().strftime("%Y%m%d")

        self.meta_vocab = self.register_resource(
            MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        )
        self.geo_collector = self.register_resource(
            GeoCollector(
                global_db_path=GLOBAL_VOCAB_PATH,
                school_db_path=self.db_path,
                failures_db_path="data/failures.db",
                debug_mode=debug_mode,
            )
        )

        # ✅ 누적 통계를 위한 변수
        self.total_new = 0
        self.total_failed = 0
        self.total_skipped = 0

        if not quiet_mode:
            print("🏫 NeisInfoCollector 초기화 완료")
        self.logger.info("🏫 NeisInfoCollector 초기화 완료")

    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schools (
                    sc_code TEXT PRIMARY KEY,
                    school_id INTEGER,
                    sc_name TEXT,
                    eng_name TEXT,
                    sc_kind TEXT,
                    atpt_code TEXT,
                    address TEXT,
                    cleaned_address TEXT,
                    address_hash TEXT,
                    tel TEXT,
                    homepage TEXT,
                    status TEXT DEFAULT '운영',
                    last_seen INTEGER,
                    load_dt TEXT,
                    latitude REAL,
                    longitude REAL,
                    geocode_attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    city_id INTEGER,
                    district_id INTEGER,
                    street_id INTEGER,
                    number_type TEXT,
                    number_value INTEGER,
                    number_start INTEGER,
                    number_end INTEGER,
                    number_bit INTEGER,
                    kakao_address TEXT,
                    jibun_address TEXT
                )
            """)
            for idx in ["idx_address_hash","idx_status","idx_city","idx_district","idx_street",
                        "idx_schools_missing","idx_schools_region","idx_schools_coords"]:
                conn.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON schools({idx.split('_',2)[-1] if idx!='idx_schools_missing' else 'latitude'})")
            self._init_db_common(conn)

    def _get_target_key(self) -> str:
        return self.run_date

    def fetch_region(self, region_code: str, limit: Optional[int] = None, **kwargs):
        region_name = REGION_NAMES.get(region_code, region_code)
        if self.debug_mode and not self.quiet_mode:
            print(f"\n📡 [{region_name}({region_code})] 데이터 수집 시작...")
        base_params = {"ATPT_OFCDC_SC_CODE": region_code}
        rows = self._fetch_paginated(
            NEIS_URL, base_params, 'NeisInfo',
            page_size=100,
            region=region_code,
            year=int(self.run_date[:4])
        )
        if not rows:
            self.logger.error(f"[{region_name}] 수집된 데이터 없음")
            if self.debug_mode and not self.quiet_mode:
                print(f"❌ [{region_name}] 수집된 데이터 없음")
            return
        self.logger.info(f"[{region_name}] 전체 {len(rows)}건 수집")
        if self.debug_mode and not self.quiet_mode:
            print(f"📋 [{region_name}] 전체 {len(rows)}건 수집")
        new, failed, skipped = self._update_schools_with_diff(rows, region_code, limit=limit)

        # ✅ 누적 통계 업데이트
        self.total_new += new
        self.total_failed += failed
        self.total_skipped += skipped

        # ✅ 지역별 결과 출력 (quiet 모드가 아니면 항상 표시)
        if not self.quiet_mode:
            print(f"  📊 [{region_name}] 신규성공={new}, 실패={failed}, 스킵={skipped}")

        if self.debug_mode and not self.quiet_mode:
            print(f"✅ [{region_name}] 처리 완료")

    def _update_schools_with_diff(self, new_rows: List[dict], region_code: str, limit: Optional[int] = None) -> Tuple[int, int, int]:
        existing = {}
        if os.path.exists(self.db_path):
            try:
                with get_db_reader(self.db_path) as conn:
                    cur = conn.execute(
                        "SELECT sc_code, address_hash, latitude, longitude, geocode_attempts, last_error FROM schools"
                    )
                    existing = {
                        row[0]: {"hash": row[1], "lat": row[2], "lon": row[3], "attempts": row[4], "last_error": row[5]}
                        for row in cur
                    }
            except sqlite3.OperationalError as e:
                self.logger.error(f"DB 읽기 실패: {e}")
                return 0, 0, 0
            except Exception as e:
                self.logger.error(f"기존 데이터 조회 실패: {e}")
                return 0, 0, 0

        row_meta = {}
        for row in new_rows:
            sc_code = row.get("SD_SCHUL_CODE")
            if not sc_code or not self._include_school(sc_code):
                continue
            full_address = row.get("ORG_RDNMA", "")
            new_hash = AddressFilter.hash(full_address) if full_address else ""
            row_meta[sc_code] = {
                "row": row,
                "full_address": full_address,
                "new_hash": new_hash,
                "old": existing.get(sc_code, {}),
            }

        new_coords: Dict[str, Tuple[float, float]] = {}
        failed_count = 0
        skipped_count = 0
        start_time = time.time()
        last_update = start_time
        processed_count = 0

        # ✅ limit 상태 표시를 위한 변수
        limit_active = limit is not None

        def _print_progress(current, total, success, failed, skipped, start_t, quiet):
            if quiet:
                return
            elapsed = time.time() - start_t
            avg = current / elapsed if elapsed > 0 else 0
            bar_len = 40
            filled = int(bar_len * current / total) if total > 0 else 0
            bar = '█' * filled + '░' * (bar_len - filled)
            status = f"{GREEN}✅{RESET}{success:3d} {RED}❌{RESET}{failed:3d} {YELLOW}⏭️{RESET}{skipped:3d}"
            suffix = f" [LIMIT:{limit}]" if limit_active and current >= limit else ""
            print(f"\r[{bar}] {current}/{total}{suffix} {status} {avg:6.1f} 개/초", end="", flush=True)

        total_items = len(row_meta)
        for i, (sc_code, meta) in enumerate(row_meta.items(), 1):
            if limit and processed_count >= limit:
                if not self.quiet_mode:
                    print(f"\n⚠️  제한 ({limit} 개) 도달, 수집 중단")
                return len(new_coords), failed_count, skipped_count

            if meta["old"].get("hash") != meta["new_hash"] and meta["full_address"]:
                cleaned = AddressFilter.clean(meta["full_address"], level=self.LEVEL_GEOCODING)
                
                # ✅ 지번 주소 추출
                jibun = AddressFilter.extract_jibun(meta["full_address"])
                meta["jibun_address"] = jibun
                
                try:
                    coords = self.geo_collector._geocode(cleaned)
                except Exception as e:
                    if not self.quiet_mode:
                        print(f"\n  ⚠️ [{sc_code}] 예외: {type(e).__name__}")
                    self.logger.warning(f"지오코딩 예외 {sc_code}: {e}")
                    coords = None

                if coords:
                    new_coords[sc_code] = coords
                else:
                    failed_count += 1
                    if self.shard != "none":
                        now_naive = now_kst().replace(tzinfo=None)
                        deadline = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
                        if now_naive > deadline:
                            deadline += timedelta(days=1)
                        self.retry_mgr.record_failure(
                            domain='school', task_type='geocode',
                            shard=self.shard, sc_code=sc_code,
                            region=region_code, address=meta["full_address"],
                            jibun_address=jibun,
                            error="Geocoding failed", deadline=deadline,
                        )
            else:
                skipped_count += 1

            processed_count += 1
            if not self.quiet_mode and (time.time() - last_update >= 0.2 or i == total_items):
                _print_progress(i, total_items, len(new_coords), failed_count, skipped_count, start_time, self.quiet_mode)
                last_update = time.time()

        if not self.quiet_mode:
            print()

        # ✅ 저장할 데이터 구성 및 enqueue
        for sc_code, meta in row_meta.items():
            row = meta["row"]
            atpt_code = row.get("ATPT_OFCDC_SC_CODE") or ""
            old = meta["old"]

            if sc_code in new_coords:
                lon, lat = new_coords[sc_code]
                attempts, last_error = 0, None
            else:
                lat, lon = old.get("lat"), old.get("lon")
                attempts = old.get("attempts", 0) + 1
                last_error = old.get("last_error") or "Geocoding failed"

            cleaned = AddressFilter.clean(meta["full_address"], level=self.LEVEL_FINAL) if meta["full_address"] else ""
            addr_ids = {}
            if cleaned:
                try:
                    addr_ids = self.meta_vocab.save_address(cleaned)
                except Exception as e:
                    self.logger.error(f"주소 변환 실패 {sc_code}: {e}")

            self.enqueue([{
                "sc_code": sc_code,
                "school_id": create_school_id(atpt_code, sc_code),
                "sc_name": row.get("SCHUL_NM", ""),
                "eng_name": row.get("ENG_SCHUL_NM", ""),
                "sc_kind": row.get("SCHUL_KND_SC_NM", ""),
                "atpt_code": atpt_code,
                "address": meta["full_address"],
                "cleaned_address": cleaned,
                "address_hash": meta["new_hash"],
                "tel": row.get("ORG_TELNO", ""),
                "homepage": row.get("HMPG_ADRES", ""),
                "status": "운영",
                "last_seen": int(self.run_date),
                "load_dt": now_kst().isoformat(),
                "latitude": lat,
                "longitude": lon,
                "geocode_attempts": attempts,
                "last_error": last_error,
                "city_id": addr_ids.get("city_id", 0),
                "district_id": addr_ids.get("district_id", 0),
                "street_id": addr_ids.get("street_id", 0),
                "number_type": addr_ids.get("number_type"),
                "number_value": addr_ids.get("number"),
                "number_start": addr_ids.get("number_start"),
                "number_end": addr_ids.get("number_end"),
                "number_bit": addr_ids.get("number_bit", 0),
                "jibun_address": meta.get("jibun_address"),
                "kakao_address": None,
            }])

        region_name = REGION_NAMES.get(region_code, region_code)
        self.logger.info(f"[{region_name}] 좌표 갱신: {len(new_coords)}개 / 완료")

        return len(new_coords), failed_count, skipped_count

    def _process_item(self, raw_item: dict) -> List[dict]:
        return []

    def _do_save_batch(self, conn: sqlite3.Connection, batch: List[dict]):
        try:
            conn.executemany("""
                INSERT OR REPLACE INTO schools
                (sc_code, school_id, sc_name, eng_name, sc_kind, atpt_code,
                 address, cleaned_address, address_hash, tel, homepage, status,
                 last_seen, load_dt, latitude, longitude, geocode_attempts, last_error,
                 city_id, district_id, street_id, number_type, number_value,
                 number_start, number_end, number_bit, jibun_address, kakao_address)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                (it['sc_code'], it['school_id'], it['sc_name'], it['eng_name'],
                 it['sc_kind'], it['atpt_code'], it['address'], it['cleaned_address'],
                 it['address_hash'], it['tel'], it['homepage'], it['status'],
                 it['last_seen'], it['load_dt'], it['latitude'], it['longitude'],
                 it['geocode_attempts'], it['last_error'], it['city_id'],
                 it['district_id'], it['street_id'], it['number_type'],
                 it['number_value'], it['number_start'], it['number_end'],
                 it['number_bit'], it.get('jibun_address'), it.get('kakao_address'))
                for it in batch
            ])
            if self.debug_mode and not self.quiet_mode:
                print(f"💾 배치 저장 완료: {len(batch)}개")
        except Exception as e:
            self.logger.error(f"배치 저장 실패: {e}")
            raise


if __name__ == "__main__":
    # ✅ GitHub Actions 환경 감지
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'

    parser = argparse.ArgumentParser(description="학교 기본정보 수집기")
    parser.add_argument("--regions", default="ALL", help="수집할 지역 (ALL 또는 쉼표 구분, 예: B10,C10)")
    parser.add_argument("--debug", action="store_true", help="상세 출력 모드")
    parser.add_argument("--quiet", action="store_true", help="출력 최소화 (GitHub Actions 등)")
    parser.add_argument("--limit", type=int, default=None, help="수집할 학교 수 제한 (테스트용)")
    args = parser.parse_args()

    # ✅ GitHub Actions 환경에서는 자동으로 quiet 모드 활성화
    if is_github_actions and not args.quiet:
        args.quiet = True

    collector = NeisInfoCollector(
        shard="none",
        debug_mode=args.debug,
        quiet_mode=args.quiet
    )

    if args.regions == "ALL":
        regions = ALL_REGIONS
    else:
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    if not args.quiet:
        print(f"\n🚀 학교 정보 수집 시작 (지역: {len(regions)}개, limit: {args.limit or '전체'})")
        print("=" * 70)

    for region in regions:
        collector.fetch_region(region, limit=args.limit)
        if args.limit:
            break

    collector.close()

    # ✅ 전체 통계 출력 (quiet 모드가 아니면)
    if not args.quiet:
        total = collector.total_new + collector.total_failed + collector.total_skipped
        success_rate = (collector.total_new / total * 100) if total > 0 else 0

        print("=" * 70)
        print(f"📊 전체 통계")
        print(f"   신규 성공: {collector.total_new}개 ({success_rate:.1f}%)")
        print(f"   실패:      {collector.total_failed}개")
        print(f"   스킵:      {collector.total_skipped}개")
        print(f"   총 처리:   {total}개")
        print("=" * 70)
        print("✅ 수집 완료")
    else:
        collector.logger.info("수집 완료")
        