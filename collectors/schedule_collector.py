#!/usr/bin/env python3
"""
학사일정 수집기 (schedule)
- BaseCollector 기반
- 샤딩 지원 (odd/even)
- 학교 코드 범위 필터링 지원 (A/B)
- vocab_event 테이블로 이벤트명 관리
- hashlib 기반 고정 ID 생성
"""
import os
import argparse
import sqlite3
import time
import hashlib  # ✅ 추가: 고정 해시용
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request
from parsers.schedule_parser import parse_schedule_row
from constants.codes import NEIS_API_KEY, NEIS_ENDPOINTS, ALL_REGIONS
from core.kst_time import now_kst
from core.school_year import get_current_school_year

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

NEIS_URL = NEIS_ENDPOINTS['schedule']


class ScheduleCollector(BaseCollector):
    """학사일정 수집기 - BaseCollector 기반"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False):
        """
        Args:
            shard: 샤드 타입 ('odd', 'even', 'none')
            school_range: 학교 코드 범위 ('A', 'B', None)
            incremental: 증분 수집 여부
            full: 전체 수집 후 백업 여부
        """
        # BaseCollector에 school_range 전달
        super().__init__("schedule", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.run_date = now_kst().strftime("%Y%m%d")
        
        # 이벤트 vocab 캐시
        self.event_cache = {}  # ev_nm -> ev_id
        self._load_event_cache()
        
        self.logger.info(f"📅 ScheduleCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _load_event_cache(self):
        """이벤트 vocab 캐시 로드"""
        try:
            with get_db_connection(self.db_path) as conn:
                cur = conn.execute("SELECT ev_id, ev_nm FROM vocab_event")
                for ev_id, ev_nm in cur:
                    self.event_cache[ev_nm] = ev_id
            self.logger.info(f"✅ 이벤트 캐시 로드: {len(self.event_cache)}개")
        except Exception as e:
            self.logger.error(f"이벤트 캐시 로드 실패: {e}")
    
    def _init_db(self):
        """DB 테이블 초기화"""
        with get_db_connection(self.db_path) as conn:
            # 이벤트 vocab 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_event (
                    ev_id INTEGER PRIMARY KEY,
                    ev_nm TEXT NOT NULL UNIQUE,
                    ev_date INTEGER
                )
            """)
            
            # 학사일정 메인 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    school_id    INTEGER NOT NULL,
                    ev_date      INTEGER NOT NULL,
                    ev_id        INTEGER NOT NULL,
                    ay           INTEGER NOT NULL,
                    is_special   INTEGER NOT NULL,
                    grade_disp   TEXT NOT NULL,
                    grade_raw    TEXT NOT NULL,
                    grade_code   INTEGER NOT NULL,
                    sub_yn       INTEGER NOT NULL,
                    sub_code     TEXT,
                    dn_yn        INTEGER NOT NULL,
                    ev_content   TEXT,
                    load_dt      TEXT,
                    PRIMARY KEY (school_id, ev_date, ev_id, grade_code),
                    FOREIGN KEY (ev_id) REFERENCES vocab_event(ev_id)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schedule_date ON schedule(ev_date)")
            
            # 공통 체크포인트 테이블
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        """체크포인트용 대상 키"""
        return self.run_date
    
    def _get_event_id(self, ev_nm: str, ev_date: int) -> int:
        """
        이벤트명으로 ID 조회/생성
        - hashlib.sha256 사용으로 고정 ID 보장 (실행 환경과 무관)
        """
        if not ev_nm:
            return 0
        
        # 캐시 확인
        if ev_nm in self.event_cache:
            return self.event_cache[ev_nm]
        
        # ✅ hashlib.sha256 기반 고정 ID 생성
        key = f"schedule:{ev_nm}:{ev_date}"
        hash_obj = hashlib.sha256(key.encode())
        # 16자리 16진수 → 정수 변환 → 12자리로 제한
        ev_id = int(hash_obj.hexdigest()[:16], 16) % 10**12
        
        # 캐시에 저장 (실제 DB 저장은 _save_batch에서)
        self.event_cache[ev_nm] = ev_id
        return ev_id
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        """API 응답 아이템 처리"""
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code:
            return []
        
        # BaseCollector의 _include_school()로 필터링
        if not self._include_school(sc_code):
            return []
        
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        school_id = school_info['school_id']
        
        parsed = parse_schedule_row(raw_item, school_info)
        if not parsed:
            return []
        
        # 이벤트 ID 생성/조회 (hashlib 사용)
        ev_id = self._get_event_id(parsed["ev_nm"], parsed["ev_date"])
        
        results = []
        for grade_code in parsed.get("grade_codes", [0]):
            d = {
                "school_id": school_id,
                "ev_date": parsed["ev_date"],
                "ev_id": ev_id,
                "ev_nm": parsed["ev_nm"],  # vocab 저장용
                "ay": parsed["ay"],
                "is_special": parsed["is_sp"],
                "grade_disp": parsed["grade_disp"],
                "grade_raw": parsed["grade_raw"],
                "grade_code": grade_code,
                "sub_yn": parsed["sub_yn"],
                "sub_code": parsed["sub_code"],
                "dn_yn": parsed["dn_yn"],
                "ev_content": parsed["content"],
                "load_dt": parsed["load_dt"]
            }
            results.append(d)
        return results
    
    def _save_batch(self, batch: List[dict]):
        """배치 데이터 저장"""
        with get_db_connection(self.db_path) as conn:
            # 이벤트 vocab 저장
            vocab_set = {(it['ev_id'], it['ev_nm'], it['ev_date']) for it in batch}
            conn.executemany(
                "INSERT OR IGNORE INTO vocab_event VALUES (?,?,?)",
                list(vocab_set)
            )
            
            # 학사일정 저장
            sched_data = [
                (
                    it['school_id'], it['ev_date'], it['ev_id'], it['ay'],
                    it['is_special'], it['grade_disp'], it['grade_raw'], it['grade_code'],
                    it['sub_yn'], it['sub_code'], it['dn_yn'], it['ev_content'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT INTO schedule VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT DO UPDATE SET
                    ev_content = excluded.ev_content,
                    sub_yn = excluded.sub_yn,
                    sub_code = excluded.sub_code,
                    dn_yn = excluded.dn_yn,
                    load_dt = excluded.load_dt
            """, sched_data)
    
    def fetch_region(self, region: str, year: int):
        """
        특정 교육청의 학사일정 수집
        year = 학년도 (예: 2026 → 2026-03-01 ~ 2027-02-28)
        """
        p_idx = 1
        consecutive_errors = 0
        max_page = 200
        
        date_from = f"{year}0301"
        date_to = (date(year + 1, 3, 1) - timedelta(days=1)).strftime("%Y%m%d")
        
        self.logger.info(f"📡 {region} 수집 시작 (기간: {date_from}~{date_to})")

        while p_idx <= max_page:
            params = {
                "KEY": NEIS_API_KEY,
                "Type": "json",
                "pIndex": p_idx,
                "pSize": 1000,
                "ATPT_OFCDC_SC_CODE": region,
                "AA_YMD_FROM": date_from,
                "AA_YMD_TO":   date_to,
            }
            
            try:
                res = safe_json_request(self.session, NEIS_URL, params, self.logger)
                if not res or "schoolSchedule" not in res:
                    break
                    
                rows = res["schoolSchedule"][1].get("row", [])
                if not rows:
                    break
                
                batch = []
                for r in rows:
                    items = self._process_item(r)  # 내부에서 _include_school() 처리
                    if items:
                        batch.extend(items)
                
                if batch:
                    self.enqueue(batch)
                    
                self.logger.info(f"[{region}] p={p_idx} → {len(rows)}건, 이벤트 {len(batch)}개")
                consecutive_errors = 0
                
                if len(rows) < 1000:
                    break
                    
                p_idx += 1
                time.sleep(0.05)
                
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"[{region}] p={p_idx} 에러: {e}")
                if consecutive_errors >= 5:
                    self.logger.warning(f"[{region}] 연속 에러 5회 → 중단")
                    break
                p_idx += 1
                time.sleep(2 ** min(consecutive_errors, 5))
    
    def close(self):
        """종료 처리"""
        if self.full:
            self.create_dated_backup()
        super().close()


def main():
    parser = argparse.ArgumentParser(description="학사일정 수집기")
    parser.add_argument("--regions", required=True, help="B10,C10,... 또는 ALL")
    parser.add_argument("--year", type=int, default=get_current_school_year(), 
                       help="수집 학년도 (예: 2026)")
    parser.add_argument("--shard", default="none", choices=["odd", "even", "none"],
                       help="샤드 필터 (odd=홀수, even=짝수, none=전체)")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none",
                       help="학교 코드 범위 필터 (A=1-4, B=5-9)")
    parser.add_argument("--incremental", action="store_true", help="증분 수집 모드")
    parser.add_argument("--full", action="store_true", help="전체 수집 후 백업 생성")
    parser.add_argument("--debug", action="store_true", help="디버그 모드")
    
    args = parser.parse_args()

    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return

    # regions 처리
    if args.regions.upper() == "ALL":
        regions = ALL_REGIONS
    else:
        regions = [r.strip() for r in args.regions.split(",")]

    # school_range 처리 ('none'이면 None으로 변환)
    school_range = None if args.school_range == "none" else args.school_range

    # 디버그 모드 출력
    if args.debug:
        print(f"🔧 디버그 모드")
        print(f"  regions: {regions}")
        print(f"  year: {args.year}")
        print(f"  shard: {args.shard}")
        print(f"  school_range: {school_range}")

    collector = ScheduleCollector(
        shard=args.shard,
        school_range=school_range,
        incremental=args.incremental,
        full=args.full
    )

    try:
        for region in regions:
            collector.logger.info(f"🚀 {region} 수집 시작 (shard={args.shard}, range={school_range})")
            collector.fetch_region(region, args.year)
    finally:
        collector.close()
        collector.logger.info("🏁 수집 완료")


if __name__ == "__main__":
    main()
    