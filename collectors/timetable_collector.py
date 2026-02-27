#!/usr/bin/env python3
"""
시간표 수집기 - 학교급별 API 지원 (특수학교 0-13학년)
"""
import os
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.network import safe_json_request, APILimitExceededException
from core.meta_vocab import MetaVocabManager
from core.id_generator import IDGenerator
from parsers.timetable_parser import parse_timetable_row
from constants.codes import (
    NEIS_API_KEY, NEIS_ENDPOINTS, 
    TIMETABLE_ENDPOINTS, TIMETABLE_RESPONSE_KEYS,
    GRADE_RANGES, SPECIAL_GRADE_DESC,
    API_CONFIG, ALL_REGIONS
)
from core.kst_time import now_kst
from core.school_year import get_current_school_year

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "../data/active")
os.makedirs(BASE_DIR, exist_ok=True)

GLOBAL_VOCAB_PATH = os.path.join(BASE_DIR, "global_vocab.db")


class TimetableCollector(BaseCollector):
    """시간표 수집기 - 학교급별 API 지원"""
    
    def __init__(self, shard: str = "none", school_range: Optional[str] = None,
                 incremental: bool = False, full: bool = False, debug_mode: bool = False):
        if shard == "none":
            db_name = "timetable.db"
        else:
            range_suffix = f"_{school_range}" if school_range else ""
            db_name = f"timetable_{shard}{range_suffix}.db"
        
        db_path = os.path.join(BASE_DIR, db_name)
        
        super().__init__("timetable", BASE_DIR, shard, school_range)
        
        self.incremental = incremental
        self.full = full
        self.debug_mode = debug_mode
        self.run_ay = get_current_school_year()
        self.retry_queue = []
        
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.subject_cache = {}
        
        self._load_subject_cache()
        
        if not self.school_cache:
            self.logger.error("❌ 학교 캐시 비어있음! school_master.db 확인 필요")
        else:
            self.logger.info(f"✅ 학교 캐시 로드: {len(self.school_cache)}개")
        
        self.logger.info(f"📚 TimetableCollector 초기화 완료 (shard={shard}, range={school_range})")
    
    def _load_subject_cache(self):
        try:
            with get_db_connection(self.db_path) as conn:
                cur = conn.execute("SELECT subject_id, subject_name FROM vocab_subject")
                for subject_id, subject_name in cur:
                    self.subject_cache[subject_name] = subject_id
        except Exception:
            pass
    
    def _init_db(self):
        with get_db_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vocab_subject (
                    subject_id INTEGER PRIMARY KEY,
                    subject_name TEXT NOT NULL UNIQUE,
                    normalized_key TEXT,
                    level TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timetable (
                    school_id INTEGER NOT NULL,
                    ay INTEGER NOT NULL,
                    semester INTEGER NOT NULL,
                    grade INTEGER NOT NULL,
                    class_nm TEXT NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    period INTEGER NOT NULL,
                    subject_id INTEGER NOT NULL,
                    load_dt TEXT,
                    PRIMARY KEY (school_id, ay, semester, grade, class_nm, day_of_week, period)
                ) WITHOUT ROWID
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timetable_school ON timetable(school_id, ay)")
            self._init_db_common(conn)
    
    def _get_target_key(self) -> str:
        return str(self.run_ay)
    
    def _get_subject_id(self, subject_name: str, normalized_key: str, level: str) -> int:
        if subject_name in self.subject_cache:
            return self.subject_cache[subject_name]
        
        subject_id = IDGenerator.text_to_int(
            text=normalized_key or subject_name,
            namespace="subject",
            bits=63
        )
        self.subject_cache[subject_name] = subject_id
        return subject_id
    
    def _enqueue_retry(self, item_id: str, error: Exception, semester: int = 1):
        if not item_id:
            return
        
        school_code = str(item_id).split('_')[0] if '_' in str(item_id) else "unknown"
        
        self.retry_queue.append({
            "id": item_id,
            "school_code": school_code,
            "semester": semester,
            "error": str(error)[:200],
            "time": now_kst().isoformat(),
            "retry_count": 0
        })
    
    def _classify_school(self, school_code: str) -> Optional[Dict]:
        school_info = self.get_school_info(school_code)
        if not school_info:
            self.logger.warning(f"⚠️ {school_code}: 학교 정보 없음")
            return None
        
        kind = school_info.get('sc_kind')
        if kind is None:
            self.logger.error(f"❌ {school_code}: sc_kind 컬럼 없음 - school_master.db 스키마 확인!")
            return None
        
        kind = kind.strip()
        
        if kind == "유치원":
            self.logger.info(f"🏫 {school_code}: 일반유치원 (수집 제외 - 추후 개발)")
            return None
        
        url = TIMETABLE_ENDPOINTS.get(kind)
        grades = GRADE_RANGES.get(kind)
        is_special = (kind in ["특수학교", "특"])
        
        if not url or grades is None:
            self.logger.warning(f"⚠️ {school_code}: 지원하지 않는 학교급 '{kind}'")
            return None
        
        return {
            'url': url,
            'grades': grades,
            'kind': kind,
            'is_special': is_special,
            'school_info': school_info
        }
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = raw_item.get("SD_SCHUL_CODE")
        if not sc_code or not self._include_school(sc_code):
            return []
        
        school_info = self.get_school_info(sc_code)
        if not school_info:
            return []
        
        parsed = parse_timetable_row(raw_item)
        if not parsed.get("subject_name"):
            return []
        
        subject_id = self._get_subject_id(
            parsed["subject_name"],
            parsed.get("normalized_key", ""),
            parsed.get("level", "")
        )
        
        return [{
            "school_id": school_info['school_id'],
            "ay": parsed["ay"],
            "semester": parsed["semester"],
            "grade": parsed["grade"],
            "class_nm": parsed["class_nm"],
            "day_of_week": parsed["day_of_week"],
            "period": parsed["period"],
            "subject_id": subject_id,
            "subject_name": parsed["subject_name"],
            "normalized_key": parsed.get("normalized_key", ""),
            "level": parsed.get("level", ""),
            "load_dt": now_kst().isoformat()
        }]
    
    def _save_batch(self, batch: List[dict]):
        with get_db_connection(self.db_path) as conn:
            subj_set = {(it['subject_id'], it['subject_name'], 
                        it['normalized_key'], it['level']) 
                       for it in batch if it.get('subject_id')}
            if subj_set:
                conn.executemany("""
                    INSERT OR IGNORE INTO vocab_subject 
                    (subject_id, subject_name, normalized_key, level)
                    VALUES (?,?,?,?)
                """, list(subj_set))
            
            tt_data = [
                (
                    it['school_id'], it['ay'], it['semester'], it['grade'], it['class_nm'],
                    it['day_of_week'], it['period'], it['subject_id'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO timetable VALUES (?,?,?,?,?,?,?,?,?)
            """, tt_data)
    
    def fetch_school_timetable(self, school_code: str, ay: int, semester: int):
        info = self._classify_school(school_code)
        if not info:
            return
        
        url = info['url']
        grades = info['grades']
        kind = info['kind']
        is_special = info['is_special']
        school_info = info['school_info']
        
        self.logger.info(f"📡 {school_code} [{kind}] 수집 시작")
        
        if is_special:
            self.logger.debug(f"  학년 범위: 0(유치원)~15(전공과3년차)")
        
        grade_empty_count = 0
        
        for grade in grades:
            class_empty_count = 0
            grade_has_data = False
            
            if is_special:
                desc = SPECIAL_GRADE_DESC.get(grade, f"{grade}학년")
                
                if grade == 0:
                    self.logger.info(f"  🏫 유치원 과정 (grade {grade})")
                elif 1 <= grade <= 6:
                    self.logger.info(f"  📚 초등과정 {grade}학년")
                elif 7 <= grade <= 9:
                    middle = grade - 6
                    self.logger.info(f"  📖 중등과정 {grade}학년 (중{middle})")
                elif 10 <= grade <= 12:
                    high = grade - 9
                    self.logger.info(f"  📕 고등과정 {grade}학년 (고{high})")
                elif grade >= 13:
                    major = grade - 12
                    self.logger.info(f"  🎓 전공과 {major}년차")
            
            for class_nm in map(str, range(1, 21)):
                params = {
                    "KEY": NEIS_API_KEY,
                    "Type": "json",
                    "pIndex": 1,
                    "pSize": 1000,
                    "ATPT_OFCDC_SC_CODE": school_info['atpt_code'],
                    "SD_SCHUL_CODE": school_code,
                    "AY": str(ay),
                    "GRADE": str(grade),
                    "CLASS_NM": class_nm,
                    "SEM": str(semester)
                }
                
                try:
                    res = safe_json_request(self.session, url, params, self.logger)
                    
                    if res is None:
                        class_empty_count += 1
                        if class_empty_count >= 5:
                            self.logger.debug(f"    {grade}학년: 5회 연속 오류 → 중단")
                            break
                        continue
                    
                    endpoint_name = url.split('/')[-1]
                    api_key = TIMETABLE_RESPONSE_KEYS.get(endpoint_name, endpoint_name)
                    
                    if api_key not in res:
                        class_empty_count += 1
                        if class_empty_count >= 5:
                            self.logger.debug(f"    {grade}학년: 5회 연속 응답 없음 → 중단")
                            break
                        continue
                    
                    rows = res[api_key][1].get("row", [])
                    if not rows:
                        class_empty_count += 1
                        if class_empty_count >= 5:
                            self.logger.debug(f"    {grade}학년: 5회 연속 데이터 없음 → 중단")
                            break
                        continue
                    
                    class_empty_count = 0
                    grade_has_data = True
                    
                    batch = []
                    for r in rows:
                        try:
                            items = self._process_item(r)
                            if items:
                                batch.extend(items)
                        except Exception as e:
                            self.logger.error(f"항목 처리 실패: {e}")
                            self._enqueue_retry(f"{school_code}_{grade}_{class_nm}", e, semester)
                            continue
                    
                    if batch:
                        self.enqueue(batch)
                        if self.debug_mode:
                            self.logger.debug(f"    {grade}학년 {class_nm}반 → {len(batch)}개")
                    
                except APILimitExceededException:
                    raise
                    
                except Exception as e:
                    self.logger.error(f"{school_code} {grade}학년 {class_nm}반 실패: {e}")
                    self._enqueue_retry(f"{school_code}_{grade}_{class_nm}", e, semester)
                    class_empty_count += 1
                    if class_empty_count >= 5:
                        self.logger.warning(f"    {grade}학년: 5회 연속 에러 → 중단")
                        break
                
                time.sleep(API_CONFIG['rate_limit']['sleep_time'])
            
            if not is_special:
                if grade_has_data:
                    grade_empty_count = 0
                else:
                    grade_empty_count += 1
                    if grade_empty_count >= 3:
                        self.logger.info(f"  {grade}학년까지 3회 연속 데이터 없음 → 수집 종료")
                        break
    
    def retry_failed(self) -> int:
        if not self.retry_queue:
            self.logger.info("✅ 재시도할 항목 없음")
            return 0
        
        self.logger.info(f"🔄 재시도 시작: {len(self.retry_queue)}개")
        failed_items = self.retry_queue.copy()
        self.retry_queue.clear()
        
        success = 0
        for item in failed_items:
            if item.get('retry_count', 0) >= 3:
                self.logger.warning(f"  ⏭️ {item['id']} 3회 초과, 포기")
                continue
            
            try:
                parts = item['id'].split('_')
                if len(parts) >= 3:
                    sc_code = parts[0]
                    grade = int(parts[1])
                    class_nm = parts[2]
                    semester = item.get('semester', 1)
                    
                    self.logger.info(f"  🔄 {sc_code} 재수집 (grade={grade}학년, semester={semester})")
                    
                    before = len(self.retry_queue)
                    self.fetch_school_timetable(sc_code, self.run_ay, semester)
                    after = len(self.retry_queue)
                    
                    if after == before:
                        success += 1
                    else:
                        self.logger.warning(f"  ⚠️ 재시도 중 새 실패 발생 (추가 {after-before}개)")
                    
            except Exception as e:
                self.logger.error(f"재시도 최종 실패 {item['id']}: {e}")
        
        self.logger.info(f"✅ 재시도 완료: {success}/{len(failed_items)} 성공")
        return success
    
    def merge_shards(self):
        """샤드 병합 - ATTACH 먼저, BEGIN 나중"""
        main_db = os.path.join(BASE_DIR, "timetable.db")
        even_db = os.path.join(BASE_DIR, "timetable_even.db")
        odd_db = os.path.join(BASE_DIR, "timetable_odd.db")
        
        main = sqlite3.connect(main_db, isolation_level=None)
        try:
            for shard_path in [even_db, odd_db]:
                if not os.path.exists(shard_path):
                    continue
                
                attached = False
                try:
                    main.execute(f"ATTACH DATABASE '{shard_path}' AS shard")
                    attached = True
                    
                    main.execute("BEGIN")
                    main.execute("""
                        INSERT OR IGNORE INTO main.vocab_subject 
                        SELECT * FROM shard.vocab_subject
                    """)
                    main.execute("""
                        INSERT OR REPLACE INTO main.timetable 
                        SELECT * FROM shard.timetable
                    """)
                    main.execute("COMMIT")
                    
                except Exception as e:
                    try:
                        main.execute("ROLLBACK")
                    except:
                        pass
                    self.logger.error(f"샤드 병합 실패: {e}")
                    raise
                finally:
                    if attached:
                        try:
                            main.execute("DETACH DATABASE shard")
                        except:
                            pass
            
            self.logger.info("✅ 샤드 병합 완료")
        finally:
            main.close()
    
    def _log_collection_summary(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM timetable").fetchone()[0]
                
                ay = getattr(self, 'run_ay', None)
                if ay:
                    today_count = conn.execute(
                        "SELECT COUNT(*) FROM timetable WHERE ay = ?", (ay,)
                    ).fetchone()[0]
                else:
                    today_count = 0
                
                recent = conn.execute(
                    "SELECT load_dt FROM timetable ORDER BY load_dt DESC LIMIT 1"
                ).fetchone()
                last_load = recent[0][:10] if recent else "없음"
            
            self.logger.info("=" * 50)
            self.logger.info(f"📊 수집 완료 요약 [timetable]")
            self.logger.info(f"   전체 레코드: {total:,}개")
            self.logger.info(f"   금학년도 수집: {today_count:,}개")
            self.logger.info(f"   마지막 수집: {last_load}")
            self.logger.info(f"   실패 대기: {len(self.retry_queue)}개")
            self.logger.info("=" * 50)
            
        except Exception as e:
            self.logger.error(f"통계 로깅 실패: {e}")
    
    def close(self):
        self.retry_failed()
        self._log_collection_summary()
        
        if self.full:
            try:
                with get_db_connection(self.db_path) as conn:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    self.logger.info("💾 WAL checkpoint 완료")
            except Exception as e:
                self.logger.error(f"checkpoint 실패: {e}")
        
        self.meta_vocab.close()
        super().close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="시간표 수집기")
    parser.add_argument("--ay", type=int, default=get_current_school_year())
    parser.add_argument("--semester", type=int, default=1, choices=[1, 2])
    parser.add_argument("--regions", help="교육청 코드 (예: B10,C10 또는 ALL)")
    parser.add_argument("--shard", choices=["odd", "even", "none"], default="none")
    parser.add_argument("--school_range", choices=["A", "B", "none"], default="none")
    parser.add_argument("--merge", action="store_true", help="샤드 병합")
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()
    
    if not NEIS_API_KEY:
        print("❌ NEIS_API_KEY 환경변수가 없습니다.")
        return
    
    if args.merge:
        collector = TimetableCollector()
        collector.merge_shards()
        return
    
    school_range = None if args.school_range == "none" else args.school_range
    
    collector = TimetableCollector(
        shard=args.shard,
        school_range=school_range,
        debug_mode=args.debug
    )
    
    if args.regions and args.regions.upper() == "ALL":
        regions = set(ALL_REGIONS)
    elif args.regions:
        regions = {r.strip() for r in args.regions.split(",")}
    else:
        regions = set(ALL_REGIONS)
    
    schools = []
    for sc_code, info in collector.school_cache.items():
        if info['atpt_code'] in regions and collector._include_school(sc_code):
            schools.append(sc_code)
    
    if not schools:
        collector.logger.error("❌ 수집 대상 학교 없음! regions 또는 shard 확인")
        collector.close()
        return
    
    collector.logger.info(f"🚀 시간표 수집 시작: {len(schools)}개 학교")
    
    try:
        for sc_code in schools:
            collector.fetch_school_timetable(sc_code, args.ay, args.semester)
    except APILimitExceededException:
        collector.logger.critical("🚨 API 일일 한도 초과! 수집 중단")
    finally:
        collector.close()


if __name__ == "__main__":
    main()