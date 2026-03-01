#!/usr/bin/env python3
"""
시간표 수집기 - 학년도 전체 버전
"""
from pathlib import Path
from core.base_collector import BaseCollector
from core.database import get_db_connection
from core.meta_vocab import MetaVocabManager
from core.id_generator import IDGenerator
from parsers.timetable_parser import parse_timetable_row
from constants.codes import (
    NEIS_API_KEY, NEIS_ENDPOINTS, TIMETABLE_ENDPOINTS,
    GRADE_RANGES, SPECIAL_GRADE_DESC, API_CONFIG, ALL_REGIONS
)
from core.kst_time import now_kst
from core.school_year import get_current_school_year

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR = PROJECT_ROOT / "data" / "active"
GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")

class AnnualFullTimetableCollector(BaseCollector):
    def __init__(self, shard="none", school_range=None, debug_mode=False):
        super().__init__("timetable", str(ACTIVE_DIR), shard, school_range)
        self.api_context = 'timetable'
        self.debug_mode = debug_mode
        self.run_ay = get_current_school_year(now_kst())
        # retry_queue 제거 (BaseCollector에서 이미 재시도)
        self.meta_vocab = MetaVocabManager(GLOBAL_VOCAB_PATH, debug_mode)
        self.subject_cache = {}
        self._load_subject_cache()
        self.logger.info(f"📚 TimetableCollector 초기화 완료")
    
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
            conn.execute("CREATE INDEX idx_timetable_school ON timetable(school_id, ay)")
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
    
    def _classify_school(self, school_code: str):
        school_info = self.get_school_info(school_code)
        if not school_info:
            return None
        kind = school_info.get('kind', '')
        if kind in ["유치원", "유"]:
            return None
        url = TIMETABLE_ENDPOINTS.get(kind)
        grades = GRADE_RANGES.get(kind)
        is_special = (kind in ["특수학교", "특"])
        if not url or grades is None:
            return None
        return {
            'url': url,
            'grades': grades,
            'is_special': is_special,
            'school_info': school_info
        }
    
    def fetch_region(self, region: str, **kwargs):
        year = kwargs.get("year", self.run_ay)
        semester = kwargs.get("semester", 1)
        self.fetch_year(region, year, semester)
    
    def fetch_year(self, region: str, year: int, semester: int):
        schools = self._get_school_list(region)
        for sch_code, sch_name in schools:
            self._fetch_school_timetable(sch_code, year, semester)
    
    def _fetch_school_timetable(self, school_code: str, ay: int, semester: int):
        info = self._classify_school(school_code)
        if not info:
            return
        school_info = info['school_info']
        for grade in info['grades']:
            class_empty_count = 0
            for class_nm in map(str, range(1, 21)):
                base_params = {
                    "ATPT_OFCDC_SC_CODE": school_info['atpt_code'],
                    "SD_SCHUL_CODE": school_code,
                    "AY": str(ay),
                    "GRADE": str(grade),
                    "CLASS_NM": class_nm,
                    "SEM": str(semester)
                }
                try:
                    # 페이지네이션 불필요 (pSize=100으로 충분)
                    params = {**base_params, "pIndex": 1, "pSize": 100}
                    res = safe_json_request(self.session, info['url'], params, self.logger)
                    if not res:
                        class_empty_count += 1
                        if class_empty_count >= 3:
                            self.logger.debug(f"    {grade}학년: 3회 연속 실패 → 중단")
                            break
                        continue
                    endpoint_name = info['url'].split('/')[-1]
                    api_key = endpoint_name  # 직접 사용
                    if api_key not in res:
                        class_empty_count += 1
                        if class_empty_count >= 3:
                            break
                        continue
                    rows = res[api_key][1].get("row", [])
                    if not rows:
                        class_empty_count += 1
                        if class_empty_count >= 3:
                            break
                        continue
                    class_empty_count = 0
                    batch = []
                    for r in rows:
                        items = self._process_item(r)
                        if items:
                            batch.extend(items)
                    if batch:
                        self.enqueue(batch)
                except Exception as e:
                    self.logger.error(f"{school_code} {grade}학년 {class_nm}반 실패: {e}")
                    class_empty_count += 1
                    if class_empty_count >= 3:
                        break
                time.sleep(API_CONFIG['rate_limit']['sleep_time'])
    
    def _process_item(self, raw_item: dict) -> List[dict]:
        sc_code = self._get_field(raw_item, 'school_code')
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
            subj_set = {(it['subject_id'], it['subject_name'], it['normalized_key'], it['level'])
                        for it in batch if it.get('subject_id')}
            if subj_set:
                conn.executemany(
                    "INSERT OR IGNORE INTO vocab_subject (subject_id, subject_name, normalized_key, level) VALUES (?,?,?,?)",
                    list(subj_set)
                )
            tt_data = [
                (
                    it['school_id'], it['ay'], it['semester'], it['grade'], it['class_nm'],
                    it['day_of_week'], it['period'], it['subject_id'], it['load_dt']
                )
                for it in batch
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO timetable VALUES (?,?,?,?,?,?,?,?,?)",
                tt_data
            )
    
    def close(self):
        self.meta_vocab.close()
        super().close()
        