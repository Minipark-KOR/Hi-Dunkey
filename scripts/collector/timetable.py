#!/usr/bin/env python3
# scripts/collector/timetable.py
# 개발 가이드: docs/developer_guide.md 참조

import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

import time
from typing import List

from core.engine.entry_collector import CollectorEngine
from core.data.database import get_db_connection
from core.data.meta_vocab import MetaVocabManager
from core.data.id_generator import IDGenerator
from core.config import config
from parsers.timetable import parse_timetable_row
from constants.codes import (
    TIMETABLE_ENDPOINTS, GRADE_RANGES, API_CONFIG
)
from core.kst_time import now_kst
from core.school.year import get_current_school_year
from constants.paths import ACTIVE_DIR, GLOBAL_VOCAB_DB_PATH
from constants.collector_names import TIMETABLE


class AnnualFullTimetableCollector(CollectorEngine):
    # ----- 메타데이터 (클래스 변수) -----
    collector_name = TIMETABLE
    description = "시간표"
    table_name = "timetable"
    merge_script = "scripts/merge_timetable_dbs.py"

    _cfg = config.get_collector_config(collector_name)
    timeout_seconds = _cfg.get("timeout_seconds", 3600)
    parallel_timeout_seconds = _cfg.get("parallel_timeout_seconds", 7200)
    merge_timeout_seconds = _cfg.get("merge_timeout_seconds", 3600)
    parallel_script = _cfg.get("parallel_script", "scripts/run_pipeline.py")
    modes = _cfg.get("modes", ["통합", "odd 샤드", "even 샤드", "병렬 실행"])
    metrics_config = _cfg.get("metrics_config", {"enabled": True})
    parallel_config = _cfg.get("parallel_config", {
        "max_workers": 4,
        "cpu_factor": 1.0,
        "max_by_api": 10,
        "absolute_max": 16,
    })
    # ------------------------------------

    def __init__(self, shard="none", school_range=None, debug_mode=False, **kwargs):
        super().__init__(self.collector_name, str(ACTIVE_DIR), shard, school_range, **kwargs)
        self.api_context = 'timetable'
        self.debug_mode = debug_mode
        self.run_ay = get_current_school_year(now_kst())
        self.meta_vocab = self.register_resource(
            MetaVocabManager(GLOBAL_VOCAB_DB_PATH, debug_mode)
        )
        self.subject_cache = {}
        self._load_subject_cache()
        self.logger.info("📚 TimetableCollector 초기화 완료")

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
                    subject_id     INTEGER PRIMARY KEY,
                    subject_name   TEXT NOT NULL UNIQUE,
                    normalized_key TEXT,
                    level          TEXT,
                    created_at     TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timetable (
                    school_id   INTEGER NOT NULL,
                    ay          INTEGER NOT NULL,
                    semester    INTEGER NOT NULL,
                    grade       INTEGER NOT NULL,
                    class_nm    TEXT NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    period      INTEGER NOT NULL,
                    subject_id  INTEGER NOT NULL,
                    load_dt     TEXT,
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

    def _classify_school(self, school_code: str):
        school_info = self.get_school_info(school_code)
        if not school_info:
            return None
        kind = school_info.get('kind', '')
        if kind in ["유치원", "유"]:
            return None
        url = TIMETABLE_ENDPOINTS.get(kind)
        grades = GRADE_RANGES.get(kind)
        if not url or grades is None:
            return None
        return {
            'url': url,
            'grades': grades,
            'is_special': kind in ["특수학교", "특"],
            'school_info': school_info,
        }

    def fetch_region(self, region: str, **kwargs):
        year = kwargs.get("year", self.run_ay)
        semester = kwargs.get("semester", 1)
        self.fetch_year(region, year, semester)

    def fetch_year(self, region: str, year: int, semester: int):
        self.iterate_schools(
            region,
            lambda sch_code, _: self._fetch_school_timetable(sch_code, year, semester)
        )

    def _fetch_school_timetable(self, school_code: str, ay: int, semester: int):
        info = self._classify_school(school_code)
        if not info:
            return
        school_info = info['school_info']
        endpoint_name = info['url'].split('/')[-1]
        sleep_time = API_CONFIG['rate_limit']['sleep_time']

        for grade in info['grades']:
            consecutive_empty = 0
            for class_nm in map(str, range(1, 21)):
                params = {
                    "ATPT_OFCDC_SC_CODE": school_info['atpt_code'],
                    "SD_SCHUL_CODE": school_code,
                    "AY": str(ay),
                    "GRADE": str(grade),
                    "CLASS_NM": class_nm,
                    "SEM": str(semester),
                }
                try:
                    rows = self._fetch_paginated(
                        info['url'], params, endpoint_name,
                        page_size=100, max_page=1,
                        region=school_info['atpt_code'],
                        year=ay
                    )
                    if not rows:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            self.logger.debug(f"  {school_code} {grade}학년: 빈 반 3회 연속 → 중단")
                            break
                        continue

                    consecutive_empty = 0
                    batch = []
                    for r in rows:
                        items = self._process_item(r)
                        if items:
                            batch.extend(items)
                    if batch:
                        self.enqueue(batch)

                except Exception as e:
                    self.logger.error(f"  {school_code} {grade}학년 {class_nm}반 실패: {e}")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break

                time.sleep(sleep_time)

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
            parsed.get("level", ""),
        )
        return [{
            "school_id":      school_info['school_id'],
            "ay":             parsed["ay"],
            "semester":       parsed["semester"],
            "grade":          parsed["grade"],
            "class_nm":       parsed["class_nm"],
            "day_of_week":    parsed["day_of_week"],
            "period":         parsed["period"],
            "subject_id":     subject_id,
            "subject_name":   parsed["subject_name"],
            "normalized_key": parsed.get("normalized_key", ""),
            "level":          parsed.get("level", ""),
            "load_dt":        now_kst().isoformat(),
        }]

    def _do_save_batch(self, conn, batch: List[dict]):
        """실제 DB 저장 로직"""
        subj_set = {
            (it['subject_id'], it['subject_name'], it['normalized_key'], it['level'])
            for it in batch if it.get('subject_id')
        }
        if subj_set:
            conn.executemany(
                "INSERT OR IGNORE INTO vocab_subject"
                " (subject_id, subject_name, normalized_key, level) VALUES (?,?,?,?)",
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
