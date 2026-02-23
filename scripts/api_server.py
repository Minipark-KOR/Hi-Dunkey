#!/usr/bin/env python3
"""
FastAPI 기반 API 서버
실행: uvicorn api_server:app --reload --host 0.0.0.0 --port 8000
"""
import os
import sys
import sqlite3
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# 프로젝트 루트를 path에 추가 (core 모듈 임포트용)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import get_db_connection  # ✅ 컨텍스트 매니저 사용

app = FastAPI(title="NEIS 데이터 API", description="급식, 학사일정, 시간표 API")

# CORS 설정 (개발용, 배포 시 특정 도메인으로 변경)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 배포 시 프론트엔드 도메인으로 변경 (예: ["https://school.hi-dunkey.com"])
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 절대 경로로 변경
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 프로젝트 루트
DATA_DIR = os.path.join(BASE_DIR, "data", "active")
MASTER_DB = os.path.join(BASE_DIR, "data", "master", "school_master.db")

# =====================[ 유틸리티 함수 ]=====================
def get_school_id(school_code: str) -> Optional[int]:
    """학교 코드로 school_id 조회 (master DB 사용)"""
    if not os.path.exists(MASTER_DB):
        return None
    with get_db_connection(MASTER_DB) as conn:
        cur = conn.execute("SELECT school_id FROM schools WHERE sc_code = ?", (school_code,))
        row = cur.fetchone()
    return row[0] if row else None

def get_shard(school_code: str) -> str:
    """학교 코드로 샤드 결정"""
    try:
        last_digit = int(school_code[-1])
        return "even" if last_digit % 2 == 0 else "odd"
    except:
        return "odd"

# =====================[ API 엔드포인트 ]=====================
@app.get("/api/schedule")
def get_schedule(
    school_code: str = Query(..., description="학교 코드"),
    year: int = Query(..., description="학년도")
):
    """학사일정 조회"""
    school_id = get_school_id(school_code)
    if not school_id:
        raise HTTPException(status_code=404, detail="School not found")
    
    shard = get_shard(school_code)
    db_path = os.path.join(DATA_DIR, f"schedule_{shard}.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Schedule data not found")
    
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT s.ev_date, v.ev_nm, s.grade_disp, s.sub_yn, s.dn_yn, s.ev_content
            FROM schedule s
            JOIN vocab_event v ON s.ev_id = v.ev_id
            WHERE s.school_id = ? AND s.ay = ?
            ORDER BY s.ev_date
        """, (school_id, year))
        rows = cur.fetchall()
    
    events = []
    for r in rows:
        date_str = str(r["ev_date"])
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        events.append({
            "title": r["ev_nm"],
            "start": formatted_date,
            "extendedProps": {
                "grade_disp": r["grade_disp"],
                "sub_yn": r["sub_yn"],
                "dn_yn": r["dn_yn"],
                "ev_content": r["ev_content"]
            }
        })
    return events

@app.get("/api/meal")
def get_meal(
    school_code: str = Query(..., description="학교 코드"),
    date: str = Query(..., description="날짜 (YYYYMMDD)")
):
    """급식 조회 (특정 날짜)"""
    school_id = get_school_id(school_code)
    if not school_id:
        raise HTTPException(status_code=404, detail="School not found")
    
    shard = get_shard(school_code)
    db_path = os.path.join(DATA_DIR, f"meal_{shard}.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Meal data not found")
    
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT m.meal_type, v.menu_name, m.allergy_info
            FROM meal m
            JOIN vocab_meal v ON m.menu_id = v.menu_id
            WHERE m.school_id = ? AND m.meal_date = ?
            ORDER BY m.meal_type
        """, (school_id, int(date)))
        rows = cur.fetchall()
    
    meals = {}
    for r in rows:
        meal_type = r["meal_type"]
        meals.setdefault(meal_type, []).append({
            "menu": r["menu_name"],
            "allergies": r["allergy_info"].split(",") if r["allergy_info"] else []
        })
    return meals

@app.get("/api/timetable")
def get_timetable(
    school_code: str = Query(..., description="학교 코드"),
    ay: int = Query(..., description="학년도"),
    semester: int = Query(1, description="학기"),
    grade: int = Query(..., description="학년"),
    class_nm: str = Query(..., description="반")
):
    """시간표 조회"""
    school_id = get_school_id(school_code)
    if not school_id:
        raise HTTPException(status_code=404, detail="School not found")
    
    shard = get_shard(school_code)
    db_path = os.path.join(DATA_DIR, f"timetable_{shard}.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail="Timetable data not found")
    
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT t.day_of_week, t.period, v.subject_name, w.teacher_name
            FROM timetable t
            JOIN vocab_subject v ON t.subject_id = v.subject_id
            LEFT JOIN vocab_teacher w ON t.teacher_id = w.teacher_id
            WHERE t.school_id = ? AND t.ay = ? AND t.semester = ?
              AND t.grade = ? AND t.class_nm = ?
            ORDER BY t.day_of_week, t.period
        """, (school_id, ay, semester, grade, class_nm))
        rows = cur.fetchall()
    
    timetable = {}
    for r in rows:
        day = r["day_of_week"]
        timetable.setdefault(day, []).append({
            "period": r["period"],
            "subject": r["subject_name"],
            "teacher": r["teacher_name"] or ""
        })
    return timetable

@app.get("/api/schools")
def search_schools(
    query: str = Query(..., min_length=2, description="검색어 (학교명 또는 코드)")
):
    """학교 검색"""
    if not os.path.exists(MASTER_DB):
        raise HTTPException(status_code=500, detail="Master DB not found")
    
    with get_db_connection(MASTER_DB) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute("""
            SELECT sc_code, sc_name, atpt_code
            FROM schools
            WHERE sc_name LIKE ? OR sc_code LIKE ?
            LIMIT 20
        """, (f"%{query}%", f"%{query}%"))
        rows = cur.fetchall()
    
    return [dict(r) for r in rows]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    