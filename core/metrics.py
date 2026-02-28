#!/usr/bin/env python3
"""
공통 메트릭 생성 모듈
- collector 실행 후 또는 백업 시점에 호출하여 요약 생성
- build_summary_markdown: 순수 함수, 마크다운 문자열 반환
- save_summary: 파일 저장 전담
- cleanup_old_metrics: 오래된 메트릭 파일 정리
"""
import os
import sqlite3
from typing import Dict, List, Optional


# ──────────────────────────────────────────
# 수집 함수
# ──────────────────────────────────────────

def collect_domain_metrics(db_path: str, table: str) -> Dict:
    """단일 도메인 DB의 레코드 수와 파일 크기 반환"""
    if not os.path.exists(db_path):
        return {"rows": None, "size_bytes": 0, "error": "파일 없음"}
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {"rows": rows, "size_bytes": os.path.getsize(db_path)}
    except Exception as e:
        return {"rows": None, "size_bytes": 0, "error": str(e)}


def collect_global_db_metrics(global_dbs: List[Dict], base_dir: str) -> Dict:
    """GLOBAL_DBS 목록을 받아 모든 테이블의 레코드 수 수집 (시스템 테이블 제외)"""
    result = {}
    for db_info in global_dbs:
        db_path = os.path.join(base_dir, db_info["path"])
        db_name = db_info["name"]
        if not os.path.exists(db_path):
            result[db_name] = {"exists": False}
            continue
        try:
            with sqlite3.connect(db_path) as conn:
                table_list = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                tables = {}
                for (t_name,) in table_list:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM {t_name}"
                    ).fetchone()[0]
                    tables[t_name] = count
            result[db_name] = {
                "exists":     True,
                "size_bytes": os.path.getsize(db_path),
                "tables":     tables,
            }
        except Exception as e:
            result[db_name] = {"exists": False, "error": str(e)}
    return result


def collect_school_geo_stats(school_db_path: str) -> Dict:
    """학교 DB에서 전체 학교 수와 좌표 확보율 계산"""
    if not os.path.exists(school_db_path):
        return {"total": 0, "with_geo": 0, "percent": 0.0}
    try:
        with sqlite3.connect(school_db_path) as conn:
            total    = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
            with_geo = conn.execute(
                "SELECT COUNT(*) FROM schools "
                "WHERE longitude IS NOT NULL AND latitude IS NOT NULL"
            ).fetchone()[0]
            percent  = round(with_geo / total * 100, 1) if total > 0 else 0.0
        return {"total": total, "with_geo": with_geo, "percent": percent}
    except Exception as e:
        return {"total": 0, "with_geo": 0, "percent": 0.0, "error": str(e)}


# ──────────────────────────────────────────
# 포맷팅 (순수 함수)
# ──────────────────────────────────────────

def build_summary_markdown(
    backup_date:           str,
    base_dir:              str,
    domain_config:         Dict,
    global_dbs:            List[Dict],
    include_geo:           bool = True,
    include_global_tables: bool = True,
) -> str:
    """
    GitHub Step Summary용 마크다운 문자열 생성 (순수 함수).
    파일 저장은 하지 않음.
    """
    lines = [f"### 📊 백업 메트릭 요약 ({backup_date})", ""]

    # 1. 학교 좌표 현황
    if include_geo and "school" in domain_config:
        school_db = os.path.join(base_dir, domain_config["school"]["db_path"])
        geo       = collect_school_geo_stats(school_db)
        if "error" not in geo:
            lines.append(f"🏫 전체 학교 수: **{geo['total']:,}개**")
            lines.append(
                f"📍 좌표 확보: **{geo['with_geo']:,}개** ({geo['percent']}%)"
            )
        else:
            lines.append(f"⚠️ 학교 좌표 통계 오류: {geo['error']}")
        lines.append("")

    # 2. 글로벌 DB 테이블 현황
    if include_global_tables:
        global_stats = collect_global_db_metrics(global_dbs, base_dir)
        for db_name, info in global_stats.items():
            if not info.get("exists"):
                lines.append(f"📦 `{db_name}`: 파일 없음")
                continue
            mb = info.get("size_bytes", 0) / (1024 * 1024)
            lines.append(f"📦 `{db_name}` ({mb:.1f} MB)")
            for t_name, count in info.get("tables", {}).items():
                lines.append(f"&nbsp;&nbsp;&nbsp;&nbsp;- `{t_name}`: {count:,}건")
        lines.append("")

    # 3. 도메인별 레코드 수 테이블
    lines += [
        "| 도메인 | 레코드 수 | 파일 크기(MB) |",
        "| --- | ---: | ---: |",
    ]
    for name, cfg in domain_config.items():
        if not cfg.get("enabled", True):
            continue
        db_path = os.path.join(base_dir, cfg["db_path"])
        m       = collect_domain_metrics(db_path, cfg["table"])
        if m["rows"] is not None:
            mb = m["size_bytes"] / (1024 * 1024)
            lines.append(f"| {name} | {m['rows']:,} | {mb:.2f} |")
        else:
            lines.append(f"| {name} | ❌ 오류 | - |")

    return "\n".join(lines)


# ──────────────────────────────────────────
# 저장 함수
# ──────────────────────────────────────────

def save_summary(markdown: str, metrics_dir: str, backup_date: str) -> str:
    """마크다운 문자열을 summary_{backup_date}.txt로 저장. 경로 반환."""
    os.makedirs(metrics_dir, exist_ok=True)
    path = os.path.join(metrics_dir, f"summary_{backup_date}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(markdown)
    return path


# ──────────────────────────────────────────
# 정리 함수
# ──────────────────────────────────────────

def cleanup_old_metrics(metrics_dir: str, keep: int = 12):
    """
    metrics_dir 내 오래된 메트릭/요약 파일 정리.
    날짜 오름차순 정렬 후 최근 keep개만 보관.
    """
    if not os.path.exists(metrics_dir):
        return

    targets = [
        f for f in os.listdir(metrics_dir)
        if f.startswith(("metrics_", "summary_"))
        and f.endswith((".json", ".txt"))
    ]

    # 날짜 접두어 기준 그룹핑
    dated = []
    for fname in targets:
        try:
            date_str = fname.split("_")[1].split(".")[0]  # metrics_20260222.json → 20260222
            dated.append((date_str, fname))
        except IndexError:
            continue

    dated.sort()
    to_delete = dated[:-keep] if len(dated) > keep else []
    for _, fname in to_delete:
        os.remove(os.path.join(metrics_dir, fname))
