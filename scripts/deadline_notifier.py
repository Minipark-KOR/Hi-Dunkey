#!/usr/bin/env python3
# scripts/deadline_notifier.py
import os
import sys
import sqlite3
import requests
from datetime import datetime, time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.retry import RetryManager
from core.logger import build_logger
from core.kst_time import now_kst

logger = build_logger("deadline_notifier", "logs/deadline_notifier.log")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")


def send_github_issue(failures):
    if not failures:
        return
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.error("GitHub 토큰 또는 레포지토리가 설정되지 않았습니다.")
        return
    
    count = len(failures)
    domains = ", ".join(sorted(set(f["domain"] for f in failures)))
    
    # ✅ 수정: 명시적 \n 사용
    title = f"Deadline exceeded: {count} failures after 15:00"
    body_lines = [
        "### Deadline exceeded notification",
        "",
        f"- **Total failures:** {count}",
        f"- **Domains:** {domains}",
        "",
        "#### Samples (max 10)",
        ""
    ]
    
    for f in failures[:10]:
        error_msg = f.get('error_msg', 'N/A')[:100]  # 너무 길면 자르기
        body_lines.append(f"- ID: {f['id']}, {f['domain']}/{f['task_type']}, error: {error_msg}")
    
    if count > 10:
        body_lines.append("")
        body_lines.append(f"... and {count - 10} more")
    
    body = "\n".join(body_lines)
    
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    data = {"title": title, "body": body, "labels": ["deadline", "retry-failed"]}
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=10)
        resp.raise_for_status()
        logger.info(f"Issue created: #{resp.json().get('number')}")
    except Exception as e:
        logger.error(f"GitHub 이슈 생성 실패: {e}", exc_info=True)


def main():
    rm = RetryManager(db_path="data/failures.db")
    now = now_kst()
    if getattr(now, "tzinfo", None) is not None:
        now = now.replace(tzinfo=None)
    
    today_3pm = datetime.combine(now.date(), time(15, 0))
    
    if now < today_3pm:
        logger.info("15:00 이전이므로 실행하지 않습니다.")
        return
    
    # 데드라인 초과된 작업만 조회 (next_attempt < 15:00)
    with rm.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM failures
            WHERE status = 'FAILED'
            AND resolved_at IS NULL
            AND next_attempt IS NOT NULL
            AND next_attempt < ?
            """,
            (today_3pm,)
        ).fetchall()
        
        failures = [dict(row) for row in rows]
        
        if not failures:
            logger.info("데드라인 초과로 만료 처리할 작업이 없습니다.")
            return
        
        try:
            send_github_issue(failures)
        except Exception as e:
            logger.error(f"GitHub 이슈 생성 실패: {e}", exc_info=True)
        
        resolved_at = now
        with rm.get_connection() as conn:
            ids = [(resolved_at, f["id"]) for f in failures]
            conn.executemany(
                """
                UPDATE failures
                SET status='EXPIRED',
                resolved_at=?,
                error_msg=COALESCE(error_msg, 'deadline exceeded')
                WHERE id=? AND status='FAILED' AND resolved_at IS NULL
                """,
                ids,
            )
            conn.commit()
        
        logger.info(f"{len(failures)}개 작업을 EXPIRED 처리했습니다.")


if __name__ == "__main__":
    main()
    