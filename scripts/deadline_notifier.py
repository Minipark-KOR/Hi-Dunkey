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
rm = RetryManager()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # 예: "username/repo"


def send_github_issue(failures):
    if not failures:
        return
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.error("GitHub 토큰 또는 레포지토리가 설정되지 않았습니다.")
        return

    count = len(failures)
    domains = ", ".join(sorted(set(f["domain"] for f in failures)))
    title = f"Deadline exceeded: {count} failures after 15:00"
    body = (
        "### Deadline exceeded notification\n\n"
        f"- Total failures: {count}\n"
        f"- Domains: {domains}\n\n"
        "#### Samples (max 10)\n"
    )
    for f in failures[:10]:
        body += f"- ID: {f['id']}, {f['domain']}/{f['task_type']}, error: {f.get('error_msg', 'N/A')}\n"
    if count > 10:
        body += f"\n... and {count - 10} more"

    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    data = {"title": title, "body": body, "labels": ["deadline", "retry-failed"]}

    resp = requests.post(url, json=data, headers=headers, timeout=10)
    resp.raise_for_status()
    logger.info(f"Issue created: #{resp.json().get('number')}")


def main():
    now = now_kst()
    if getattr(now, "tzinfo", None) is not None:
        now = now.replace(tzinfo=None)

    today_3pm = datetime.combine(now.date(), time(15, 0))

    if now < today_3pm:
        logger.info("15:00 이전이므로 실행하지 않습니다.")
        return

    with rm._get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM failures
            WHERE status = 'FAILED'
              AND resolved_at IS NULL
              AND next_attempt IS NOT NULL
              AND next_attempt <= ?
            """,
            (today_3pm,),
        ).fetchall()
        failures = [dict(row) for row in rows]

    if not failures:
        logger.info("데드라인 초과로 만료 처리할 작업이 없습니다.")
        return

    try:
        send_github_issue(failures)
    except Exception as e:
        logger.error(f"GitHub 이슈 생성 실패: {e}")

    resolved_at = now  # KST naive

    with rm._get_connection() as conn:
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
    