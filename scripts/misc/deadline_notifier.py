#!/usr/bin/env python3
# scripts/deadline_notifier.py
import os
import sys
import sqlite3
import requests
from datetime import datetime, time

sys.path.append(str(Path(__file__).parent.parent.parent))

from core.engine.retry import RetryManager
from core.util.manage_log import build_domain_logger
from core.kst_time import now_kst
from constants.paths import NEIS_INFO_DB_PATH, FAILURES_DB_PATH, LOG_DIR # 추가

logger = build_domain_logger("deadline_notifier", "deadline_notifier", __file__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")


def send_github_issue(failures):
    if not failures or not GITHUB_TOKEN or not GITHUB_REPO:
        return
    count = len(failures)
    title = f"⚠️ 데드라인 초과 미처리 작업 {count} 건"
    body = f"### 미처리 작업 목록\n\n"
    for f in failures[:20]:
        body += f"- `{f['sc_code']}`: {f['address'][:50]}... ({f['error_msg']})\n"
    if count > 20:
        body += f"\n... and {count - 20} more"
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    data = {"title": title, "body": body, "labels": ["deadline", "auto-generated"]}
    try:
        # ✅ 수정: timeout 추가
        requests.post(url, headers=headers, json=data, timeout=10).raise_for_status()
        logger.info(f"Issue created")
    except Exception as e:
        logger.error(f"GitHub 이슈 생성 실패: {e}")


def main():
    rm = RetryManager(db_path=str(FAILURES_DB_PATH))
    now = now_kst().replace(tzinfo=None)
    today_3pm = datetime.combine(now.date(), time(15, 0))
    if now < today_3pm:
        logger.info("15:00 이전이므로 실행하지 않습니다.")
        return

    with rm.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM failures
            WHERE status='FAILED' AND resolved_at IS NULL
            AND next_attempt IS NOT NULL AND next_attempt < ?
            """,
            (today_3pm,)
        ).fetchall()
        failures = [dict(row) for row in rows]

    if not failures:
        logger.info("데드라인 초과 작업 없음")
        return

    send_github_issue(failures)
    with rm.get_connection() as conn:
        ids = [(now, f["id"]) for f in failures]
        conn.executemany(
            "UPDATE failures SET status='EXPIRED', resolved_at=?, error_msg=COALESCE(error_msg, 'deadline exceeded') WHERE id=?",
            ids,
        )
        conn.commit()
    logger.info(f"{len(failures)}개 작업 EXPIRED処理")


if __name__ == "__main__":
    main()
    