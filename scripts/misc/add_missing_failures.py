#!/usr/bin/env python3
"""
좌표가 NULL인 학교를 failures 테이블에 추가 (재시도 대상)
"""
import sqlite3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.engine.retry import RetryManager
from constants.paths import NEIS_INFO_DB_PATH

rm = RetryManager()
conn = sqlite3.connect(str(NEIS_INFO_DB_PATH))
conn.row_factory = sqlite3.Row

cur = conn.execute("SELECT sc_code, address, atpt_code FROM schools WHERE latitude IS NULL")
rows = cur.fetchall()
print(f"📌 좌표 누락 학교: {len(rows)}개")

for row in rows:
    # 학교 코드 마지막 자리로 샤드 결정
    last_digit = int(row['sc_code'][-1])
    shard = "even" if last_digit % 2 == 0 else "odd"
    
    rm.record_failure(
        domain='geo',
        task_type='geocode',
        shard=shard,
        sc_code=row['sc_code'],
        region=row['atpt_code'],
        address=row['address'],
        error="수동 추가: 좌표 누락"
    )
    print(f"✅ 추가됨: {row['sc_code']} ({row['address'][:30]}...)")

conn.close()
print(f"\n🎉 총 {len(rows)}개 작업이 failures 테이블에 추가되었습니다.")
