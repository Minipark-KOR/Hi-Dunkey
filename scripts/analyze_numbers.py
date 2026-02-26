#!/usr/bin/env python3
import sqlite3
import json
from collections import Counter
import re

def analyze_school_numbers():
    db_path = "../data/master/school_master.db"
    
    print(f"🔍 학교 DB 분석: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    total = conn.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
    print(f"📊 전체 학교: {total}개")
    
    with_addr = conn.execute("""
        SELECT COUNT(*) FROM schools 
        WHERE address IS NOT NULL AND address != ''
    """).fetchone()[0]
    print(f"📮 주소 있음: {with_addr}개 ({with_addr/total*100:.1f}%)")
    
    cur = conn.execute("""
        SELECT address FROM schools
        WHERE address IS NOT NULL AND address != ''
    """)
    
    number_counter = Counter()
    
    for row in cur:
        address = row[0]
        numbers = re.findall(r'\b(\d{2,4})\b', address)
        for num in numbers:
            if 100 <= int(num) <= 9999:
                number_counter[num] += 1
    
    print("\n📊 번호별 등장 횟수 TOP 50")
    print("=" * 60)
    print(f"{'순위':4} {'번호':6} {'횟수':6}")
    print("-" * 60)
    
    frequent_numbers = []
    for i, (num, freq) in enumerate(number_counter.most_common(50), 1):
        print(f"{i:4} {num:6} {freq:6}")
        frequent_numbers.append(int(num))
    
    top_32 = frequent_numbers[:32]
    
    print("\n✅ FREQUENT_NUMBERS = [")
    for num in top_32:
        print(f"    {num},")
    print("]")
    
    with open("../data/frequent_numbers.json", "w") as f:
        json.dump(top_32, f, indent=2)
    
    print(f"\n💾 저장 완료: ../data/frequent_numbers.json")
    conn.close()

if __name__ == "__main__":
    analyze_school_numbers()
