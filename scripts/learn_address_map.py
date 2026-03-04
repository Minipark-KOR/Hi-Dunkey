#!/usr/bin/env python3
# scripts/learn_address_map.py
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List

LOG_FILE = "logs/address_mapping.log"
OUTPUT_FILE = "address_mapping_stats.txt"


def load_mappings(log_file: str) -> List[Dict]:
    mappings = []
    if not Path(log_file).exists():
        return mappings
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(" - INFO - ", 1)
            if len(parts) >= 2:
                try:
                    data = json.loads(parts[1])
                    mappings.append(data)
                except json.JSONDecodeError:
                    continue
    return mappings


def analyze_mappings(mappings: List[Dict]) -> Dict:
    from_to = Counter()
    for m in mappings:
        key = (m['original'], m['mapped'])
        from_to[key] += 1

    top20 = from_to.most_common(20)
    suggestions = []
    for (orig, mapped), cnt in top20:
        if orig.endswith('면') and mapped.endswith('읍') and orig[:-1] == mapped[:-1]:
            suggestions.append((orig, mapped, cnt))
        if '광역시' in orig and ' ' in mapped and mapped.startswith(orig.split('광역시')[0]):
            suggestions.append((orig, mapped, cnt))
        if '번지' in orig and '번지' not in mapped:
            suggestions.append((orig, mapped, cnt))

    return {
        "total": len(mappings),
        "unique": len(from_to),
        "top20": top20,
        "suggestions": suggestions
    }


def main():
    if not Path(LOG_FILE).exists():
        print(f"❌ 로그 파일 없음: {LOG_FILE}")
        return
    mappings = load_mappings(LOG_FILE)
    if not mappings:
        print("ℹ️  변환 기록 없음")
        return

    stats = analyze_mappings(mappings)
    # ✅ 수정: literal newline → \n
    print(f"\n📊 학습 결과:")
    print(f"  총 변환 기록: {stats['total']} 개")
    print(f"  고유 변환 쌍: {stats['unique']} 개")

    if stats['top20']:
        print("\n🔝 자주 발생한 변환 Top 5:")
        for (orig, mapped), cnt in stats['top20'][:5]:
            print(f"  {cnt} 회: '{orig[:40]}...' → '{mapped[:40]}...'")

    if stats['suggestions']:
        print("\n✅ ADMIN_DISTRICT_MAP 업데이트 제안:")
        for orig, mapped, cnt in stats['suggestions']:
            print(f'    "{orig}": "{mapped}",  # {cnt} 회 발생')

    # 결과 저장
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("# AddressFilter 업데이트 제안\n")
        for orig, mapped, cnt in stats['suggestions']:
            f.write(f'    "{orig}": "{mapped}",  # {cnt} 회 발생\n')


if __name__ == "__main__":
    main()
    