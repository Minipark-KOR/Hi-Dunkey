#!/usr/bin/env python3
# parsers/meal.py
"""
급식 HTML 파서
"""
import re
from typing import Dict, List, Any
from core.filters import TextFilter

# ✅ 패턴을 상수로 정의하여 소실 방지
_BR_PATTERN = r'<br\s*/?>'  # HTML <br> 태그 분리자


def parse_meal_html(html_menu: str) -> Dict[str, Any]:
    result = {"items": []}
    if not html_menu:
        return result
    cleaned = re.sub(r'\([^)]*?[0-9]+(?:,[0-9]+)*\s*학년?\)', '', html_menu)
    items = re.split(_BR_PATTERN, cleaned, flags=re.IGNORECASE)
    for item in items:
        clean_item = TextFilter.clean_html(item)
        if not clean_item:
            continue
        allergy_codes = []
        allergy_match = re.search(r'\(([0-9.]+)\)$', clean_item)
        if allergy_match:
            codes = allergy_match.group(1).split('.')
            allergy_codes = [int(c) for c in codes if c.isdigit()]
            clean_item = clean_item[:allergy_match.start()].strip()
        menu_name = re.sub(r'\([^)]*\)', '', clean_item).strip()
        if not menu_name:
            continue
        result["items"].append({
            "menu_name": menu_name,
            "allergies": allergy_codes
        })
    return result


def normalize_allergy_info(allergy_codes: List[int]) -> str:
    return ",".join(str(c) for c in sorted(allergy_codes))
