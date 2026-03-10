#!/usr/bin/env python3
"""
급식 HTML 파서
"""
import re
import logging
from typing import Dict, List, Any

from core.filters import TextFilter

logger = logging.getLogger(__name__)

# HTML <br> 태그 분리자
_BR_PATTERN = r'<br\s*/?>'


def parse_meal_html(html_menu: str) -> Dict[str, Any]:
    """
    급식 HTML 문자열을 파싱하여 메뉴 항목 리스트를 반환합니다.
    """
    result = {"items": []}
    if not html_menu or not isinstance(html_menu, str):
        return result

    try:
        # 괄호 안 학년 정보 제거 (예: (1,2학년))
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
    except Exception as e:
        logger.error(f"급식 HTML 파싱 중 오류: {e}", exc_info=True)

    return result


def normalize_allergy_info(allergy_codes: List[int]) -> str:
    """
    알레르기 코드 리스트를 콤마로 구분된 문자열로 정규화합니다.
    """
    if not allergy_codes:
        return ""
    try:
        return ",".join(str(c) for c in sorted(allergy_codes))
    except Exception as e:
        logger.error(f"알레르기 코드 정규화 오류: {e}")
        return ""
        