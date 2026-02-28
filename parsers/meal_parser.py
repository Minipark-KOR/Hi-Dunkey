#!/usr/bin/env python3
"""
급식 HTML 파서 (알레르기 정보 추출, 학년 정보 무시)
"""
import re
from typing import Dict, List, Any

from core.filters import TextFilter
from core.id_generator import IDGenerator

def parse_meal_html(html_menu: str) -> Dict[str, Any]:
    """
    급식 HTML 파싱
    반환:
        {
            "items": [{"menu_id": int, "allergies": List[int]}, ...],
            "vocab": {menu_id: menu_name, ...}
        }
    """
    result = {
        "items": [],
        "vocab": {}
    }
    if not html_menu:
        return result
    
    # 학년 정보 패턴 제거 (예: (1,2학년) 등)
    cleaned = re.sub(r'\([^)]*?[0-9]+(?:,[0-9]+)*\s*학년?\)', '', html_menu)
    
    # <br/> 태그로 분리
    items = re.split(r'<br\s*/>|<br>', cleaned, flags=re.IGNORECASE)
    
    for item in items:
        clean_item = TextFilter.clean_html(item)
        if not clean_item:
            continue
        
        # 알레르기 정보 추출 (예: (1.5.6))
        allergy_codes = []
        allergy_match = re.search(r'\(([0-9.]+)\)$', clean_item)
        if allergy_match:
            codes = allergy_match.group(1).split('.')
            allergy_codes = [int(c) for c in codes if c.isdigit()]
            clean_item = clean_item[:allergy_match.start()].strip()
        
        # 메뉴명 정제 (괄호 안 내용 제거)
        menu_name = re.sub(r'\([^)]*\)', '', clean_item).strip()
        if not menu_name:
            continue
        
        menu_id = IDGenerator.text_to_int(menu_name, namespace="meal")
        
        result["items"].append({
            "menu_id": menu_id,
            "allergies": allergy_codes
        })
        result["vocab"][menu_id] = menu_name
    
    return result


def normalize_allergy_info(allergy_codes: List[int]) -> str:
    """알레르기 코드 리스트를 저장용 문자열로 변환 (예: "1,2,5")"""
    return ",".join(str(c) for c in sorted(allergy_codes))
    