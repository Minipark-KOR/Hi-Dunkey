#!/usr/bin/env python3
"""
학년 표시 파서
"""
import re
import logging
from typing import List, Tuple

from constants.codes import E_KEYS, M_KEYS, H_KEYS, GRADE_CODES

logger = logging.getLogger(__name__)


class GradeDisplayFormatter:
    @staticmethod
    def format_grades(grade_indices: List[int], prefix: str = "") -> str:
        if not grade_indices:
            return ""
        try:
            grade_str = ",".join(str(g) for g in sorted(grade_indices))
            return f"[{prefix}{grade_str}]" if prefix else f"[{grade_str}]"
        except Exception as e:
            logger.error(f"format_grades 오류: {e}")
            return ""

    @staticmethod
    def extract_grade_indices(grade_disp: str) -> List[int]:
        if not isinstance(grade_disp, str):
            return []
        try:
            match = re.search(r'\[(?:\D*)?([0-9,]+)\]', grade_disp)
            if match:
                return [int(x.strip()) for x in match.group(1).split(',')]
        except Exception as e:
            logger.warning(f"extract_grade_indices 오류: {e}")
        return []

    @staticmethod
    def extract_prefix(grade_disp: str) -> str:
        if not isinstance(grade_disp, str):
            return ""
        try:
            match = re.search(r'\[([^\d\]]*)', grade_disp)
            return match.group(1) if match else ""
        except Exception:
            return ""

    @staticmethod
    def _group_consecutive(indices: List[int]) -> List[List[int]]:
        if not indices:
            return []
        indices = sorted(indices)
        groups = []
        current = [indices[0]]
        for i in indices[1:]:
            if i == current[-1] + 1:
                current.append(i)
            else:
                groups.append(current)
                current = [i]
        groups.append(current)
        return groups


def analyze_grade_display(row: dict, is_special: bool) -> Tuple[str, str]:
    """
    NEIS 학사일정 row에서 학년 표시 문자열과 원본 플래그 문자열을 생성합니다.
    """
    # 플래그 추출 (각 학년/과정별 이벤트 여부)
    flags = [1 if row.get("TK_GRADE_EVENT_YN") == "Y" else 0]
    flags += [1 if row.get(f"{k}_GRADE_EVENT_YN") == "Y" else 0 for k in E_KEYS + M_KEYS + H_KEYS]
    flags.append(1 if row.get("JC_GRADE_EVENT_YN") == "Y" else 0)

    grade_raw = "".join(map(str, flags))

    try:
        if not is_special:
            indices = [i + 1 for i, f in enumerate(flags) if f]
            grade_disp = GradeDisplayFormatter.format_grades(indices)
        else:
            disp_parts = []
            if flags[0]:  # 유치원
                class_nm = (row.get("CLASS_NM") or "").strip()
                if class_nm:
                    disp_parts.append(f"[유-{class_nm[0]}]")
                else:
                    disp_parts.append("[유]")
            e_indices = [i + 1 for i, f in enumerate(flags[1:7]) if f]
            if e_indices:
                groups = GradeDisplayFormatter._group_consecutive(e_indices)
                for g in groups:
                    if len(g) == 1:
                        disp_parts.append(f"[초{g[0]}]")
                    else:
                        disp_parts.append(f"[초{g[0]},{g[-1]}]")
            m_indices = [i + 1 for i, f in enumerate(flags[7:10]) if f]
            if m_indices:
                groups = GradeDisplayFormatter._group_consecutive(m_indices)
                for g in groups:
                    if len(g) == 1:
                        disp_parts.append(f"[중{g[0]}]")
                    else:
                        disp_parts.append(f"[중{g[0]},{g[-1]}]")
            h_indices = [i + 1 for i, f in enumerate(flags[10:13]) if f]
            if h_indices:
                groups = GradeDisplayFormatter._group_consecutive(h_indices)
                for g in groups:
                    if len(g) == 1:
                        disp_parts.append(f"[고{g[0]}]")
                    else:
                        disp_parts.append(f"[고{g[0]},{g[-1]}]")
            if flags[-1]:  # 전공과
                disp_parts.append("[전]")
            grade_disp = "".join(disp_parts)
    except Exception as e:
        logger.error(f"analyze_grade_display 오류: {e}", exc_info=True)
        grade_disp = ""
        grade_raw = ""

    return grade_disp, grade_raw


def get_grade_codes(row: dict) -> List[int]:
    """
    학년별 이벤트 여부를 GRADE_CODES 매핑에 따라 정수 코드 리스트로 반환합니다.
    """
    codes = []
    try:
        codes = [code for key, code in GRADE_CODES.items() if row.get(f"{key}_GRADE_EVENT_YN") == "Y"]
    except Exception as e:
        logger.error(f"get_grade_codes 오류: {e}")
    return codes if codes else [0]
    