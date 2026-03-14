#!/usr/bin/env python3
# core/data_validator.py
# 최종 수정: PRIMARY KEY를 schema["primary_key"]에서 직접 가져옴

from typing import List, Dict, Tuple, Optional


class DataValidator:
    """데이터 무결성 검증 (컬럼 누락, PK null 체크 등)"""

    @staticmethod
    def validate_batch(batch: List[Dict], expected_columns: List[str],
                        pk_columns: Optional[List[str]] = None) -> Tuple[bool, List[str], List[str]]:
        """
        배치 전체 검증
        - pk_columns: PRIMARY KEY 컬럼 목록 (None이면 expected_columns[0] 사용)
        Returns: (통과 여부, 오류 목록, 경고 목록)
        """
        if pk_columns is None:
            pk_columns = [expected_columns[0]] if expected_columns else []

        errors = []
        warnings = []

        for i, row in enumerate(batch):
            # 필수 컬럼 누락 경고
            missing = [col for col in expected_columns if col not in row]
            if missing:
                warnings.append(f"Row {i}: 누락된 컬럼 {missing} (NULL 저장됨)")

            # PRIMARY KEY null 체크
            for pk in pk_columns:
                if pk in row and row[pk] is None:
                    errors.append(f"Row {i}: PRIMARY KEY '{pk}'가 NULL입니다.")

        return len(errors) == 0, errors, warnings
        