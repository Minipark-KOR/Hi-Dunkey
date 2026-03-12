#!/usr/bin/env python3
# core/data_validator.py
# 배치 데이터 유효성 검증

from typing import List, Dict, Tuple, Any, Optional


class DataValidator:
    """데이터 무결성 검증 (컬럼 누락, 타입 경고 등)"""

    @staticmethod
    def validate_batch(batch: List[Dict], expected_columns: List[str]) -> Tuple[bool, List[str], List[str]]:
        """
        배치 전체 검증
        Returns: (통과 여부, 오류 목록, 경고 목록)
        """
        errors = []
        warnings = []

        for i, row in enumerate(batch):
            valid, err, warn = DataValidator.validate_row(row, expected_columns, i)
            if not valid:
                errors.extend(err)
            warnings.extend(warn)

        return len(errors) == 0, errors, warnings

    @staticmethod
    def validate_row(row: Dict, expected_columns: List[str], idx: int) -> Tuple[bool, List[str], List[str]]:
        """
        단일 행 검증
        Returns: (통과 여부, 오류 목록, 경고 목록)
        """
        errors = []
        warnings = []

        # 필수 컬럼 존재 여부 (여기서는 모든 컬럼이 없으면 경고, 오류는 PRIMARY KEY 등)
        missing = [col for col in expected_columns if col not in row]
        if missing:
            warnings.append(f"Row {idx}: 누락된 컬럼 {missing} (NULL 저장됨)")

        # PRIMARY KEY 컬럼이 None인지 체크 (간단히 첫 번째 컬럼이 PK라고 가정)
        if expected_columns:
            pk_col = expected_columns[0]
            if pk_col in row and row[pk_col] is None:
                errors.append(f"Row {idx}: PRIMARY KEY '{pk_col}'가 NULL입니다.")

        # 타입 체크 (선택) - REAL, INTEGER 등
        # 생략 (실제로는 스키마 정보를 더 활용해야 함)

        return len(errors) == 0, errors, warnings
        