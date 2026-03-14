#!/usr/bin/env python3
"""필터 호환 진입점. 실제 구현은 core.util.filters를 사용."""

from core.util.filters import SubjectNameFilter, TextFilter

__all__ = ["TextFilter", "SubjectNameFilter"]
        