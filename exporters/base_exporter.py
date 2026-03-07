#!/usr/bin/env python3
"""데이터 내보내기 기본 인터페이스"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class BaseExporter(ABC):
    """데이터 내보내기 추상 클래스"""

    def __init__(self, output_dir: str, filename_prefix: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.filename_prefix = filename_prefix

    @abstractmethod
    def export(self, data: List[Dict[str, Any]], metadata: Optional[Dict] = None) -> str:
        """데이터를 파일로 내보내고 생성된 파일 경로 반환"""
        pass

    def _generate_filename(self, extension: str, region: Optional[str] = None) -> str:
        """파일명 생성 (타임스탬프 + 지역 + 접두어)"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        region_part = f"_{region}" if region else ""
        return f"{self.filename_prefix}{region_part}_{timestamp}.{extension}"
        