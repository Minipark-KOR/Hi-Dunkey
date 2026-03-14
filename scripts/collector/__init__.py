#!/usr/bin/env python3
# scripts/collector/__init__.py
# 수집기 자동 등록 시스템

import importlib
import pkgutil
from pathlib import Path
from typing import Dict, Type

# 전역 캐시
_COLLECTOR_MAP: Dict[str, Type] = None


def get_collector_map() -> Dict[str, Type]:
    """
    scripts/collector/ 디렉토리에서 CollectorEngine 을 상속한
    모든 클래스를 자동 탐색하여 등록합니다.
    """
    global _COLLECTOR_MAP
    if _COLLECTOR_MAP is not None:
        return _COLLECTOR_MAP
    
    collector_map = {}
    collector_dir = Path(__file__).parent
    
    for module_info in pkgutil.iter_modules([str(collector_dir)]):
        # 언더스코어 시작 모듈 제외 (__init__, _util 등)
        if module_info.name.startswith('_'):
            continue
        
        try:
            # 모듈 동적 임포트
            module = importlib.import_module(f"scripts.collector.{module_info.name}")
            
            # 모듈 내 모든 클래스 검사
            for name in dir(module):
                obj = getattr(module, name)
                
                # CollectorEngine 상속 클래스만 필터링
                # (schema_name, table_name 이 있는 클래스 = 수집기)
                if (isinstance(obj, type) and
                    obj.__module__ == module.__name__ and
                    hasattr(obj, 'schema_name') and
                    hasattr(obj, 'table_name')):

                    # collector_name이 있으면 우선 사용 (중앙 이름 관리), 없으면 모듈명 fallback
                    key = getattr(obj, 'collector_name', None) or module_info.name
                    if key in collector_map and collector_map[key] is not obj:
                        print(f"⚠️ 수집기 키 중복 감지: {key} (모듈: {module_info.name})")
                    collector_map[key] = obj
                    
        except Exception as e:
            print(f"⚠️ 수집기 로드 실패: {module_info.name} - {e}")
    
    _COLLECTOR_MAP = collector_map
    return collector_map


def get_registered_collectors() -> Dict[str, Type]:
    """캐시된 수집기 맵 반환"""
    return get_collector_map()


def reload_collectors():
    """수집기 맵 강제 새로고침 (테스트용)"""
    global _COLLECTOR_MAP
    _COLLECTOR_MAP = None
    return get_collector_map()


# 하위 호환성을 위한 별칭
COLLECTOR_MAP = get_registered_collectors()
