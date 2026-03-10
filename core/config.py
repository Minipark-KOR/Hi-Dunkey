# core/config.py
# 설정 파일 로더
import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional

class Config:
    _instance = None
    _config: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        if not config_path.exists():
            # 기본 설정 (파일이 없을 경우 하드코딩된 fallback)
            self._config = self._default_config()
        else:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        # 환경변수로 오버라이드 가능하게 (예: CONFIG__PATHS__MASTER_DIR)
        self._override_from_env()

    def _default_config(self):
        # 주의: MASTER_DIR, ACTIVE_DIR 등은 constants.paths에서 정의되므로
        # 여기서는 기본값만 제공 (실제 경로는 paths.py에서 관리)
        return {
            "paths": {
                "master_dir": "data/master",
                "active_dir": "data/active",
                "logs_dir": "logs",
                "metrics_dir": "metrics",
            },
            "collectors": {},
            "api": {}
        }

    def _override_from_env(self):
        """환경변수로 설정 오버라이드 (예: CONFIG__PATHS__MASTER_DIR=/new/path)"""
        prefix = "CONFIG__"
        for key, value in os.environ.items():
            if key.startswith(prefix):
                parts = key[len(prefix):].lower().split("__")
                target = self._config
                for part in parts[:-1]:
                    target = target.setdefault(part, {})
                target[parts[-1]] = value

    def get(self, *keys, default=None):
        """중첩된 설정값 조회 (예: config.get('paths', 'master_dir'))"""
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value

    def get_collector_config(self, name: str) -> Dict[str, Any]:
        """특정 수집기의 설정 반환 (없으면 빈 딕셔너리)"""
        return self._config.get("collectors", {}).get(name, {})

    def get_api_key(self, name: str) -> str:
        """
        API 키 조회.
        설정 파일의 api 섹션에서 '{name}_api_key_env'에 해당하는 환경변수명을 찾아 값을 반환.
        예: get_api_key('neis') → config['api']['neis_api_key_env'] 환경변수 값
        """
        env_var = self.get('api', f'{name}_api_key_env')
        if env_var:
            return os.environ.get(env_var, '').strip()
        return ''


# 싱글톤 인스턴스
config = Config()
