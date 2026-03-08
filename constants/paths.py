# constants/paths.py (수정본)
# 설정 기반 경로 상수
from pathlib import Path
from core.config import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 설정에서 경로 가져오기 (없으면 기본값)
ACTIVE_DIR = PROJECT_ROOT / config.get('paths', 'active_dir', default='data/active')
MASTER_DIR = PROJECT_ROOT / config.get('paths', 'master_dir', default='data/master')
METRICS_DIR = PROJECT_ROOT / config.get('paths', 'metrics_dir', default='data/metrics')
LOG_DIR = PROJECT_ROOT / config.get('paths', 'logs_dir', default='logs')

GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
UNKNOWN_DB_PATH = str(ACTIVE_DIR / "unknown_patterns.db")
MASTER_DB_PATH = str(MASTER_DIR / "neis_info.db")

# 필요시 디렉토리 생성
ACTIVE_DIR.mkdir(parents=True, exist_ok=True)
MASTER_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
