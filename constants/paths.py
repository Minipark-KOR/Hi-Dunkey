# constants/paths.py
from pathlib import Path
from core.config import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# data/ 아래 모든 디렉토리 정의
DATA_DIR = PROJECT_ROOT / "data"

# 설정 파일에서 경로를 읽되, 없으면 기본값 사용
ACTIVE_DIR = DATA_DIR / config.get('paths', 'active_dir', default='active')
MASTER_DIR = DATA_DIR / config.get('paths', 'master_dir', default='master')
LOG_DIR = DATA_DIR / config.get('paths', 'logs_dir', default='logs')
METRICS_DIR = DATA_DIR / config.get('paths', 'metrics_dir', default='metrics')
TEMP_DIR = DATA_DIR / config.get('paths', 'temp_dir', default='temp')
CACHE_DIR = DATA_DIR / config.get('paths', 'cache_dir', default='cache')
EXPORT_DIR = DATA_DIR / config.get('paths', 'export_dir', default='export')
QUERIES_DIR = DATA_DIR / config.get('paths', 'queries_dir', default='queries')
GA4_DIR = DATA_DIR / config.get('paths', 'ga4_dir', default='ga4')

# DB 파일 경로
GLOBAL_VOCAB_PATH = str(ACTIVE_DIR / "global_vocab.db")
UNKNOWN_DB_PATH = str(ACTIVE_DIR / "unknown_patterns.db")
MASTER_DB_PATH = str(MASTER_DIR / "neis_info.db")

# 모든 디렉토리 생성 (필요시)
for d in [ACTIVE_DIR, MASTER_DIR, LOG_DIR, METRICS_DIR, 
          TEMP_DIR, CACHE_DIR, EXPORT_DIR, QUERIES_DIR, GA4_DIR]:
    d.mkdir(parents=True, exist_ok=True)
    