import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트(core 폴더의 부모 폴더)에 있는 .env 파일을 찾습니다.
env_path = Path(__file__).resolve().parent.parent / '.env'

if env_path.exists():
    load_dotenv(env_path)
    # 로드 확인용 (개발 중에만 켜두셔도 됩니다)
    # print(f"✅ Loaded environment variables from {env_path}")
else:
    pass # .env가 없어도 에러를 내지 않고 넘어갑니다.
    