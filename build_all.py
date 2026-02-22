import os

# 1. 생성할 전체 디렉토리 구조 (public 및 데이터 폴더 포함)
directories = [
    "scripts/core", "scripts/parsers", "scripts/constants", 
    "scripts/collectors", "scripts/baskets",
    "data/active", "data/backup", "data/archive", "data/baskets/warm", 
    "logs", "public"
]

# 2. 파일별 내용 정의
project_files = {
    # 인덱스 HTML 파일
    "public/index.html": """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hi-Dunkey 프로젝트</title>
    <style>
        body { font-family: 'Malgun Gothic', sans-serif; line-height: 1.6; padding: 40px; background-color: #f4f7f6; }
        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        .container { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    </style>
</head>
<body>
    <div class="container">
        <h1>Hi-Dunkey 데이터 수집기</h1>
        <p>프로젝트 폴더 구조 및 기본 설정이 완료되었습니다.</p>
        <ul>
            <li>수집 상태: <strong>운영 중</strong></li>
            <li>저장소: <strong>GitHub 연동 완료</strong></li>
        </ul>
    </div>
</body>
</html>""",

    "scripts/constants/codes.py": """
import os
# 깃허브 시크릿에서 API 키 로드
NEIS_API_KEY = os.getenv("NEIS_API_KEY", "")
NEIS_ENDPOINTS = {
    'school': 'https://open.neis.go.kr/hub/schoolInfo',
    'meal': 'https://open.neis.go.kr/hub/mealServiceDietInfo',
    'schedule': 'https://open.neis.go.kr/hub/SchoolSchedule',
    'timetable': 'https://open.neis.go.kr/hub/hisTimetable'
}
ALL_REGIONS = ["B10", "C10", "D10", "E10", "F10", "G10", "H10", "I10", "J10", "K10", "M10", "N10", "P10", "Q10", "R10", "S10", "T10"]
""",
}

def build():
    print("🏗️ 프로젝트 올인원 빌드 시작...")
    
    # 폴더 및 .gitkeep 생성 (빈 폴더 유지용)
    for d in directories:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, ".gitkeep"), "w") as f:
            pass
        print(f"📂 생성 완료: {d}/.gitkeep")
    
    # 파일 작성
    for path, content in project_files.items():
        with open(path, "w", encoding="utf-8") as f:
            f.write(content.strip())
        print(f"📄 작성 완료: {path}")

    print("\n✨ 모든 폴더와 파일(index.html 포함)이 준비되었습니다!")

if __name__ == "__main__":
    build()