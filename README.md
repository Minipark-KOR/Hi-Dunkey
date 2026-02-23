# NEIS 데이터 수집기 프로젝트

## 개요
NEIS API를 활용하여 급식, 학사일정, 시간표, 학교정보를 수집하고 관리하는 시스템입니다.

## 디렉토리 구조
- `scripts/`: 실행 스크립트
  - `core/`: 공통 모듈
  - `parsers/`: 도메인별 파서
  - `constants/`: 상수
  - `collectors/`: 수집기
  - `baskets/`: 캐시 관리
  - `run_*.py`: 주기별 실행 스크립트
- `data/`: 데이터 저장소
  - `active/`: 현재 학년도 데이터 (샤딩)
  - `backup/`: 지난 3개 학년도 데이터
  - `archive/`: 10년 블록 통합본
  - `baskets/`: Hot/Warm 캐시
- `logs/`: 로그 파일

## 주요 기능
- 샤딩(홀수/짝수 학교)으로 동시성 확보
- 학년도(3월~2월) 기준 데이터 관리
- 2/20 전체 수집, 2/22 백업, 4/15 아카이브 정리
- 4단계 캐시 (L1~L4)

## 환경변수
- `NEIS_API_KEY`: NEIS API 키

## 실행 방법
- 매일: `python scripts/run_daily.py`
- 2/20: `python scripts/run_feb20.py`
- 2/22: `python scripts/run_feb22.py`
- 4/15: `python scripts/run_april15.py`
- Hot 갱신: `python scripts/baskets/update_hot.py`
- Warm 갱신: `python scripts/baskets/build_warm.py`