# constants/errors.py
"""API 오류 코드 정의"""

NEIS_ERRORS = {
    "ERROR-300": "필수 값 누락",
    "ERROR-290": "인증키 유효하지 않음",
    "ERROR-310": "서비스를 찾을 수 없음",
    "ERROR-333": "요청위치 값 타입 오류",
    "ERROR-336": "최대 요청 건수 초과(1,000건)",
    "ERROR-337": "일별 트래픽 제한 초과",
    "ERROR-500": "서버 오류",
    "ERROR-600": "DB 연결 오류",
    "ERROR-601": "SQL 문장 오류",
    "INFO-000": "정상",
    "INFO-100": "단순 참고용",
    "INFO-200": "데이터 없음",
    "INFO-300": "인증키 사용 제한",
}

SCHOOL_INFO_ERRORS = {
    "fail": "실패",
    # 필요시 추가
}

VWORLD_ERRORS = {
    "LIMIT_EXCEEDED": "일일 API 사용량 초과",
    "NOT_FOUND": "주소를 찾을 수 없음",
    # ...
}
