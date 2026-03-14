# constants/errors.py
"""API 오류 코드 정의 (NEIS, VWorld, 학교알리미 등)"""

# NEIS API 오류 코드 (출처: https://open.neis.go.kr/portal/data/service/selectServicePage.do)
NEIS_ERRORS = {
    "ERROR-300": "필수 값이 누락되었습니다.",
    "ERROR-290": "인증키가 유효하지 않습니다.",
    "ERROR-310": "해당하는 서비스를 찾을 수 없습니다.",
    "ERROR-333": "요청위치 값의 타입이 유효하지 않습니다.",
    "ERROR-336": "데이터 요청은 한 번에 최대 1,000건을 넘을 수 없습니다.",
    "ERROR-337": "일별 트래픽 제한을 넘었습니다.",
    "ERROR-500": "서버 오류입니다.",
    "ERROR-600": "데이터베이스 연결 오류입니다.",
    "ERROR-601": "SQL 문장 오류입니다.",
    "INFO-000": "정상 처리되었습니다.",
    "INFO-100": "해당 자료는 단순 참고용입니다.",
    "INFO-200": "해당하는 데이터가 없습니다.",
    "INFO-300": "관리자에 의해 인증키 사용이 제한되었습니다.",
}

# VWorld API 오류 상태 코드
VWORLD_STATUS = {
    "OK": "성공",
    "NOT_FOUND": "주소를 찾을 수 없음",
    "LIMIT_EXCEEDED": "일일 API 사용량 초과",
    "ERROR": "서버 오류",
}

# 학교알리미 API (필요시 추가)
SCHOOL_INFO_RESULT_CODES = {
    "success": "성공",
    "fail": "실패",
}

# 공통 HTTP 상태 코드 설명 (선택 사항)
HTTP_STATUS = {
    200: "성공",
    400: "잘못된 요청",
    401: "인증 실패",
    403: "권한 없음",
    404: "찾을 수 없음",
    429: "너무 많은 요청",
    500: "서버 내부 오류",
    502: "Bad Gateway",
    503: "서비스 불가",
    504: "Gateway Timeout",
}
