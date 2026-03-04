# core/error_classifier.py
def classify_error(status_code: int, consecutive_404: int = 0) -> dict:
    if status_code in (401, 403):
        return {
            'type': 'FATAL_AUTH',
            'action': 'stop',
            'message': '인증 실패! API 키를 확인하세요.'
        }
    elif status_code == 404:
        if consecutive_404 >= 2:
            return {
                'type': 'ORPHAN',
                'action': 'orphan',
                'message': '3회 연속 404 → 존재하지 않는 학교로 간주'
            }
        else:
            return {
                'type': 'ORPHAN_CANDIDATE',
                'action': 'skip',
                'message': f'404 발생 (연속 {consecutive_404+1}회)'
            }
    elif status_code == 400:
        return {
            'type': 'FATAL_REQUEST',
            'action': 'skip',
            'message': '잘못된 요청 (코드 버그 가능성)'
        }
    elif status_code == 429 or status_code >= 500:
        return {
            'type': 'TRANSIENT',
            'action': 'retry',
            'message': '일시적 오류'
        }
    else:
        return {
            'type': 'UNKNOWN',
            'action': 'retry',
            'message': f'알 수 없는 오류 {status_code}'
        }
        