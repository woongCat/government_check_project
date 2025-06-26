import requests
from loguru import logger

def request_paginated_data(
    url, base_params, key_name, date_key=None, date_value=None, page_size=100
):
    """
    페이징 처리된 데이터를 반복적으로 요청하여 모두 수집하는 함수입니다.

    Args:
        url (str): 요청할 API URL
        base_params (dict): 기본 요청 파라미터 (API 키 등)
        key_name (str): 응답 JSON에서 실제 데이터가 담긴 key 이름
        date_key (str, optional): 특정 날짜 필터를 위한 파라미터 이름 (예: "CONF_DATE")
        date_value (str, optional): 날짜 필터로 사용할 값
        page_size (int, optional): 페이지당 데이터 개수. 기본값은 100

    Returns:
        list: 수집된 전체 row 데이터 리스트
    """
    all_data = []  # NOTE: 결과 누적 리스트
    pIndex = 1     # NOTE: 페이지 인덱스 초기화

    while True:
        # NOTE: 요청 파라미터 구성
        params = base_params.copy()
        params["pIndex"] = str(pIndex)
        params["pSize"] = str(page_size)
        if date_key and date_value:
            params[date_key] = date_value  # NOTE: 날짜 필터 적용

        logger.info(f"📡 요청 중... pIndex={pIndex}, URL: {url}")
        response = requests.get(url=url, params=params)

        # NOTE: HTTP 요청 실패 처리
        if response.status_code != 200:
            logger.error(f"❌ 요청 실패! 상태 코드: {response.status_code}, 응답: {response.text}")
            break

        data = response.json()

        # NOTE: 데이터가 존재하지 않음을 명시한 경우
        if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
            logger.warning("📢 데이터 없음, 반복 중지")
            break

        # NOTE: key_name이 없을 경우 비정상 응답 처리
        if key_name not in data:
            logger.error(f"❌ 예상된 키({key_name})가 응답 데이터에 없습니다.")
            break

        try:
            # NOTE: 일반적으로 두 번째 요소에 'row' 데이터가 존재함
            rows = data[key_name][1]["row"]
            all_data.extend(rows)
            logger.info(f"✅ {pIndex} 페이지 데이터 추가 (총 {len(rows)}개)")
        except (KeyError, IndexError) as e:
            logger.error(f"❌ 데이터 구조 오류: {e}")
            break

        # NOTE: 다음 페이지 요청
        pIndex += 1  

    return all_data