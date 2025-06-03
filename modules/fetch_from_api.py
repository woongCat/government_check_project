import os

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
OPEN_GOVERMETN_API_KEY = os.getenv("OPEN_GOVERMETN_API_KEY")


def fetch_schedule(url, unit_cd="100022", page_size=100):
    """_summary_
    전체 일정 데이터를 가져오는 함수
    _설정된 일정 API URL에서 데이터를 가져오고, 페이지 단위로 요청하여 전체 데이터를 수집합니다._

    Args:
        url (_type_): _설정된 일정 API URL_
        unit_cd (str, optional): 국회의원 치수. Defaults to "100022".
        page_size (int, optional): 페이지의 크기. Defaults to 100.

    Returns:
        _type_: _전체 일정 데이터 리스트_
    """
    logger.info(f"📢 일정 데이터를 {url}에서 가져옵니다.")
    key_name = url[43:]
    base_params = {
        "KEY": OPEN_GOVERMETN_API_KEY,
        "Type": "json",
        "UNIT_CD": unit_cd,
    }
    all_data = request_paginated_data(url, base_params, key_name, page_size=page_size)
    logger.info(f"📌 전체 일정 데이터 로드 완료! 총 {len(all_data)}개 수집")
    return all_data

# -------------------------------------------- #

def fetch_pdf_url(url, meeting_date_list, unit_cd="22", page_size=100):
    """_summary_

    Args:
        url (_type_): _설정된 PDF URL API URL_
        meeting_date_list (_type_): 이거 때문에 fetch_schedule을 통해서 schedule을 가져와야 함
        unit_cd (str, optional): 국회의원 선거 번호 fetch_schedule이랑은 또 다름. Defaults to "22".
        page_size (int, optional): 페이지의 크기. Defaults to 100.

    Returns:
        _type_: _description_
    """
    logger.info(f"📢 PDF URL 데이터를 {url}에서 가져옵니다.")
    key_name = url[43:]
    base_params = {
        "KEY": OPEN_GOVERMETN_API_KEY,
        "Type": "json",
        "DAE_NUM": unit_cd,
    }
    all_data = []
    get_pdf_dates = set()

    for meeting_date in meeting_date_list:
        logger.info(f"📡 {meeting_date} 데이터 요청 중...")
        page_data = request_paginated_data(
            url,
            base_params,
            key_name,
            date_key="CONF_DATE",
            date_value=meeting_date,
            page_size=page_size,
        )
        if page_data:
            get_pdf_dates.add(meeting_date)
            all_data.extend(page_data)

    get_pdf_dates = sorted(list(get_pdf_dates))
    logger.info(f"📌 전체 PDF URL 데이터 로드 완료! 총 {len(all_data)}개 수집")
    return all_data, get_pdf_dates

# =========================================== #

def request_paginated_data(
    url, base_params, key_name, date_key=None, date_value=None, page_size=100
):
    """_summary_

    Args:
        url (_type_): _description_
        base_params (_type_): _description_
        key_name (_type_): _description_
        date_key (_type_, optional): _description_. Defaults to None.
        date_value (_type_, optional): _description_. Defaults to None.
        page_size (int, optional): _description_. Defaults to 100.

    Returns:
        _type_: _description_
    """
    all_data = []
    pIndex = 1

    while True:
        params = base_params.copy()
        params["pIndex"] = str(pIndex)
        params["pSize"] = str(page_size)
        if date_key and date_value:
            params[date_key] = date_value

        logger.info(f"📡 요청 중... pIndex={pIndex}, URL: {url}")
        response = requests.get(url=url, params=params)

        if response.status_code != 200:
            logger.error(f"❌ 요청 실패! 상태 코드: {response.status_code}, 응답: {response.text}")
            break

        data = response.json()

        if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
            logger.warning("📢 데이터 없음, 반복 중지")
            break

        if key_name not in data:
            logger.error(f"❌ 예상된 키({key_name})가 응답 데이터에 없습니다.")
            break

        try:
            rows = data[key_name][1]["row"]
            all_data.extend(rows)
            logger.info(f"✅ {pIndex} 페이지 데이터 추가 (총 {len(rows)}개)")
        except (KeyError, IndexError) as e:
            logger.error(f"❌ 데이터 구조 오류: {e}")
            break

        pIndex += 1

    return all_data