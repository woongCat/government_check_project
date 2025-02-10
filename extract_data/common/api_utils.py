import requests
import os
from dotenv import load_dotenv

def load_api_key():
    """api에 필요한 Key를 로드하는 함수"""
    load_dotenv()
    return os.getenv("OPEN_GOVERMETN_API_KEY")


def fetch_data(url, key, unit_cd="100022", page_size=100):
    """API 데이터 가져오는 함수"""
    pIndex = 1
    all_data = []

    while True:
        params = {
            "KEY": key,
            "Type": "json",
            "pIndex": str(pIndex),
            "pSize": str(page_size),
            "UNIT_CD": unit_cd,
        }

        response = requests.get(url=url, params=params)
        data = response.json()

        if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
            print("📢 데이터 없음, 반복 중지")
            break

        all_data.append(data)
        print(f"📌 {pIndex} 페이지 데이터 저장 완료")

        pIndex += 1

    return all_data