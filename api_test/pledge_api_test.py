import requests
from dotenv import load_dotenv
import os
import json

# API Key 로드
def load_api_key():
    load_dotenv()  # .env 파일에서 환경 변수 로드
    serviceKey = os.getenv("GONGGONG_API_KEY")  # API 키 저장
    if not serviceKey:
        raise ValueError("Error: API key not found in environment variables.")
    return serviceKey

# API 호출
def get_api_data():
    serviceKey = load_api_key()
    base_url = "http://apis.data.go.kr/9760000/ElecPrmsInfoInqireService/getCnddtElecPrmsInfoInqire"
    
    params = {
        'serviceKey': serviceKey,
        'pageNo': '1',        # 페이지 번호
        'numOfRows': '10',    # 한 번에 가져올 데이터 개수
        'resultType': 'json', # 응답 형식 (JSON)
        'sgId': '20080409',   # 선거 ID
        'sgTypecode': '2',    # 선거 구분 코드
        'cnddtId': '100003487', # 후보자 ID
    }
    
    response = requests.get(base_url, params=params)
    print(response)
    print(response.text)
    if response.status_code == 200:
        try:
            response_json = response.json()
            print(json.dumps(response_json, indent=4, ensure_ascii=False))  # 데이터를 보기 쉽게 출력
            return response_json
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print(f"API request failed with status code {response.status_code}")
        print(f"Response: {response.text}")

# 실행
if __name__ == "__main__":
    data = get_api_data()