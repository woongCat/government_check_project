import os
import requests
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 읽기
client_id = os.getenv("NAVER_API_ID")
client_secret = os.getenv("NAVER_API_PW")

# 검색어와 URL 설정
enc_text = "국회의원"
url = f"https://openapi.naver.com/v1/search/news?query={enc_text}"

# 헤더 추가
headers = {
    "X-Naver-Client-Id": client_id,
    "X-Naver-Client-Secret": client_secret
}

# API 요청
response = requests.get(url, headers=headers)

# 응답 확인 및 출력
if response.status_code == 200:
    print(response.json())  # JSON 형태로 출력
else:
    print(f"Error Code: {response.status_code}")