import requests
from dotenv import load_dotenv
import json
import os

# 나중에 airflow에 추가되야 하는 코드

load_dotenv()
key = os.getenv("OPEN_GOVERMETN_API_KEY")
url = "https://open.assembly.go.kr/portal/openapi/nekcaiymatialqlxr"

params = {
    "KEY" : key,
    "Type" : 'json',
    "pIndex" : '1',
    "pSize" : "100",
    "UNIT_CD" : "100022",
}

# 요청 받기
response = requests.get(url=url, params=params)

# json으로 저장 받기
file_path = 'congress_meeting/congress_schedule.json'
with open(file_path, 'w') as f:
    json.dump(response.json(), f)
    
print(response.json())