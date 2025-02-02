import requests
from dotenv import load_dotenv
import json
import os

# 나중에 airflow에 추가되야 하는 코드
def get_schedule(url):
    load_dotenv()
    key = os.getenv("OPEN_GOVERMETN_API_KEY")
    
    pIndex = 1  # 페이지 번호 초기화
    all_data = []  # 전체 데이터를 저장할 리스트
    
    while True:
        params = {
            "KEY" : key,
            "Type" : 'json',
            "pIndex" : str(pIndex),
            "pSize" : "100",
            "UNIT_CD" : "100022",
        }
        # 요청 받기
        response = requests.get(url=url, params=params)
        data = response.json()
        
        # 종료 조건: 데이터가 없을 경우 중단
        if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
            print("📢 데이터 없음, 반복 중지")
            break
        
        # 데이터 추가
        all_data.append(data)

        print(f"📌 {pIndex} 페이지 데이터 저장 완료")
        
        # 다음 페이지로 이동
        pIndex += 1
        
    # JSON으로 저장
    file_path = "comittee/json/comittee_schedule.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    
    print(f"✅ 모든 데이터를 {file_path}에 저장 완료!")

if __name__ == "__main__":
    url = "https://open.assembly.go.kr/portal/openapi/nrsldhjpaemrmolla"
    get_schedule(url)

