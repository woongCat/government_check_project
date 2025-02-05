import requests
from dotenv import load_dotenv
import os
import json 
import csv

# 나중에 airflow에 추가되야 하는 코드 -- main_meetings에서 가져옴 

load_dotenv()
key = os.getenv("OPEN_GOVERMETN_API_KEY")

def get_meeting_dates():
    """의원회 일정 리스트 가져오기"""
    meeting_date_set = set()
    with open("comittee/json/comittee_schedule.json" ,'r') as file:
        schedule_file = json.load(file)
        for schedule in schedule_file: # 리스트로 연결해둔 거 하나씩 꺼냄  
            print(type(schedule))
            data_count = len(schedule['nrsldhjpaemrmolla'][1]['row'])
            print(data_count)
            for i in range(data_count): # 일정 가져오기
                date = schedule['nrsldhjpaemrmolla'][1]['row'][i]['MEETING_DATE']
                formed_date = '-'.join(date[:10].split('.')) # 
                meeting_date_set.add(formed_date)
        meeting_date_list = list(meeting_date_set)
        return meeting_date_list
    
def make_meeting_dates_to_csv(meeting_date_list):
    """위원회 일정 리스트를 csv로 저장하는 함수"""
    with open('comittee/csv/meeting_dates.csv','w',newline='') as f:
        writer = csv.writer(f)
        for date in meeting_date_list:
            writer.writerow([date])
        
def get_responses(meeting_date_list):
    """위원회 회의록 가져오기"""
    url = "https://open.assembly.go.kr/portal/openapi/ncwgseseafwbuheph"
    for meeting_date in meeting_date_list:
        pIndex = 1  # 페이지 번호 초기화
        all_data = []  # 전체 데이터를 저장할 리스트
        while True:
            print(f"데이터를 저장중입니다.{meeting_date}")
            params = {
                "KEY" : key,
                "Type" : 'json',
                "pIndex" : str(pIndex),
                "pSize" : "100",
                "DAE_NUM" : "22",
                "CONF_DATE" : meeting_date
            }
            # 요청 받기 
            response = requests.get(url=url, params=params)
            data = response.json()
            print(response.text)
            
            # 종료 조건: 데이터가 없을 경우 중단
            if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
                print("📢 데이터 없음, 반복 중지")
                break
            
            # 데이터 추가
            all_data.append(data)
            
            print(f"📌 {pIndex} 페이지 데이터 저장 완료")
        
            # 다음 페이지로 이동
            pIndex += 1
        

        if response.status_code == 200:
            with open (f"comittee/comittee_meetings/{meeting_date}.json",'w') as f:
                json.dump(response.json(), f)
        else:
            print(response.text)
            response.raise_for_status()
            
if __name__ == "__main__":
    meeting_date_list = get_meeting_dates()
    make_meeting_dates_to_csv(meeting_date_list)
    get_responses(meeting_date_list)
