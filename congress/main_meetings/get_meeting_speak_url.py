import requests
from dotenv import load_dotenv
import os
import json 
import csv

# 나중에 airflow에 추가되야 하는 코드

load_dotenv()
key = os.getenv("OPEN_GOVERMETN_API_KEY")

def get_conf_dates():
    """국회 일정 리스트 가져오기"""
    conf_date_list = []
    with open("congress/main_meetings/json/congress_schedule.json" ,'r') as file:
        schedule_file = json.load(file)
        data_count = schedule_file['nekcaiymatialqlxr'][0]['head'][0]['list_total_count'] # 국회 일정 개수
        for i in range(data_count):
            conf_date_list.append(schedule_file['nekcaiymatialqlxr'][1]['row'][i]['MEETTING_DATE'])
        return conf_date_list
    
def make_conf_dates_to_csv(conf_date_list):
    """국회 일정 리스트를 csv로 저장하는 함수"""
    with open('congress_meeting/conf_dates.csv','w',newline='') as f:
        writer = csv.writer(f)
        for date in conf_date_list:
            writer.writerow([date])
        
def get_responses(conf_date_list):
    """국회 회의록 가져오기"""
    url = "https://open.assembly.go.kr/portal/openapi/nzbyfwhwaoanttzje"
    for conf_date in conf_date_list:
        print(f"데이터를 저장중입니다.{conf_date}")
        params = {
            "KEY" : key,
            "Type" : 'json',
            "pIndex" : '1',
            "pSize" : "100",
            "DAE_NUM" : "22",
            "CONF_DATE" : conf_date
        }

        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            with open (f"congress/main_meetings/meetings{conf_date}.json",'w') as f:
                json.dump(response.json(), f)
        else:
            print(response.text)
            response.raise_for_status()
        
if __name__ == "__main__":
    conf_date_list = get_conf_dates()
    make_conf_dates_to_csv(conf_date_list)
    get_responses(conf_date_list)