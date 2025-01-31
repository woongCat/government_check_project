import requests
from dotenv import load_dotenv
import os
import json 

# 추후 dag로 변경될 필요 있음

load_dotenv()
key = os.getenv("OPEN_GOVERMETN_API_KEY")

def get_conf_dates():
    # 국회 일정 리스트 가져오기
    conf_date_list = []
    with open("congress_meeting/congress_schedule.json" ,'r') as file:
        schedule_file = json.load(file)
        data_count = schedule_file['nekcaiymatialqlxr'][0]['head'][0]['list_total_count'] # 국회 일정 개수
        for i in range(data_count):
            conf_date_list.append(schedule_file['nekcaiymatialqlxr'][1]['row'][i]['MEETTING_DATE'])
        return conf_date_list
        
def get_responses():
    # 국회 일정 가져오기
    url = "https://open.assembly.go.kr/portal/openapi/nzbyfwhwaoanttzje"
    conf_date_list = get_conf_dates()
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
            with open (f"congress_meeting/meetings/{conf_date}.json",'w') as f:
                json.dump(response.json(), f)
        else:
            print(response.text)
            response.raise_for_status()
        
if __name__ == "__main__":
    get_responses()