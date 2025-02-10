import requests
import os
from dotenv import load_dotenv

def load_api_key():
    """
    api에 필요한 Key를 로드하는 함수
    """
    load_dotenv()
    return os.getenv("OPEN_GOVERMETN_API_KEY")


def fetch_data(url, key, unit_cd="100022", page_size=100):
    """
    congress scedhule에서 API 데이터 가져오는 함수
    """
    pIndex = 1
    all_data = []

    while True:
        params = {
            "KEY": key,
            "Type": "json",
            "pIndex": str(pIndex),
            "pSize": str(page_size),
            "UNIT_CD": unit_cd,
        } # params를 따로 만드는 방법 필요함

        response = requests.get(url=url, params=params)
        data = response.json() # 가져오는 값도 다르게 해야함 url뒤에서 따오기를 하면 될 듯
        
        if "RESULT" in data and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다.":
            print("📢 데이터 없음, 반복 중지")
            break
    
        data = data['nekcaiymatialqlxr'][1]['row']

        all_data.append(data)
        print(f"📌 {pIndex} 페이지 데이터 저장 완료")

        pIndex += 1

    return all_data

def get_conf_dates():
    """
    국회 일정 리스트 가져오기
    """
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

