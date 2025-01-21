import requests
from dotenv import load_dotenv
import json
import os

def api_test(url,params):
    file_path = 'gonggong_code.json'
    response = requests.get(url, params=params)

    if response.status_code == 200:
        print(json.loads(response.content))
    else:
        print(f"Error Code :{response.status_code}")
        
    with open(file_path, "w") as f:
        json.dump(json.loads(response.content), f, ensure_ascii=False, indent=4)
        
    return
    
if __name__ == "__main__":
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    
    # 공공데이터 국회의원 선거공약정보
    pledge_url = "http://apis.data.go.kr/9760000/ElecPrmsInfoInqireService/getCnddtElecPrmsInfoInqire"
    pledge_params ={'serviceKey' : serviceKey, 'resultType' : 'json', 'pageNo' : '1', 'numOfRows' : '10', 'sgId' : '20231011', 'sgTypecode' : '4', 'cnddtId' : '1000000000' }
    
    # 공공데이터 국회의원 선거공약정보확인을 위한 코드 정보
    govcode_url = "http://apis.data.go.kr/9760000/CommonCodeService/getCommonSgCodeList"
    govcode_params = {'serviceKey' : serviceKey, 'resultType' : 'json', 'pageNo' : '1', 'numOfRows' : '100'}
    
    # 추가로 할 일 
    # 데이터를 반복문으로 읽고 데이터를 json으로 갱신하는 코드 작성
    # 이 때 airflow를 활용해서 S3에 추가 적재할지를 고민
    
    # 공공데이터
    api_test(govcode_url,params=govcode_params)