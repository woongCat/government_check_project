import requests
from dotenv import load_dotenv
import os

def api_test(url,params):
    response = requests.get(url, params=params)

    if response.status_code == 200:
        print(response.content)
    else:
        print(f"Error Code :{response.status_code}")

if __name__ == "__main__":
    
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    
    # 공공데이터 국회의원 선거공약정보
    url = "http://apis.data.go.kr/9760000/ElecPrmsInfoInqireService/getCnddtElecPrmsInfoInqire"
    params ={'serviceKey' : f'{serviceKey}', 'pageNo' : '1', 'numOfRows' : '10', 'sgId' : '20231011', 'sgTypecode' : '4', 'cnddtId' : '1000000000' }
    
    api_test(url,params=params)
