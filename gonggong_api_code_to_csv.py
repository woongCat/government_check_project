import requests
from dotenv import load_dotenv
import json
import os
import pandas as pd
        
def read_api_to_df(url,params):
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print(f"API READ CORRECTLY {response.status_code}")
    else:
        print(f"Error Code :{response.status_code}")
    
    response_json = json.loads(response.content)
    json_to_df = pd.json_normalize(response_json)
    return json_to_df
    
if __name__ == "__main__":
    
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    
    # 공공데이터 국회의원 선거공약정보확인을 위한 코드 정보
    govcode_url = "http://apis.data.go.kr/9760000/CommonCodeService/getCommonSgCodeList"
    govcode_params = {'serviceKey' : serviceKey, 'resultType' : 'json', 'pageNo' : '1', 'numOfRows' : '100'}
    
    print(read_api_to_df(govcode_url,govcode_params))