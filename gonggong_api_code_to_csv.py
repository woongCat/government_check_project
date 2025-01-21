import requests
from dotenv import load_dotenv
import json
import os
import pandas as pd
        
def read_api_to_df(url,params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # HTTP 에러가 있으면 예외 발생
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return pd.DataFrame()

    try: 
        response_json = response.json()
        items = response_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
        print(items)

        if items:
            json_to_df = pd.json_normalize(items)
        else:
            print("No items found in response")
            return pd.DataFrame()
            
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error during JSON processing: {e}")
        return pd.DataFrame()
    
    return json_to_df

def api_to_csv(url):
    
    df = pd.DataFrame()
    file_path = 'public_vote_code'
    
    for i in range(1,10):
        params = {'serviceKey' : serviceKey, 'resultType' : 'json', 'pageNo' : f'{i}', 'numOfRows' : '100'}
        new_df = read_api_to_df(url,params=params)
        df = pd.concat([df, new_df])
        
    df.to_csv("public_vote_code.csv")
        
if __name__ == "__main__":    
    # api key
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    
    # 공공데이터 국회의원 선거공약정보확인을 위한 코드 정보
    govcode_url = "http://apis.data.go.kr/9760000/CommonCodeService/getCommonSgCodeList"
    
    # api code csv로 저장
    api_to_csv(govcode_url)