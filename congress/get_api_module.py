from dotenv import load_dotenv
import requests
import os
import json

# 나중에 추가되어야 함.

class GET_API:
    def __init__(self):
        load_dotenv()
        self.key = os.getenv("OPEN_GOVERMETN_API_KEY")
    
    def get_response(self,url,params):
        '''
        api 요청을 보내는 코드 
        '''
        params['KEY'] = self.key
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            return response
        else:
            print(response.text)
        
    def response_to_json(self,file_path,response):
        '''
        api 응답을 json으로 저장하는 코드
        '''
        try:
            with open(file_path, 'w') as f:
                json.dumps(response.json(), f)
        except Exception as e:
            print(f"json으로 저장이 안 되었습니다 {e}")
    