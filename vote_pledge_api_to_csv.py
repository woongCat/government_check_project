import requests
from dotenv import load_dotenv
import json
import os
import pandas as pd
import time

def get_votecode():
    try:
        csv_file = pd.read_csv('vote_erection.csv')
        csv_file = csv_file[csv_file['sgId'] == 20200415]
        pledge_code_list = [(row['huboid'], row['sgId']) for row in csv_file[['huboid', 'sgId']].to_dict('records')]
        print(len(pledge_code_list))
        return pledge_code_list
    except FileNotFoundError:
        print("Error: 'vote_erection.csv' not found. Please make sure the file exists.")
        return []
    
def load_api_key():
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    if not serviceKey:
        raise ValueError("Error: API key not found in environment variables. Please check your .env file.")
    return serviceKey


def read_api_to_df(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        print(f"Response status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        print(f"Failed URL: {url}")
        print(f"Parameters: {params}")
        return pd.DataFrame()

    try:
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            print(f"Unexpected Content-Type: {content_type}")
            return pd.DataFrame()

        response_json = response.json()
        items = response_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
        if not items:
            print(f"No items found in response for parameters: {params}")
            return pd.DataFrame()
        print(items)
        return pd.json_normalize(items)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return pd.DataFrame()
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error during JSON processing: {e}")
        return pd.DataFrame()


def api_to_csv(url):
    serviceKey = load_api_key()
    
    df = pd.DataFrame()
    pledge_code_list = get_votecode()
    if not pledge_code_list:
        print("Error: No vote codes found.")
        return
    
    file_path = "vote_pledge.csv"
    
    # 시간 측정 시작
    start_time = time.time()
    
    for huboid,sgId in pledge_code_list:
        for i in range(1, 20):
            params = {
                'serviceKey': f'{serviceKey}',
                'pageNo': f'{i}',
                'numOfRows': '100',
                'resultType': 'json',
                'sgId': f'{sgId}',
                'sgTypecode': '2',
                'cnddtId': f'{huboid}',
            }
            new_df = read_api_to_df(url, params=params)
            df = pd.concat([df, new_df], ignore_index=True)
    
    try:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"Error saving data to {file_path}: {e}")
        
    # 시간 측정 종료
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    pledge_url = "http://apis.data.go.kr/9760000/ElecPrmsInfoInqireService/getCnddtElecPrmsInfoInqire"    
    api_to_csv(pledge_url)