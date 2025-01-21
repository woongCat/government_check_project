import requests
from dotenv import load_dotenv
import json
import os
import pandas as pd

def get_votecode():
    try:
        csv_file = pd.read_csv('public_vote_code.csv')
        congress_vote_id_list = csv_file[csv_file['sgTypecode'] == 2]['sgId'].to_list()
        return congress_vote_id_list
    except FileNotFoundError:
        print("Error: 'public_vote_code.csv' not found.")
        return []

def read_api_to_df(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        print(f"Response status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return pd.DataFrame()

    try:
        response_json = response.json()
        items = response_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
        if items:
            return pd.json_normalize(items)
        else:
            print("No items found in response")
            return pd.DataFrame()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return pd.DataFrame()
    except (KeyError, ValueError, TypeError) as e:
        print(f"Error during JSON processing: {e}")
        return pd.DataFrame()

def api_to_csv(url):
    serviceKey = load_api_key()
    
    df = pd.DataFrame()
    vote_code = get_votecode()
    if not vote_code:
        print("Error: No vote codes found.")
        return
    
    file_path = "vote_erection.csv"
    
    for id in vote_code:
        for i in range(1, 10):
            params = {
                'serviceKey': f'{serviceKey}',
                'pageNo': f'{i}',
                'resultType': 'json',
                'numOfRows': '100',
                'sgId': f'{id}',
                'sgTypecode': '2',
                'sdName': '',
                'sggName': ''
            }
            new_df = read_api_to_df(url, params=params)
            df = pd.concat([df, new_df], ignore_index=True)
    
    df.drop_duplicates(inplace=True)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Data saved to {file_path}")

def load_api_key():
    load_dotenv()
    serviceKey = os.getenv("GONGGONG_API_KEY")
    if not serviceKey:
        raise ValueError("API key not found in environment variables.")
    return serviceKey

if __name__ == "__main__":
    erection_url = 'http://apis.data.go.kr/9760000/WinnerInfoInqireService2/getWinnerInfoInqire'
    api_to_csv(erection_url)