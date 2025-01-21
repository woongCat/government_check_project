import requests

def api_test(url,headers):
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error Code :{response.status_code}")

if __name__ == "__main__":
    
    # 국회의원 명단
    url = "https://openwatch.kr/api/national-assembly/members"
    # 정치후원금 명단 
    # url = "https://openwatch.kr/api/political-contributions"

    headers = {}
    
    api_test(url,headers=headers)