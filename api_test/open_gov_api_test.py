import requests

url = "https://openwatch.kr/api/national-assembly/members"

response = requests.get(url,headers={})

print(response.json())