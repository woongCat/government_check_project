from pymongo import MongoClient

# MongoDB 서버에 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["government"]  
collection = db["congress_schedule"]

# 데이터베이스 목록 출력
db_list = client.list_database_names()
print("✅ 현재 MongoDB 데이터베이스 목록:", db_list)

# 데이터 삭제
# collection.delete_many({})

# 데이터 조회
result = collection.find({},{}).to_list()
print("✅ MongoDB 데이터 확인:", result)

# 연결 종료
client.close()