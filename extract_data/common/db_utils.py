from pymongo import MongoClient

def save_to_mongo(data, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    """
    데이터를 MongoDB에 저장하는 함수.

    :param data: 저장할 데이터 (리스트 또는 딕셔너리)
    :param db_name: 사용할 데이터베이스 이름
    :param collection_name: 사용할 컬렉션 이름
    :param mongo_uri: MongoDB 연결 URI (기본값: 로컬 MongoDB)
    """
    try:
        # MongoDB 연결
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # 데이터 저장
        if isinstance(data, list):  # 리스트 형태면 여러 개 저장
            collection.insert_many(data)
        else:  # 단일 딕셔너리면 하나만 저장
            collection.insert_one(data)

        print(f"✅ 데이터 저장 완료! (DB: {db_name}, Collection: {collection_name})")
    except Exception as e:
        print(f"❌ 데이터 저장 실패: {e}")
    finally:
        client.close()