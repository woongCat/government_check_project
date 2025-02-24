import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging

# 로그 설정
logging.basicConfig(
    filename="database_manager.log", 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

def log(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    print(message)

class PostgreSQLManager:
    def __init__(self):
        load_dotenv()
        try:
            self.connection = psycopg2.connect(
                dbname=os.getenv("SQL_DBNAME"),
                user=os.getenv("SQL_USER"),
                password=os.getenv("SQL_PWD"),
                host=os.getenv("SQL_HOST"),
                port=os.getenv("SQL_PORT")
            )
            self.cursor = self.connection.cursor()
            log("✅ PostgreSQL 연결 성공")
        except Exception as e:
            log(f"❌ PostgreSQL 연결 실패: {e}", "error")

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            log(f"✅ PostgreSQL 쿼리 실행 성공: {query}")
            return self.cursor.fetchall()
        except Exception as e:
            log(f"❌ PostgreSQL 쿼리 실행 실패: {e}", "error")
            return None

    def close(self):
        self.cursor.close()
        self.connection.close()
        log("✅ PostgreSQL 연결 종료")

class MongoDBManager:
    def __init__(self, database_name, uri="mongodb://localhost:27017/"):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[database_name]
            log("✅ MongoDB 연결 성공")
        except Exception as e:
            log(f"❌ MongoDB 연결 실패: {e}", "error")

    def insert_document(self, collection_name, document):
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            log(f"✅ MongoDB 문서 삽입 성공: {result.inserted_id}")
            return result
        except Exception as e:
            log(f"❌ MongoDB 문서 삽입 실패: {e}", "error")
            return None
        
    def find_documents(self, collection_name, query, select):
        try:
            collection = self.db[collection_name]
            results = list(collection.find(query, select))
            log(f"✅ MongoDB 문서 검색 성공: {query}")
            return results
        except Exception as e:
            log(f"❌ MongoDB 문서 검색 실패: {e}", "error")
            return None
        
    def find_distinct_documents(self, collection_name, select):
        try:
            collection = self.db[collection_name]
            results = list(collection.distinct(select))
            log(f"✅ MongoDB 문서 검색 성공: {select}")
            return results
        except Exception as e:
            log(f"❌ MongoDB 문서 검색 실패: {e}", "error")
            return None
        
    def drop_collection(self, collection_name):
        try:
            collection = self.db[collection_name]
            collection.drop()
            log(f"✅ MongoDB collection 삭제 성공")
        except Exception as e:
            log(f"❌ MongoDB collection 삭제 실패 {e}", "error")
            return None
        
    def close(self):
        self.client.close()
        log("✅ MongoDB 연결 종료")

# ✅ PostgreSQLManager, MongoDBManager 상속
class DatabaseManager(PostgreSQLManager, MongoDBManager):
    def __init__(self, database_name, uri="mongodb://localhost:27017/"):
        PostgreSQLManager.__init__(self)  # PostgreSQL 초기화
        MongoDBManager.__init__(self, database_name, uri)  # MongoDB 초기화

    def schedule_to_postgresql(self, schedule_list, table_name):
        try:
            for meeting in schedule_list:
                date = meeting["MEETING_DATE"]
                self.execute_query(f"""
                    INSERT INTO {table_name} (meeting_date)
                    VALUES (%s)
                    ON CONFLICT (meeting_date) DO NOTHING
                """, (date,))
            self.connection.commit()
            log("✅ PostgreSQL 일정 저장 성공")
        except Exception as e:
            self.connection.rollback()
            log(f"❌ PostgreSQL 일정 저장 실패: {e}", "error")

    def schedule_from_postgresql(self, table_name):
        try:
            result = self.execute_query(f"SELECT meeting_date FROM {table_name} WHERE get_pdf = FALSE")
            meeting_dates = [row[0] for row in result] if result else []
            log(f"✅ PostgreSQL 일정 조회 성공: {meeting_dates}")
            return meeting_dates
        except Exception as e:                       
            log(f"❌ PostgreSQL 일정 조회 실패: {e}", "error")
            return None

    def close_connections(self):
        self.close()  # PostgreSQL & MongoDB 둘 다 close 실행
        log("✅ 모든 데이터베이스 연결 종료")