import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from loguru import logger


class PostgreSQLManager:
    def __init__(self):
        load_dotenv()
        try:
            self.connection = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DBNAME"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PWD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ PostgreSQL 연결 성공")
        except Exception as e:
            logger.error(f"❌ PostgreSQL 연결 실패: {e}", "error")

    def execute_query(self, query, params=None):
        """기본적인 쿼리 기능"""
        try:
            self.cursor.execute(query, params)
            logger.success(f"✅ PostgreSQL 쿼리 실행 성공: {query}")

            # Check if the query is a SELECT statement
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()  # Return results for SELECT queries
            else:
                self.connection.commit()  # Commit changes for INSERT/UPDATE/DELETE queries
                return None  # No results to return for non-SELECT queries
        except Exception as e:
            self.connection.rollback()  # Rollback in case of error
            logger.error(f"❌ PostgreSQL 쿼리 실행 실패: {e}", "error")
            return None

    def close(self):
        self.cursor.close()
        self.connection.close()
        logger.info("✅ PostgreSQL 연결 종료")

# -------------------------------------------- #

class MongoDBManager:
    def __init__(self, database_name, uri="mongodb://localhost:27017/"):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[database_name]
            logger.success("✅ MongoDB 연결 성공")
        except Exception as e:
            logger.error(f"❌ MongoDB 연결 실패: {e}", "error")

    def insert_document(self, collection_name, document):
        """
        기본적인 mongo db 삽입 기능
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            logger.success(f"✅ MongoDB 문서 삽입 성공: {result.inserted_id}")
            return result
        except Exception as e:
            logger.error(f"❌ MongoDB 문서 삽입 실패: {e}", "error")
            return None

    def find_documents(self, collection_name, query, select):
        """
        기본적인 mongo db 검색 기능
        """
        try:
            collection = self.db[collection_name]
            results = list(collection.find(query, select))
            logger.success(f"✅ MongoDB 문서 검색 성공: {query}")
            return results
        except Exception as e:
            logger.error(f"❌ MongoDB 문서 검색 실패: {e}", "error")
            return None

    def find_distinct_documents(self, collection_name, select):
        """
        중복없이 mongo db 검색하는 기능

        *사용중
        : mongodb에서 스케쥴 가져오는 용도로 사용되고 있음
        """
        try:
            collection = self.db[collection_name]
            results = list(collection.distinct(select))
            logger.success(f"✅ MongoDB 문서 검색 성공: {select}")
            logger(results)
            return results
        except Exception as e:
            logger.error(f"❌ MongoDB 문서 검색 실패: {e}", "error")
            return None

    def drop_collection(self, collection_name):
        """
        기본적인 mongo db 삭제 기능

        *사용중
        : full refresh 하는 용도로 사용 중
        """
        try:
            collection = self.db[collection_name]
            collection.drop()
            logger.success("✅ MongoDB collection 삭제 성공")
        except Exception as e:
            logger.error(f"❌ MongoDB collection 삭제 실패 {e}", "error")
            return None

    def close(self):
        self.client.close()
        logger.success("✅ MongoDB 연결 종료")

