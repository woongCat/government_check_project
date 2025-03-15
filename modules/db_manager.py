import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging
import requests
import io
import pdfplumber
import re
import datetime

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
        """기본적인 쿼리 기능"""
        try:
            self.cursor.execute(query, params)
            log(f"✅ PostgreSQL 쿼리 실행 성공: {query}")

            # Check if the query is a SELECT statement
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()  # Return results for SELECT queries
            else:
                self.connection.commit()  # Commit changes for INSERT/UPDATE/DELETE queries
                return None  # No results to return for non-SELECT queries
        except Exception as e:
            self.connection.rollback()  # Rollback in case of error
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
        """
        기본적인 mongo db 삽입 기능
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            log(f"✅ MongoDB 문서 삽입 성공: {result.inserted_id}")
            return result
        except Exception as e:
            log(f"❌ MongoDB 문서 삽입 실패: {e}", "error")
            return None
        
    def find_documents(self, collection_name, query, select):
        """
        기본적인 mongo db 검색 기능
        """
        try:
            collection = self.db[collection_name]
            results = list(collection.find(query, select))
            log(f"✅ MongoDB 문서 검색 성공: {query}")
            return results
        except Exception as e:
            log(f"❌ MongoDB 문서 검색 실패: {e}", "error")
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
            log(f"✅ MongoDB 문서 검색 성공: {select}")
            log(results)
            return results
        except Exception as e:
            log(f"❌ MongoDB 문서 검색 실패: {e}", "error")
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
        
    def schedule_to_mongodb(self, schedules, schedule_colleciton_name): # 함수가 같은 기능이지만 2가지 조건 분기를 갖고 있어서 나중에 쪼개줄 필요도 있음
        """
        api로 받은 schedule을 mongodb에 저장하는 함수
        """
        if schedule_colleciton_name == "congress_schedule":
            congress_schedules = schedules
            congress_schedule_colleciton_name = schedule_colleciton_name
            try:
                for i in range(len(congress_schedules)):
                    log(f"MongoDB 저장 시도! {i}번째, {len(congress_schedules[i])}개 데이터 저장 시도")
                    for j in range(len(congress_schedules[i])):
                        congress_data = congress_schedules[i][j]
                        congress_data["MEETING_DATE"] = congress_data.pop("MEETTING_DATE") # 데이터가 MEETTING_DATE로 오타나있음 MEETING_DATE로 수정
                        self.insert_document(congress_schedule_colleciton_name, congress_data)
                    log("✅ MongoDB 일정 저장 성공")
            except Exception as e:
                log(f"❌ MongoDB 일정 저장 실패: {e}", "error")
    
        elif schedule_colleciton_name == "committee_schedule":
            committee_schedules = schedules
            committee_schedule_colleciton_name = schedule_colleciton_name
            try:
                for i in range(len(committee_schedules)):
                    log(f"MongoDB 저장 시도! {i}번째, {len(committee_schedules[i])}개 데이터 저장 시도")
                    for j in range(len(committee_schedules[i])):
                        committee_data = committee_schedules[i][j]
                        committee_data['MEETING_DATE'] = '-'.join(committee_data['MEETING_DATE'][:10].split('.')) # 데이터 형식을 - 형태로 바꿔줌
                        self.insert_document(committee_schedule_colleciton_name, committee_data)
                    log("✅ MongoDB 일정 저장 성공")
            except Exception as e:
                log(f"❌ MongoDB 일정 저장 실패: {e}", "error")
            

    def schedule_to_postgresql(self, schedules, table_name):
        """
        mongodb에 저장된 스케쥴 데이터를 postgresql에 없는 거만 추가하는 함수
        """
        try:
            for date in schedules:
                print(date)
                self.execute_query(f"""
                    INSERT INTO {table_name} (meeting_date)
                    VALUES (%s)
                    ON CONFLICT (meeting_date) DO NOTHING
                """, (date,))
            self.connection.commit()
            log(f"✅ PostgreSQL {table_name}일정 저장 성공")
        except Exception as e:
            self.connection.rollback()
            log(f"❌ PostgreSQL 일정 저장 실패: {e}", "error")

    def schedule_from_postgresql(self, table_name):
        """
        mongodb에 저장된 스케쥴 데이터 중 pdf를 가져오지 않은 날짜만 데이터를 가져옴
        """
        try:
            result = self.execute_query(f"SELECT meeting_date FROM {table_name} WHERE get_pdf = FALSE")
            meeting_dates = [row[0] for row in result] if result else []
            log(f"✅ PostgreSQL 일정 조회 성공: {meeting_dates}")
            return meeting_dates
        except Exception as e:                       
            log(f"❌ PostgreSQL 일정 조회 실패: {e}", "error")
            return None
        
    def speak_pdf_url_to_mongodb(self, pdfs, collection_name): 
        """
        schedule_from_postgresql에서 받은 날짜로 pdf_url을 저장함
        """
        try:
            print(len(pdfs), len(pdfs[0]))
            for i in range(len(pdfs)):
                log(f"MongoDB 저장 시도! {i}번째")
                pdf_data = pdfs[i]
                self.insert_document(collection_name, pdf_data)
                log("✅ MongoDB 발언 데이터 저장 성공")
        except Exception as e:
            log(f"❌ MongoDB 발언 데이터 저장 실패: {e}", "error")
            
    def change_get_status(self, pdf_dates, table_name):
        """
        그 이후 그 날짜에 get_pdf를 True로 값을 바꿈
        """
        try:
            query = f"UPDATE {table_name} SET get_pdf = TRUE WHERE get_pdf = FALSE AND meeting_date = %s"
            for date in pdf_dates:
                self.execute_query(query, (date,))  # ✅ 파라미터화된 쿼리 사용
                log(f"✅ Postgresql pdf 가져온 상태 변환 성공 {date}")
        except Exception as e:
            log(f"❌ Postgresql pdf 가져온 상태 변환 실패: {e}", "error")
            
        
    def mongodb_pdf_url_to_postgresql(self, collection_name, table_name):
        """
        api로 받은 speak_pdf_url을 postgresql에 저장하는 함수
        """
        try:
            query = {}
            select = {"CONF_DATE": 1, "PDF_LINK_URL": 1, "TITLE": 1, "_id": 0}  # _id는 제외하고 선택
            data = self.find_documents(collection_name, query, select)
            if data is None:  # Check if data is None
                log("❌ MongoDB에서 가져온 데이터가 없습니다.", "error")
                return  # Early exit if no data
            log("✅ MongoDB 발언 링크 가져오기 성공")
        except Exception as e:
            log(f"❌ MongoDB 발언 링크 가져오기 실패: {e}", "error")
            return  # Early exit on failure

        try:
            for pdf_data in data:
                pdf_date = pdf_data["CONF_DATE"]
                pdf_link = pdf_data["PDF_LINK_URL"]
                pdf_title = pdf_data["TITLE"]
                self.execute_query(f"""
                    INSERT INTO {table_name} (title, conf_date, pdf_url)
                    VALUES (%s, %s, %s)
                """, (pdf_title, pdf_date, pdf_link))  # Ensure the order matches
            self.connection.commit()
            log("✅ PostgreSQL 발언 링크 저장 성공!")
        except Exception as e:
            self.connection.rollback()
            log(f"❌ PostgreSQL 발언 링크 저장 실패: {e}", "error")
        
            
    def pdf_url_from_postgresql(self, table_name):
        try:
            pdf_urls = self.execute_query(f"SELECT pdf_url FROM {table_name} WHERE get_pdf = False")
            log("✅ PostgreSQL 발언 링크 읽어오기 성공!")
        except Exception as e:
            log(f"❌ PostgreSQL 발언 링크 저장 실패: {e}", "error")
            
        return pdf_urls
    
    def get_read_pdf(self,pdf_url):
        text = ""
    
        try:
            response = requests.get(pdf_url, timeout=10)  # Timeout 설정 추가
            response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
            log(f"pdf url 응답 성공: {response.status_code}")

            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])  # None 방지

            log(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
            return text

        except requests.exceptions.RequestException as e:
            log(f"pdf url 응답 성공: {text[:100]}")
            print(f"PDF 다운로드 실패: {e}")
        
        return ""  # 오류 발생 시 빈 문자열 반환
    
    def get_speaker_data(id,title,date, committee_name, text):
        """ 텍스트에서 발언자와 발언 내용을 추출하는 함수 """
        speaker_pattern = re.compile(r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE)
        speech_list = []
        
        for match in speaker_pattern.finditer(text):
            speaker = match.group(1).strip()
            speech = match.group(2).strip()

            speech_list.append({
                "document_id": id,
                "title": title,
                "committee_name" : committee_name,
                "date": date,
                "speaker": speaker, # 발언자
                "text": speech, # 발언
                "summary": None,  # 요약은 나중에 추가
                'timestamp': datetime.now(), # 편집날짜
            })

        return speech_list

        
    def _speak_data():
        pass
    
    def close_connections(self):
        self.close()  # PostgreSQL & MongoDB 둘 다 close 실행
        log("✅ 모든 데이터베이스 연결 종료")