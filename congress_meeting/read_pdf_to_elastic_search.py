import requests
import pdfplumber
import io
import re
from dotenv import load_dotenv
import os
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime
import logging

# 나중에 airflow에 추가되야 하는 코드

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def read_date_title_url():
    """csv를 읽어서 id, date, title, pdf_url를 가져오는 함수"""
    file_path = "congress_meeting/pdf_url.csv"
    date_title_url_df = pd.read_csv(file_path)
    return date_title_url_df.values.tolist()  # DataFrame을 한 번에 리스트로 변환

def get_pdf_to_text(url):
    """ PDF URL에서 텍스트를 추출하는 함수 """
    text = ""
    
    try:
        response = requests.get(url, timeout=10)  # Timeout 설정 추가
        response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
        logger.info(f"pdf url 응답 성공: {response.status_code}")

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])  # None 방지

        logger.info(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
        return text

    except requests.exceptions.RequestException as e:
        logger.error(f"pdf url 응답 성공: {text[:100]}")
        print(f"PDF 다운로드 실패: {e}")
    
    return ""  # 오류 발생 시 빈 문자열 반환
            
def get_speaker_data(id,title,date,text):
    """ 텍스트에서 발언자와 발언 내용을 추출하는 함수 """
    speaker_pattern = re.compile(r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE)
    speech_list = []
    
    for match in speaker_pattern.finditer(text):
        speaker = match.group(1).strip()
        speech = match.group(2).strip()

        speech_list.append({
            "document_id": id,
            "title": title,
            "date": date,
            "speaker": speaker, # 발언자
            "text": speech, # 발언
            "summary": None,  # 요약은 나중에 추가
            'timestamp': datetime.now(), # 편집날짜
        })

    return speech_list

def connect_to_elasticsearch():
    """Elasticsearch에 연결하는 함수"""
    load_dotenv()

    Elasticsearch_key = os.getenv("ELASTIC_SEARCH_PW")
    # Elasticsearch 연결
    es = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", Elasticsearch_key),  # 인증 추가
        verify_certs=False  # 자체 서명된 인증서 문제 방지
        )

    # 인덱스 설정 (인덱스가 없으면 생성)
    index_name = "congress_meetings"

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        
    return es,index_name

def save_to_elasticsearch(data):
    """ 추출된 국회의원 발언 데이터를 Elasticsearch에 저장하는 함수 """
    es, index_name = connect_to_elasticsearch()
    for entry in data:
        es.index(index=index_name, body=entry)
    print("Elasticsearch 저장 완료!")

if __name__ == "__main__":
    # PDF 다운로드 URL
    url_csv = read_date_title_url()
    for id, title, date, pdf_url in url_csv:
        pdf_text = get_pdf_to_text(pdf_url)
        speech_data = get_speaker_data(id, title, date, pdf_text)
        save_to_elasticsearch(speech_data)
