import requests
import pdfplumber
import io
import re
from dotenv import load_dotenv
import os
from elasticsearch import Elasticsearch
from datetime import datetime

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

def get_pdf_to_text(url):
    """ PDF URL에서 텍스트를 추출하는 함수 """
    try:
        response = requests.get(url, timeout=10)  # Timeout 설정 추가
        response.raise_for_status()  # HTTP 오류 발생 시 예외 처리

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])  # None 방지

        print(f'text 추출 완료 (미리보기): {text[:500]}...')  # 너무 긴 출력 방지
        return text

    except requests.exceptions.RequestException as e:
        print(f"PDF 다운로드 실패: {e}")
    except pdfplumber.PDFSyntaxError as e:
        print(f"PDF 파싱 실패: {e}")
    
    return ""  # 오류 발생 시 빈 문자열 반환
            
def get_speaker(text):
    """ 텍스트에서 발언자와 발언 내용을 추출하는 함수 """
    speaker_pattern = re.compile(r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE)
    speech_list = []
    
    for match in speaker_pattern.finditer(text):
        speaker = match.group(1).strip()
        speech = match.group(2).strip()

        speech_list.append({
            "document_id": "421회-2차-2025-01-23",
            "title": "제421회 국회 본회의 회의록",
            "date": "2025-01-23",
            "speaker": speaker,
            "text": speech,
            "summary": None,  # 요약은 나중에 추가
            'timestamp': datetime.now(),
        })

    print(f'분류 완료 (샘플 2개): {speech_list[:2]}')
    return speech_list

# 이후에는 이 내용을 어떻게 저장할지 데이터베이스로 만들어야 함
# elastic search를 이용해서 저장하면 될 거 같음

def save_to_elasticsearch(data):
    """ 추출된 국회의원 발언 데이터를 Elasticsearch에 저장하는 함수 """
    for entry in data:
        es.index(index=index_name, body=entry)
    print("Elasticsearch 저장 완료!")

if __name__ == "__main__":
    # PDF 다운로드 URL
    url = "http://likms.assembly.go.kr/record/mhs-10-040-0040.do?conferNum=054706&fileId=0000124454&deviceGubun=P&imsiYn=P"

    text = get_pdf_to_text(url)
    speech_data = get_speaker(text)
    save_to_elasticsearch(speech_data)