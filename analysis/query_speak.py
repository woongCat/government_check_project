from elasticsearch import Elasticsearch
import os

# .env에서 환경변수 불러오기
from dotenv import load_dotenv
load_dotenv()

# 환경변수에서 비밀번호 가져오기
ELASTIC_PASSWORD = os.getenv("ELASTIC_SEARCH_PW")

# Elasticsearch 클라이언트 설정
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", ELASTIC_PASSWORD),  # 인증 추가
    verify_certs=False  # 자체 서명된 인증서 문제 방지
)

# 인덱스명 설정 (저장된 데이터가 들어 있는 인덱스)
index_name = "comitee_meetings"

# 모든 데이터 조회
def get_speaker_documents(speaker_name="교육위원장 김영호"):
    query = {
        "query": {
            "match": {
                "speaker": speaker_name  # ✅ 발언자 필터 적용
            }
        }
    }

    res = es.search(index=index_name, body=query, size=100)  # 최대 10개 조회
    return res['hits']['hits']

# 데이터 가져오기
documents = get_speaker_documents()
for doc in documents:
    print(f"문서 ID: {doc['_id']}, 내용: {doc['_source']}")