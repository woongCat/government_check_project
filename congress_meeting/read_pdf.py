import requests
import pdfplumber
import io

# PDF 다운로드 URL
url = "http://likms.assembly.go.kr/record/mhs-10-040-0040.do?conferNum=054706&fileId=0000124454&deviceGubun=P&imsiYn=P"

# HTTP 요청하여 PDF 다운로드
response = requests.get(url)
response.raise_for_status()  # 오류 발생 시 예외 처리

# PDF 내용을 메모리에서 직접 읽기
with pdfplumber.open(io.BytesIO(response.content)) as pdf: # 반면, io.BytesIO를 사용하면 메모리에서 직접 처리할 수 있어 더 효율적입니다.
    for page in pdf.pages:
        print(page.extract_text())  # 페이지별 텍스트 출력
        
# 이후에는 이 내용을 어떻게 저장할지 데이터베이스로 만들어야 함