import datetime
import re
from io import BytesIO

import pdfplumber
import requests
from loguru import logger


def fetch_pdf_content_in_url(pdf_url):
    text = ""

    try:
        response = requests.get(pdf_url, timeout=10)  # Timeout 설정 추가
        response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
        logger.info(f"pdf url 응답 성공: {response.status_code}")

        with pdfplumber.open(BytesIO(response.content)) as pdf:
            text = "\n".join(
                [page.extract_text() for page in pdf.pages if page.extract_text()]
            )  # None 방지

        logger.info(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
        return text

    except requests.exceptions.RequestException as e:
        logger.info(f"pdf url 응답 성공: {text[:100]}")
        print(f"PDF 다운로드 실패: {e}")

    return ""  # 오류 발생 시 빈 문자열 반환


def get_speaker_data(id, title, date, committee_name, text):
    """텍스트에서 발언자와 발언 내용을 추출하는 함수"""
    speaker_pattern = re.compile(
        r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE
    )
    speech_list = []

    for match in speaker_pattern.finditer(text):
        speaker = match.group(1).strip()
        speech = match.group(2).strip()

        speech_list.append(
            {
                "document_id": id,
                "title": title,
                "committee_name": committee_name,
                "date": date,
                "speaker": speaker,  # 발언자
                "text": speech,  # 발언
                "summary": None,  # 요약은 나중에 추가
                "timestamp": datetime.now(),  # 편집날짜
            }
        )

    return speech_list


def get_speak_data():
    pass
