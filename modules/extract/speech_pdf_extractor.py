from io import BytesIO
from typing import List, Dict

import pdfplumber
import requests

from modules.base.base_extractor import BaseExtractor


class SpeechPDFExtractor(BaseExtractor):
    """PDF URL에서 텍스트 추출"""
    def __init__(self, pdf_url: str, title: str, date: str):
        self.pdf_url : str = pdf_url
        self.title : str = title
        self.date : str = date

    def extract(self) -> List[dict]:
        text : str = self._fetch_pdf_content()
        if not text:
            return []
        return self._parse_speeches(text)

    def _fetch_pdf_content(self) -> str:
        """PDF URL에서 텍스트 추출"""
        try:
            response : requests.Response = requests.get(self.pdf_url, timeout=10)
            response.raise_for_status()
            self.log_info(f"pdf url 응답 성공: {response.status_code}")

            with pdfplumber.open(BytesIO(response.content)) as pdf:
                text : str = "\n".join(
                    [page.extract_text() for page in pdf.pages if page.extract_text()]
                )
            self.log_info(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
            return text
        except requests.exceptions.RequestException as e:
            self.log_info(f"PDF 다운로드 실패: {e}")
            return ""

class SpeechPDFUrlProvider:
    """PDF URL Provider"""
    def __init__(self, connection):            
        self.connection : connection = connection

    def fetch_unprocessed_urls(self) -> List[Dict[str, str]]: 
        """Unprocessed PDF URL List"""
        query : str = """
            SELECT pdf_url, title, date
            FROM pdf_url
            WHERE get_pdf = false
            LIMIT 100
        """
        with self.connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return [dict(zip([col.name for col in cur.description], row)) for row in rows]