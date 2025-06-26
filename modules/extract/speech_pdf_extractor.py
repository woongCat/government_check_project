import datetime
import re
from io import BytesIO
from typing import List

import pdfplumber
import requests

from modules.base.base_extractor import BaseExtractor


class SpeechPDFExtractor(BaseExtractor):
    def __init__(self, pdf_url: str, document_id: str, title: str, date: str, committee_name: str):
        self.pdf_url = pdf_url
        self.document_id = document_id
        self.title = title
        self.date = date
        self.committee_name = committee_name

    def extract(self) -> List[dict]:
        text = self._fetch_pdf_content()
        if not text:
            return []
        return self._parse_speeches(text)

    def _fetch_pdf_content(self) -> str:
        """PDF URL에서 텍스트 추출"""
        try:
            response = requests.get(self.pdf_url, timeout=10)
            response.raise_for_status()
            self.log_info(f"pdf url 응답 성공: {response.status_code}")

            with pdfplumber.open(BytesIO(response.content)) as pdf:
                text = "\n".join(
                    [page.extract_text() for page in pdf.pages if page.extract_text()]
                )
            self.log_info(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
            return text
        except requests.exceptions.RequestException as e:
            self.log_info(f"PDF 다운로드 실패: {e}")
            return ""

