import datetime
import re
from typing import Dict, List

from modules.base.base_transformer import BaseTransformer


class PDFToSpeechTransformer(BaseTransformer):
    """
    국회 회의록 PDF에서 발언 텍스트를 파싱하여 구조화된 리스트로 반환하는 클래스
    """

    def __init__(self, document_id: str, title: str, date: str, committee_name: str):
        self.document_id = document_id
        self.title = title
        self.date = date
        self.committee_name = committee_name

        # 발언자 패턴 정의: ◯홍길동\n 발언내용
        self.speaker_pattern = re.compile(
            r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE
        )

    def transform(self, text: str) -> List[Dict]:
        """
        전체 회의록 텍스트에서 발언 정보를 파싱

        Args:
            text (str): 전체 회의록 텍스트

        Returns:
            List[dict]: 발언 데이터가 담긴 딕셔너리 리스트
        """
        speech_list = []

        for match in self.speaker_pattern.finditer(text):
            speaker = match.group(1).strip()
            speech = match.group(2).strip()

            speech_list.append(
                {
                    "document_id": self.document_id,
                    "title": self.title,
                    "committee_name": self.committee_name,
                    "date": self.date,
                    "speaker": speaker,
                    "text": speech,
                    "summary": None,  # 요약은 후처리로 추가 예정
                    "timestamp": datetime.datetime.now(),
                }
            )

        return speech_list
