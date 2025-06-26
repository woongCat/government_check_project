import datetime
import re
from typing import List

def parse_speeches(
    text: str, document_id: str, title: str, date: str, committee_name: str
) -> List[dict]:
    """
    텍스트에서 국회의원 발언 데이터를 파싱하여 구조화된 리스트로 반환합니다.

    Args:
        text (str): 전체 회의록 텍스트
        document_id (str): 문서 고유 ID
        title (str): 회의 제목
        date (str): 회의 날짜
        committee_name (str): 위원회 이름

    Returns:
        List[dict]: 발언 데이터가 담긴 딕셔너리 리스트
    """
    # 발언자 패턴 정의: ◯홍길동\n 발언내용
    speaker_pattern = re.compile(
        r"◯([\w]+ [\w]+)\s*\n*([\s\S]+?)(?=\n◯|\Z)", re.MULTILINE
    )
    speech_list = []

    # 패턴에 매칭되는 모든 발언을 찾아 리스트에 저장
    for match in speaker_pattern.finditer(text):
        speaker = match.group(1).strip()
        speech = match.group(2).strip()

        speech_list.append(
            {
                "document_id": document_id,
                "title": title,
                "committee_name": committee_name,
                "date": date,
                "speaker": speaker,
                "text": speech,
                "summary": None,  # 요약은 후처리로 추가 예정
                "timestamp": datetime.datetime.now(),  # 현재 시각으로 기록
            }
        )

    return speech_list
