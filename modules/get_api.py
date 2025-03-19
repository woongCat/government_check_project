import requests
import os
from dotenv import load_dotenv
import logging
import pdfplumber
import re
from io import BytesIO
from datetime import datetime

# 로그 설정
logging.basicConfig(
    filename="api_manager.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)


def log(message, level="info"):
    """
    로그 메시지를 출력하고 파일에 저장하는 함수
    """
    if level == "info":
        logging.info(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    print(message)


class GET_API:
    def __init__(self):
        load_dotenv()
        self.key = os.getenv("OPEN_GOVERMETN_API_KEY")

    def get_schedule(self, url, unit_cd="100022", page_size=100):
        """
        회의 일정을 가져오는 함수
        """
        pIndex = 1
        all_data = []

        log(f"📢 일정 데이터를 {url}에서 가져옵니다.")

        while True:
            params = {
                "KEY": self.key,
                "Type": "json",
                "pIndex": str(pIndex),
                "pSize": str(page_size),
                "UNIT_CD": unit_cd,
            }

            log(f"📡 {pIndex} 페이지 요청 중... URL: {url}")
            response = requests.get(url=url, params=params)

            if response.status_code != 200:
                log(
                    f"❌ 요청 실패! 상태 코드: {response.status_code}, 응답: {response.text}",
                    "error",
                )
                break

            data = response.json()

            if (
                "RESULT" in data
                and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다."
            ):
                log("📢 데이터 없음, 반복 중지", "warning")
                break

            key_name = url[43:]  # URL에서 API 고유 키 추출
            if key_name not in data:
                log(f"❌ 예상된 키({key_name})가 응답 데이터에 없습니다.", "error")
                break

            try:
                page_data = data[key_name][1]["row"]
                all_data.append(page_data)
                log(f"✅ {pIndex} 페이지 데이터 추가 (총 {len(page_data)}개)")
            except (KeyError, IndexError) as e:
                log(f"❌ 데이터 구조 오류: {e}", "error")
                break

            pIndex += 1

        log(f"📌 전체 일정 데이터 로드 완료! 총 {len(all_data)} 페이지 수집")
        return all_data

    def get_pdf_url(self, url, meeting_date_list, unit_cd="22", page_size=100):
        """
        회의록 PDF URL을 가져오는 함수
        """
        all_data = []
        get_pdf_dates = set()
        log(f"📢 PDF URL 데이터를 {url}에서 가져옵니다.")

        for meeting_date in meeting_date_list:
            pIndex = 1

            while True:
                log(f"📡 {meeting_date} 데이터 요청 중... 페이지: {pIndex}")

                params = {
                    "KEY": self.key,
                    "Type": "json",
                    "pIndex": str(pIndex),
                    "pSize": str(page_size),
                    "DAE_NUM": unit_cd,
                    "CONF_DATE": meeting_date,
                }

                response = requests.get(url=url, params=params)

                if response.status_code != 200:
                    log(
                        f"❌ {meeting_date} 데이터 요청 실패! 상태 코드: {response.status_code}, 응답: {response.text}",
                        "error",
                    )
                    break

                data = response.json()

                if (
                    "RESULT" in data
                    and data["RESULT"]["MESSAGE"] == "해당하는 데이터가 없습니다."
                ):
                    log(f"📢 {meeting_date}에 대한 데이터 없음, 반복 중지", "warning")
                    break

                get_pdf_dates.add(meeting_date)
                log(f"✅ {meeting_date} 데이터 추가 (페이지 {pIndex})")

                key_name = url[43:]  # URL에서 API 고유 키 추출
                if key_name not in data:
                    log(f"❌ 예상된 키({key_name})가 응답 데이터에 없습니다.", "error")
                    break

                try:
                    rows = data[key_name][1]["row"]
                    for i in range(len(rows)):
                        page_data = rows[i]
                        all_data.append(page_data)
                        log(f"✅ {pIndex} 페이지 데이터 추가 (총 {len(page_data)}개)")
                except (KeyError, IndexError) as e:
                    log(f"❌ 데이터 구조 오류: {e}", "error")
                    break

                pIndex += 1

        get_pdf_dates = list(get_pdf_dates)
        get_pdf_dates.sort()
        # with open('data_json.json', 'w') as f:
        #     json.dump(all_data, f)
        log(f"📌 전체 PDF URL 데이터 로드 완료! 총 {len(all_data)}개 수집")
        return all_data, get_pdf_dates

    def get_pdf_content_in_url(self, pdf_url):
        text = ""

        try:
            response = requests.get(pdf_url, timeout=10)  # Timeout 설정 추가
            response.raise_for_status()  # HTTP 오류 발생 시 예외 처리
            log(f"pdf url 응답 성공: {response.status_code}")

            with pdfplumber.open(BytesIO(response.content)) as pdf:
                text = "\n".join(
                    [page.extract_text() for page in pdf.pages if page.extract_text()]
                )  # None 방지

            log(f"pdf 텍스트 추출 성공(미리 보기): {text[:100]}")
            return text

        except requests.exceptions.RequestException as e:
            log(f"pdf url 응답 성공: {text[:100]}")
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