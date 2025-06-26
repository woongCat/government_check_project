# modules/extract/congress_schedule_extractor.py
from modules.base.base_extractor import BaseExtractor
from modules.utils.request_utils import request_paginated_data
import os

OPEN_GOV_API_KEY = os.getenv("OPEN_GOVERMETN_API_KEY")

class CongressScheduleExtractor(BaseExtractor):
    """_summary_
    전체 일정 데이터를 가져오는 함수
    _설정된 일정 API URL에서 데이터를 가져오고, 페이지 단위로 요청하여 전체 데이터를 수집합니다._

    Args:
        url (_type_): _설정된 일정 API URL_
        unit_cd (str, optional): 국회의원 치수. Defaults to "100022".
        page_size (int, optional): 페이지의 크기. Defaults to 100.

    Returns:
        _type_: _전체 일정 데이터 리스트_
    """
    def __init__(self, url, unit_cd="100022", page_size=100):
        self.url = url
        self.unit_cd = unit_cd
        self.page_size = page_size

    def extract(self):
        self.log_info(f"📢 CongressScheduleExtractor: Extracting from {self.url}")
        key_name = self.url[43:]
        base_params = {
            "KEY": OPEN_GOV_API_KEY,
            "Type": "json",
            "UNIT_CD": self.unit_cd,
        }
        all_data = request_paginated_data(
            self.url, base_params, key_name, page_size=self.page_size
        )
        self.log_info(f"📌 전체 일정 데이터 로드 완료! 총 {len(all_data)}개 수집")
        return all_data