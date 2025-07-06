from loguru import logger

from modules.base.base_pipeline import BasePipeline
from modules.constants.url_constants import (
    MAIN_CONGRESS_SCHEDULE_URL,
    MAIN_CONGRESS_SPEECH_PDF_URL,
)
from modules.extract.congress_schedule_extractor import CongressScheduleExtractor
from modules.extract.pdf_url_extractor import PDFUrlExtractor
from modules.load.pdf_url_loader import PDFUrlLoader
from modules.transform.congress_schedule_transformer import CongressScheduleTransformer
from modules.transform.pdf_url_transformer import PDFUrlTransformer
from modules.utils.db_connections import get_postgres_connection

# TODO : 없는 스케쥴만 가져오는 기능 추가 -> 지금은 구현이 먼저


class ScheduleToPDFPipeline(BasePipeline):
    def __init__(self, unit_cd="22"):
        self.schedule_extractor = CongressScheduleExtractor(
            url=MAIN_CONGRESS_SCHEDULE_URL
        )
        self.schedule_transformer = CongressScheduleTransformer()
        self.pdf_extractor = PDFUrlExtractor(
            url=MAIN_CONGRESS_SPEECH_PDF_URL,
            meeting_dates=[],
            unit_cd=unit_cd,
        )
        self.pdf_transformer = PDFUrlTransformer()
        self.loader = PDFUrlLoader(get_postgres_connection())

    def run(self):
        # Step 1: 일정 추출
        logger.info("✅ 일정 extractor 시작")
        schedule_data = self.schedule_extractor.extract()
        logger.info("✅ 일정 데이터 추출 완료")

        # Step 2: 날짜 리스트 생성
        meeting_dates = self.schedule_transformer.transform(schedule_data)
        logger.info(f"✅ 날짜 리스트 생성 완료 ({len(meeting_dates)}건)")

        # Step 3: PDF URL 추출
        self.pdf_extractor.meeting_dates = meeting_dates
        logger.info("✅ PDF extractor 시작")
        pdf_data, fetched_dates = self.pdf_extractor.extract()
        logger.info(f"✅ PDF 데이터 추출 완료 ({len(pdf_data)}건)")

        # Step 4: 변환
        transformed_pdf_data = self.pdf_transformer.transform(pdf_data)
        logger.info(f"✅ PDF 데이터 변환 완료 ({len(transformed_pdf_data)}건)")

        # Step 5: DB 저장
        self.loader.create_table()
        for item in transformed_pdf_data:
            self.loader.load(item)

        logger.info("✅ PostgreSQL 저장 완료")
        return len(transformed_pdf_data)
