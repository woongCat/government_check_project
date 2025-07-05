from modules.constants.url_constants import (
    MAIN_CONGRESS_SCHEDULE_URL,
    MAIN_CONGRESS_SPEECH_PDF_URL,
)
from modules.extract.congress_schedule_extractor import CongressScheduleExtractor
from modules.extract.pdf_url_extractor import PDFUrlExtractor
from modules.load.pdf_url_loader import PDFUrlLoader
from modules.utils.db_connections import get_postgres_connection

if __name__ == "__main__":
    # Step 1: 일정 추출
    schedule_extractor = CongressScheduleExtractor(url=MAIN_CONGRESS_SCHEDULE_URL)
    print("✅ 일정 extractor 시작")
    schedule_data = schedule_extractor.extract()
    print("✅ 일정 데이터 추출 완료")

    # Step 2: 날짜 리스트 생성
    meeting_dates = sorted(set(item["MEETTING_DATE"] for item in schedule_data))

    # Step 3: PDF URL 추출
    pdf_extractor = PDFUrlExtractor(
        url=MAIN_CONGRESS_SPEECH_PDF_URL, meeting_dates=meeting_dates, unit_cd="22"
    )

    print("✅ PDF extractor 시작")
    pdf_data, fetched_dates = pdf_extractor.extract()
    print(f"✅ PDF 데이터 추출 완료 ({len(pdf_data)}건)")

    # Step 4: PostgreSQL 저장
    connection = get_postgres_connection()
    pdf_url_loader = PDFUrlLoader(connection)
    pdf_url_loader.create_table()

    for item in pdf_data:            
        pdf_url_data = {
            "date": item.get("CONF_DATE"),
            "title": item.get("TITLE"),
            "description": item.get("SUB_NAME"),
            "get_pdf": False,
            "pdf_url": item.get("PDF_LINK_URL"),
        }
        pdf_url_loader.load(pdf_url_data)

    connection.close()
    print("✅ PostgreSQL 저장 완료")

