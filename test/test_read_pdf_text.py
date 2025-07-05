from modules.utils.db_connections import get_postgres_connection
from modules.extract.speech_pdf_extractor import SpeechPDFUrlProvider, SpeechPDFExtractor

def test_pdf_extraction():
    # DB 연결
    conn = get_postgres_connection()

    # 처리되지 않은 PDF URL 목록 가져오기
    provider = SpeechPDFUrlProvider(conn)
    unprocessed_list = provider.fetch_unprocessed_urls()

    print(f"총 {len(unprocessed_list)}건의 PDF 처리 시작")

    for item in unprocessed_list:
        print(f"\n{item['title']} ({item['date']})")
        extractor = SpeechPDFExtractor(
            pdf_url=item['pdf_url'],
            title=item['title'],
            date=item['date']
        )
        result = extractor.extract()
        print(f"추출된 발언 수: {len(result)}")
        for speech in result[:3]:  # 미리보기 3개
            print(speech)

if __name__ == "__main__":
    test_pdf_extraction()