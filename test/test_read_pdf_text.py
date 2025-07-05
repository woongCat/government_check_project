from modules.utils.db_connections import get_postgres_connection
from modules.extract.pdf_to_speech_extractor import PDFToSpeechExtractor
from modules.transform.pdf_to_speech_transformer import PDFToSpeechTransformer
from modules.load.pdf_to_speech_loader import PDFToSpeechLoader

def test_pdf_extraction():
    # DB 연결
    connection = get_postgres_connection()

    # PDFToSpeechLoader 인스턴스 생성
    loader = PDFToSpeechLoader(connection)
    loader.create_table()
    
    # 병렬 추출 인스턴스 생성
    extractor = PDFToSpeechExtractor(connection=connection)
    print("✅ 병렬로 PDF 추출 시작")
    extracted_data = extractor.extract()
    print(f"✅ 처리 완료: 총 {len(extracted_data)}건")

    transformer = PDFToSpeechTransformer()

    for item in extracted_data:
        print(f"\n{item['title']} ({item['date']})")
        
        # PDF 텍스트를 발언 단위로 변환
        transformed_result = transformer.transform(
            text=item['text'],
            title=item['title'],
            date=item['date'],
            class_name=item['class_name'],
            file_path=item['file_path'],
            confer_number=item['confer_number'],
            dae_number=item['dae_number'],
            pdf_url_id=item['pdf_url_id']
        )
        
        print(f"추출된 발언 수: {len(transformed_result)}")
        for speech in transformed_result[:3]:  # 처음 3개 발언만 출력
            print(f"- {speech['speaker']}: {speech['text'][:100]}...")

        if not transformed_result:
            print("⚠️ 변환된 발언이 없어 건너뜁니다.")
            continue

        # 변환된 발언 데이터를 DB에 저장
        try:
            loader.load(
                speech_data=transformed_result
            )
            print(f"✅ DB 저장 완료: {len(transformed_result)}건")
        except Exception as e:
            print(f"❌ DB 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    test_pdf_extraction()