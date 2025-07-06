
from modules.pipeline.pdf_to_speech_pipeline import PDFToSpeechPipeline
from modules.pipeline.schedule_to_pdf_pipeline import ScheduleToPDFPipeline


def test_pdf_to_speech_pipeline():
    pipeline = PDFToSpeechPipeline()
    try:
        pipeline.run()
        print("✅ PDFToSpeechPipeline test passed (no exception raised).")
    except Exception as e:
        assert False, f"❌ PDFToSpeechPipeline test failed: {e}"


def test_schedule_to_pdf_pipeline():
    pipeline = ScheduleToPDFPipeline()
    saved_count = pipeline.run()
    assert isinstance(saved_count, int)
    assert saved_count >= 0  # 최소 0건 이상 저장됐는지만 확인
    print(f"✅ ScheduleToPdfPipeline test passed with {saved_count} items saved.")

