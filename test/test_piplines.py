
from modules.pipeline.pdf_to_speech_pipeline import PDFToSpeechPipeline
from modules.pipeline.schedule_to_pdf_pipeline import ScheduleToPDFPipeline


def test_pdf_to_speech_pipeline():
    pipeline = PDFToSpeechPipeline()
    extracted_data = pipeline.run()
    assert isinstance(extracted_data, list)
    assert all("text" in item for item in extracted_data)
    print(f"✅ PDFToSpeechPipeline test passed with {len(extracted_data)} items.")


def test_schedule_to_pdf_pipeline():
    pipeline = ScheduleToPDFPipeline()
    result = pipeline.run()
    assert result is True or result is None  # depending on run implementation
    print("✅ ScheduleToPdfPipeline test passed.")

