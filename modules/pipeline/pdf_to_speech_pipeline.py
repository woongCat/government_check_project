from modules.base.base_pipeline import BasePipeline
from modules.extract.speech_pdf_extractor import SpeechPDFExtractor
from modules.load.speech_pdf_loader import SpeechPDFLoader
from modules.transform.speech_pdf_transformer import SpeechPDFTransformer

class PDFToSpeechPipeline(BasePipeline):
    def __init__(self, extractor, loader, transformer=None):
        super().__init__(extractor, loader, transformer)

    def run(self):
        # 1. 추출
        raw_data = self.extractor.run()
        
        # 2. 변환
        transformed_data = self.transformer.run(raw_data)

        # 3. 적재
        self.loader.run(transformed_data)