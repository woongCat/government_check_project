from modules.base.base_pipeline import BasePipeline
from modules.extract.congress_schedule_extractor import CongressScheduleExtractor
from modules.load.pdf_url_loader import PDFUrlLoader

class ScheduleToPDFPipeline(BasePipeline):
    def __init__(self, extractor, loader, transformer=None):
        super().__init__(extractor, loader, transformer)

    def run(self):
        # 1. 추출
        raw_data = self.extractor.run()
        
        # 2. 변환
        transformed_data = self.transformer.run(raw_data)

        # 3. 적재
        self.loader.run(transformed_data)
