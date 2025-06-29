class BasePipeline:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        # 1. 추출
        raw_data = self.extractor.run()
        
        # 2. 변환
        transformed_data = self.transformer.run(raw_data)

        # 3. 적재
        self.loader.run(transformed_data)