import os
from typing import List, Dict
import pdfplumber
from pathlib import Path
from modules.base.base_loader import BaseLoader
import requests
from io import BytesIO
from loguru import logger

class PDFToSpeechLoader(BaseLoader):
    def __init__(self, connection):
        super().__init__(connection)
        self._create_tables()


    def create_table(self) -> None:
        """Create necessary database tables."""
        try:
            # Create speech_contents table
            query = """
                CREATE TABLE IF NOT EXISTS speech_contents (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) NOT NULL,
                    speech_number INTEGER NOT NULL,
                    speaker TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_speech UNIQUE (file_name, speech_number)
                );
            """
            self._execute_query(query)
            
            # Create index for faster queries
            query = """
                CREATE INDEX IF NOT EXISTS idx_speech_file ON speech_contents (file_name);
            """
            self._execute_query(query)
            
            logger.info("✅ 데이터베이스 테이블 생성 완료")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 테이블 생성 실패: {e}")


    def load(self, file_path: str, text: str) -> None:
        """Save the file name and extracted text to PostgreSQL."""
        file_name = os.path.basename(file_path)
        try:
            query = "INSERT INTO speech_pdfs (file_name, content) VALUES (%s, %s)"
            self._execute_query(query, (file_name, text))
        except Exception as e:
            logger.error(f"❌ PostgreSQL 저장 실패 - {file_name}: {e}")

