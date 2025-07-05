import os
from typing import Dict, List, Optional, Tuple

from loguru import logger

from modules.base.base_loader import BaseLoader


# TODO : pdfurl에서 pdf를 추출하고 상태를 True로 바꾸는 게 중요함
class PDFToSpeechLoader(BaseLoader):
    def __init__(self, connection):
        super().__init__(connection)

    def _create_tables(self) -> None:
        """Create all necessary database tables."""
        try:
            queries = [
                """
                CREATE TABLE IF NOT EXISTS speakers (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_speaker UNIQUE (name)
                );
                """,
                """CREATE INDEX IF NOT EXISTS idx_speakers_name ON speakers(name);""",
                """
                CREATE TABLE IF NOT EXISTS speeches (
                    pdf_url_id TEXT NOT NULL,
                    speaker_id UUID NOT NULL REFERENCES speakers(id) ON DELETE CASCADE,
                    speech_number INT NOT NULL,
                    date DATE NOT NULL,
                    class_name TEXT NOT NULL,
                    confer_number INT NOT NULL,
                    dae_number INT NOT NULL,
                    speech TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_speech UNIQUE (pdf_url_id, speech_number)
                );
                """,
                """CREATE INDEX IF NOT EXISTS idx_speeches_pdf_url_id ON speeches(pdf_url_id);""",
            ]
            for query in queries:
                self._execute_query(query)
            logger.info("✅ 데이터베이스 테이블 생성 완료")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 테이블 생성 실패: {e}")
            raise

    def create_table(self) -> None:
        """Create all necessary database tables."""
        self._create_tables()

    def _save_all_data(self, speech_data: List[Dict]) -> None:
        speaker_names = {s["speaker"] for s in speech_data}
        speaker_map = {}
        speaker_query = """
            INSERT INTO speakers (name) 
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """
        for name in speaker_names:
            result = self._execute_query(speaker_query, (name,))
            if result:
                speaker_map[name] = result[0][0]

        speech_query = """
            INSERT INTO speeches (
                pdf_url_id,
                speech_number,
                speaker_id,
                date,
                class_name,
                confer_number,
                dae_number,
                speech,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for idx, speech in enumerate(speech_data):
            speaker_id = speaker_map.get(speech["speaker"])
            if not speaker_id:
                logger.warning(f"⚠️ 발언자 정보 없음 - {speech['speaker']}")
                continue
            self._execute_query(
                speech_query,
                (
                    speech["pdf_url_id"],
                    idx + 1,
                    speaker_id,
                    speech["date"],
                    speech["class_name"],
                    speech["confer_number"],
                    speech["dae_number"],
                    speech["text"],
                    speech["timestamp"],
                ),
            )

    def load(
        self,
        speech_data: List[Dict],
    ) -> None:
        """Save the parsed speech data to PostgreSQL."""
        if not speech_data:
            logger.warning("⚠️ 저장할 발언 데이터가 없습니다.")
            return

        first_speech = speech_data[0]

        # 필수 필드 검증
        required_fields = ("speaker", "text", "timestamp")

        if not all(k in first_speech for k in required_fields):
            logger.error(f"❌ 필수 항목이 누락되었습니다: {required_fields}")
            return

        try:
            # 트랜잭션 시작
            self._execute_query("BEGIN")
            self._save_all_data(speech_data)
            # 트랜잭션 커밋
            self._execute_query("COMMIT")
            logger.info(f"✅ PostgreSQL 저장 완료 - {first_speech['pdf_url_id']}")
        except Exception as e:
            self._execute_query("ROLLBACK")
            logger.error(f"❌ PostgreSQL 저장 실패: {e}")
            raise
