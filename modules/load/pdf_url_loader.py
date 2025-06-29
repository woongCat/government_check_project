from loguru import logger
from modules.base.base_loader import BaseLoader


class PDFUrlLoader(BaseLoader):
    """
    PDF URL 데이터를 저장하는 클래스
    
    Args:
        loader (BaseLoader): BaseLoader 인스턴스
    
    """
    def __init__(self, connection):
        super().__init__(connection)

    def create_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS pdf_url (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                date DATE NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                get_pdf BOOLEAN DEFAULT FALSE,
                pdf_url TEXT,
                CONSTRAINT unique_pdf UNIQUE (date, title)
            );
        """
        self._execute_query(query)
        logger.info("✅ pdf_url 테이블 생성 완료 (또는 이미 존재함)")

    def insert(self, pdf_url_data):
        try:
            logger.info(
                f"➕ PDF URL 삽입 시도: {pdf_url_data.get('date')}"
            )
            query = """
                INSERT INTO pdf_url (date, title, description, get_pdf, pdf_url)
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (date, title) DO NOTHING;
            """
            params = (
                pdf_url_data.get("date"),
                pdf_url_data.get("title"),
                pdf_url_data.get("description"),
                pdf_url_data.get("get_pdf"),
                pdf_url_data.get("pdf_url"),
            )
            result = self._execute_query(query, params)
            if result is not None:
                logger.debug(f"Query result: {result}")
            logger.success(f"✅ PDF URL 저장 성공: {pdf_url_data.get('date')}")
        except Exception as e:
            logger.error(f"❌ PDF URL 저장 중 오류 발생: {str(e)}")
            raise

    def get_pdf_urls(self, condition=None):
        query = "SELECT id, date, title, description, get_pdf, pdf_url FROM pdf_url"
        params = None
        if condition:
            query += " WHERE " + condition[0]
            params = condition[1]
        return self._execute_query(query, params)