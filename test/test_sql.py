import os
from datetime import datetime
from dotenv import load_dotenv
from modules.utils.db_connections import get_postgres_connection
from modules.load.pdf_url_loader import PDFUrlLoader
from loguru import logger

# 환경 변수 로드
load_dotenv()

def test_pdf_url_table():
    """
    PDF URL 테이블 생성 및 데이터 삽입/조회 테스트
    """
    try:
        # 데이터베이스 연결
        connection = get_postgres_connection()
        loader = PDFUrlLoader(connection)
        
        # 데이터 조회
        logger.info("\n=== 모든 데이터 조회 ===")
        all_data = loader.get_pdf_urls()
        for row in all_data:
            logger.info(row)
        # 조회된 row 개수 확인
        logger.info(f"\n=== 총 {len(all_data)}개의 row가 조회되었습니다 ===")
        
        # 특정 조건으로 조회
        logger.info("\n=== 2025-05-01 데이터 조회 ===")
        condition = ("date = %s", [datetime(2025, 5, 1)])
        specific_data = loader.get_pdf_urls(condition)
        for row in specific_data:
            logger.info(row)
        # 조건에 맞는 row 개수 확인
        logger.info(f"\n=== 조건에 맞는 {len(specific_data)}개의 row가 조회되었습니다 ===")
        
        logger.info("\n=== 테스트 완료 ===")
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    test_pdf_url_table()