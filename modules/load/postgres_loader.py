from loguru import logger
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# ✅ PostgreSQLLoader: Handles loading of schedules and PDF metadata into PostgreSQL
class PostgreSQLLoader(object):
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host=os.getenv("PG_HOST"),
                port=os.getenv("PG_PORT"),
                dbname=os.getenv("PG_DATABASE"),
                user=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD"),
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ PostgreSQL connection established")
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            raise e

    def _execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            try:
                result = self.cursor.fetchall()
                return result
            except psycopg2.ProgrammingError:
                # No results to fetch
                return None
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            raise e

    def schedule_to_postgresql(self, schedules, table_name):
        """
        postgres에 저장된 스케쥴 데이터를 postgresql에 없는 거만 추가하는 함수
        """
        try:
            for date in schedules:
                print(date)
                self._execute_query(
                    f"""
                    INSERT INTO {table_name} (meeting_date)
                    VALUES (%s)
                    ON CONFLICT (meeting_date) DO NOTHING
                """,
                    (date,),
                )
            self.connection.commit()
            logger.info(f"✅ PostgreSQL {table_name}일정 저장 성공")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ PostgreSQL 일정 저장 실패: {e}")

    def schedule_from_postgresql(self, table_name):
        """
        postgres에 저장된 스케쥴 데이터 중 pdf를 가져오지 않은 날짜만 데이터를 가져옴
        """
        try:
            result = self._execute_query(
                f"SELECT meeting_date FROM {table_name} WHERE get_pdf = FALSE"
            )
            meeting_dates = [row[0] for row in result] if result else []
            logger.info(f"✅ PostgreSQL 일정 조회 성공: {meeting_dates}")
            return meeting_dates
        except Exception as e:
            logger.error(f"❌ PostgreSQL 일정 조회 실패: {e}")
            return None

    def change_get_status(self, pdf_dates, table_name):
        """
        그 이후 그 날짜에 get_pdf를 True로 값을 바꿈
        """
        try:
            query = f"UPDATE {table_name} SET get_pdf = TRUE WHERE get_pdf = FALSE AND meeting_date = %s"
            for date in pdf_dates:
                self._execute_query(query, (date,))  # ✅ 파라미터화된 쿼리 사용
                logger.info(f"✅ Postgresql pdf 가져온 상태 변환 성공 {date}")
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Postgresql pdf 가져온 상태 변환 실패: {e}")

    def pdf_url_from_postgresql(self, table_name):
        try:
            pdf_urls = self._execute_query(
                f"SELECT pdf_url FROM {table_name} WHERE get_pdf = False"
            )
            logger.info("✅ PostgreSQL 발언 링크 읽어오기 성공!")
            return pdf_urls
        except Exception as e:
            logger.error(f"❌ PostgreSQL 발언 링크 저장 실패: {e}")
            return None

    def close_connections(self):
        try:
            self.cursor.close()
            self.connection.close()
            logger.info("✅ 모든 데이터베이스 연결 종료")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 연결 종료 실패: {e}")
