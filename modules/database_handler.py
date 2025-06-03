
from loguru import logger
from modules.database_connectors import PostgreSQLManager, MongoDBManager

# ✅ PostgreSQLManager, MongoDBManager 상속
class DatabaseManager(PostgreSQLManager, MongoDBManager):
    def __init__(self, database_name, uri="mongodb://localhost:27017/"):
        PostgreSQLManager.__init__(self)  # PostgreSQL 초기화
        MongoDBManager.__init__(self, database_name, uri)  # MongoDB 초기화

    def schedule_to_mongodb(
        self, schedules, schedule_colleciton_name
    ):  # TODO: 함수가 같은 기능이지만 2가지 조건 분기를 갖고 있어서 나중에 쪼개줄 필요도 있음
        """
        api로 받은 schedule을 mongodb에 저장하는 함수
        """
        if schedule_colleciton_name == "congress_schedule":
            congress_schedules = schedules
            congress_schedule_colleciton_name = schedule_colleciton_name
            try:
                for i in range(len(congress_schedules)):
                    logger(
                        f"MongoDB 저장 시도! {i}번째, {len(congress_schedules[i])}개 데이터 저장 시도"
                    )
                    for j in range(len(congress_schedules[i])):
                        congress_data = congress_schedules[i][j]
                        congress_data["MEETING_DATE"] = congress_data.pop(
                            "MEETTING_DATE"
                        )  # 데이터가 MEETTING_DATE로 오타나있음 MEETING_DATE로 수정
                        self.insert_document(
                            congress_schedule_colleciton_name, congress_data
                        )
                    logger("✅ MongoDB 일정 저장 성공")
            except Exception as e:
                logger(f"❌ MongoDB 일정 저장 실패: {e}", "error")

        elif schedule_colleciton_name == "committee_schedule":
            committee_schedules = schedules
            committee_schedule_colleciton_name = schedule_colleciton_name
            try:
                for i in range(len(committee_schedules)):
                    logger(
                        f"MongoDB 저장 시도! {i}번째, {len(committee_schedules[i])}개 데이터 저장 시도"
                    )
                    for j in range(len(committee_schedules[i])):
                        committee_data = committee_schedules[i][j]
                        committee_data["MEETING_DATE"] = "-".join(
                            committee_data["MEETING_DATE"][:10].split(".")
                        )  # 데이터 형식을 - 형태로 바꿔줌
                        self.insert_document(
                            committee_schedule_colleciton_name, committee_data
                        )
                    logger("✅ MongoDB 일정 저장 성공")
            except Exception as e:
                logger(f"❌ MongoDB 일정 저장 실패: {e}", "error")

    def schedule_to_postgresql(self, schedules, table_name):
        """
        mongodb에 저장된 스케쥴 데이터를 postgresql에 없는 거만 추가하는 함수
        """
        try:
            for date in schedules:
                print(date)
                self.execute_query(
                    f"""
                    INSERT INTO {table_name} (meeting_date)
                    VALUES (%s)
                    ON CONFLICT (meeting_date) DO NOTHING
                """,
                    (date,),
                )
            self.connection.commit()
            logger(f"✅ PostgreSQL {table_name}일정 저장 성공")
        except Exception as e:
            self.connection.rollback()
            logger(f"❌ PostgreSQL 일정 저장 실패: {e}", "error")

    def schedule_from_postgresql(self, table_name):
        """
        mongodb에 저장된 스케쥴 데이터 중 pdf를 가져오지 않은 날짜만 데이터를 가져옴
        """
        try:
            result = self.execute_query(
                f"SELECT meeting_date FROM {table_name} WHERE get_pdf = FALSE"
            )
            meeting_dates = [row[0] for row in result] if result else []
            logger(f"✅ PostgreSQL 일정 조회 성공: {meeting_dates}")
            return meeting_dates
        except Exception as e:
            logger(f"❌ PostgreSQL 일정 조회 실패: {e}", "error")
            return None

    def speak_pdf_url_to_mongodb(self, pdfs, collection_name):
        """
        schedule_from_postgresql에서 받은 날짜로 pdf_url을 저장함
        """
        try:
            print(len(pdfs), len(pdfs[0]))
            for i in range(len(pdfs)):
                logger(f"MongoDB 저장 시도! {i}번째")
                pdf_data = pdfs[i]
                self.insert_document(collection_name, pdf_data)
                logger("✅ MongoDB 발언 데이터 저장 성공")
        except Exception as e:
            logger(f"❌ MongoDB 발언 데이터 저장 실패: {e}", "error")

    def change_get_status(self, pdf_dates, table_name):
        """
        그 이후 그 날짜에 get_pdf를 True로 값을 바꿈
        """
        try:
            query = f"UPDATE {table_name} SET get_pdf = TRUE WHERE get_pdf = FALSE AND meeting_date = %s"
            for date in pdf_dates:
                self.execute_query(query, (date,))  # ✅ 파라미터화된 쿼리 사용
                logger(f"✅ Postgresql pdf 가져온 상태 변환 성공 {date}")
        except Exception as e:
            logger(f"❌ Postgresql pdf 가져온 상태 변환 실패: {e}", "error")

    def mongodb_pdf_url_to_postgresql(self, collection_name, table_name):
        """
        api로 받은 speak_pdf_url을 postgresql에 저장하는 함수
        """
        try:
            query = {}
            select = {
                "CONF_DATE": 1,
                "PDF_LINK_URL": 1,
                "TITLE": 1,
                "_id": 0,
            }  # _id는 제외하고 선택
            data = self.find_documents(collection_name, query, select)
            if data is None:  # Check if data is None
                logger("❌ MongoDB에서 가져온 데이터가 없습니다.", "error")
                return  # Early exit if no data
            logger("✅ MongoDB 발언 링크 가져오기 성공")
        except Exception as e:
            logger(f"❌ MongoDB 발언 링크 가져오기 실패: {e}", "error")
            return  # Early exit on failure

        try:
            for pdf_data in data:
                pdf_date = pdf_data["CONF_DATE"]
                pdf_link = pdf_data["PDF_LINK_URL"]
                pdf_title = pdf_data["TITLE"]
                self.execute_query(
                    f"""
                    INSERT INTO {table_name} (title, conf_date, pdf_url)
                    VALUES (%s, %s, %s)
                """,
                    (pdf_title, pdf_date, pdf_link),
                )  # Ensure the order matches
            self.connection.commit()
            logger("✅ PostgreSQL 발언 링크 저장 성공!")
        except Exception as e:
            self.connection.rollback()
            logger(f"❌ PostgreSQL 발언 링크 저장 실패: {e}", "error")

    def pdf_url_from_postgresql(self, table_name):
        try:
            pdf_urls = self.execute_query(
                f"SELECT pdf_url FROM {table_name} WHERE get_pdf = False"
            )
            logger("✅ PostgreSQL 발언 링크 읽어오기 성공!")
        except Exception as e:
            logger(f"❌ PostgreSQL 발언 링크 저장 실패: {e}", "error")

        return pdf_urls

    def close_connections(self):
        self.close()  # PostgreSQL & MongoDB 둘 다 close 실행
        logger("✅ 모든 데이터베이스 연결 종료")
