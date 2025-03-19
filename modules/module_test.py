from get_api import GET_API
from db_manager import DatabaseManager
import logging

# 로그 설정
logging.basicConfig(
    filename="api_manager.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

if (
    __name__ == "__main__"
):  # 굳이 코드를 이런식으로 2번 하도록 만들 필요가 있을까 싶은데 # 일단은 한다
    # api 가져오기
    get_api = GET_API()

    # 1) 일정 데이터 가져오기
    congress_schedule_url = (
        "https://open.assembly.go.kr/portal/openapi/nekcaiymatialqlxr"
    )
    committee_schedule_url = (
        "https://open.assembly.go.kr/portal/openapi/nrsldhjpaemrmolla"
    )

    # 1-1) 본회의, 위원회 일정 가져오기
    logging.info("1-1 | 본회의 알정 api 시작")
    congress_schedules = get_api.get_schedule(congress_schedule_url)
    logging.info("1-1 |위원회 일정 api 시작")
    committee_schedules = get_api.get_schedule(committee_schedule_url)

    # 2) 일정데이터 MongoDB에 저장하기
    db_manager = DatabaseManager(database_name="government")

    congress_schedule_colleciton_name = (
        "congress_schedule"  # 컬랙션 이름과 테이블 이름은 동일함
    )
    committee_schedule_colleciton_name = "committee_schedule"

    # 2-1) MongoDB에 있는 schedule 삭제
    logging.info("2 | MongoDB에 있는 schedule 삭제")
    db_manager.drop_collection(congress_schedule_colleciton_name)
    db_manager.drop_collection(committee_schedule_colleciton_name)

    # 2-1) MongoDB에 데이터 저장
    logging.info("2-1 | MongoDB에 본회의 일정 저장")
    db_manager.schedule_to_mongodb(
        congress_schedules, congress_schedule_colleciton_name
    )
    logging.info("2-1 | MongoDB에 위원회 일정 저장")
    db_manager.schedule_to_mongodb(
        committee_schedules, committee_schedule_colleciton_name
    )

    # 3) 일정데이터 Postgresql에 저장하기

    # 3-1) MongoDB에 저장된 데이터 읽어오기
    logging.info("3-1 | MongoDB에 본회의 일정 읽어오기")
    congress_dates = db_manager.find_distinct_documents(
        congress_schedule_colleciton_name, "MEETING_DATE"
    )
    logging.info("3-1 | MongoDB에 위원회의 일정 읽어오기")
    committee_dates = db_manager.find_distinct_documents(
        committee_schedule_colleciton_name, "MEETING_DATE"
    )

    # 3-2) 읽어 온 뒤에 이제 Postgresql에 없는 날짜만 Postgresql에 저장하기
    logging.info("3-2 | Postgres에 본회의 일정 저장")
    db_manager.schedule_to_postgresql(congress_dates, congress_schedule_colleciton_name)
    logging.info("3-2 | Postgres에 위원회 일정 저장")
    db_manager.schedule_to_postgresql(
        congress_dates, committee_schedule_colleciton_name
    )

    # 4) 발언 링크 데이터 MongoDB에 저장하기
    congress_speak_url = "https://open.assembly.go.kr/portal/openapi/nzbyfwhwaoanttzje"
    committee_speak_url = "https://open.assembly.go.kr/portal/openapi/ncwgseseafwbuheph"

    congress_pdfs_colleciton_name = (
        "congress_pdf_url"  # 컬랙션 이름과 테이블 이름은 동일함
    )
    committee_pdfs_colleciton_name = "committee_pdf_url"

    # 4-1) Postgresql에서 get_pdf가 안 된 날짜만 가져옴
    logging.info("4-1 | Postgres에 본회의 일정 읽어오기")
    congress_meeting_dates = db_manager.schedule_from_postgresql(
        congress_schedule_colleciton_name
    )
    logging.info("4-1 | Postgres에 위원회 일정 읽어오기")
    committee_meeting_dates = db_manager.schedule_from_postgresql(
        committee_schedule_colleciton_name
    )

    # 4-2) Postgresql에서 안 읽힌 날짜만 speak pdf url api 가져오기
    logging.info("4-2 | Postgres에 본회의 pdf_url api 가져오기")
    congress_speak_pdfs, congress_get_pdf_dates = get_api.get_pdf_url(
        congress_speak_url, congress_meeting_dates, unit_cd="22", page_size=100
    )
    logging.info("4-2 | Postgres에 위원회 pdf_url api 가져오기")
    committee_speak_pdfs, committee_get_pdf_dates = get_api.get_pdf_url(
        committee_speak_url, congress_meeting_dates, unit_cd="22", page_size=100
    )

    # 4-3) MongoDB에 pdf url incremental하게 저장하기
    logging.info("4-3 | Postgres에 본회의 pdf_url 저장")
    db_manager.speak_pdf_url_to_mongodb(
        congress_speak_pdfs, congress_pdfs_colleciton_name
    )
    logging.info("4-3 | Postgres에 위원회 pdf_url 저장")
    db_manager.speak_pdf_url_to_mongodb(
        committee_speak_pdfs, committee_pdfs_colleciton_name
    )

    # 4-4) MongoDB에 저장된 날짜 Postgresql에서 상태 바꾸기
    logging.info("4-4 | Postgresql에서 본회의 get_pdf 상태 변환")
    db_manager.change_get_status(
        congress_get_pdf_dates, congress_schedule_colleciton_name
    )
    logging.info("4-4 | Postgresql에서 위원회 get_pdf 상태 변환")
    db_manager.change_get_status(
        committee_get_pdf_dates, committee_schedule_colleciton_name
    )

    # 4-5) MongoDB에 pdf url postgres에 정리해서 저장하기
    logging.info("4-5 | Postgresql에서 본회의 pdf_url 저장")
    db_manager.mongodb_pdf_url_to_postgresql(
        congress_pdfs_colleciton_name, congress_pdfs_colleciton_name
    )
    logging.info("4-5 | Postgresql에서 위원회 pdf_url 저장")
    db_manager.mongodb_pdf_url_to_postgresql(
        committee_pdfs_colleciton_name, committee_pdfs_colleciton_name
    )

    # 5) 발언 데이터 MongoDB에 저장하기
    db_manager.find_distinct_documents(pdf_url)
    pdf_text = get_api.get_pdf_content_in_url(pdf_url)

    # pdf url에서 text mongo db로 전환하기
