from modules.fetch_from_api import fetch_schedule, fetch_pdf_url
from modules.pdf_reader import fetch_pdf_content_in_url, get_speaker_data
from modules.database_connectors import PostgreSQLManager
from loguru import logger


if __name__ == "__main__":
    # api 가져오기

    # 1) 일정 데이터 가져오기
    congress_schedule_url = (
        "https://open.assembly.go.kr/portal/openapi/nekcaiymatialqlxr"
    )
    committee_schedule_url = (
        "https://open.assembly.go.kr/portal/openapi/nrsldhjpaemrmolla"
    )

    # 1-1) 본회의, 위원회 일정 가져오기
    logger.info("1-1 | 본회의 알정 api 시작")
    congress_schedules = fetch_schedule(congress_schedule_url)
    logger.info("1-1 |위원회 일정 api 시작")
    committee_schedules = fetch_schedule(committee_schedule_url)

    # 2) 일정데이터 Postgresql에 저장하기
    postgres_manager = PostgreSQLManager(database_name="government")

    congress_schedule_colleciton_name = (
        "congress_schedule"  # 컬랙션 이름과 테이블 이름은 동일함
    )
    committee_schedule_colleciton_name = "committee_schedule"

    # 2-1) Postgresql에 있는 schedule 삭제
    logger.info("2 | Postgresql에 있는 schedule 삭제")
    postgres_manager.clear_table(congress_schedule_colleciton_name)
    postgres_manager.clear_table(committee_schedule_colleciton_name)

    # 2-1) Postgresql에 데이터 저장
    logger.info("2-1 | Postgresql에 본회의 일정 저장")
    postgres_manager.schedule_to_postgresql(
        congress_schedules, congress_schedule_colleciton_name
    )
    logger.info("2-1 | Postgresql에 위원회 일정 저장")
    postgres_manager.schedule_to_postgresql(
        committee_schedules, committee_schedule_colleciton_name
    )

    # 3) 일정데이터 Postgresql에 저장하기

    # 3) 일정데이터 Postgresql에 저장하기 (이미 위에서 저장했으므로 생략 가능)
    # 만약 중복 저장 방지나 특정 로직이 필요하다면, PostgreSQLManager에서 직접 distinct/exists 체크를 구현해야 함.

    # 4) 발언 링크 데이터 Postgresql에 저장하기
    congress_speak_url = "https://open.assembly.go.kr/portal/openapi/nzbyfwhwaoanttzje"
    committee_speak_url = "https://open.assembly.go.kr/portal/openapi/ncwgseseafwbuheph"

    congress_pdfs_colleciton_name = (
        "congress_pdf_url"  # 컬랙션 이름과 테이블 이름은 동일함
    )
    committee_pdfs_colleciton_name = "committee_pdf_url"

    # 4-1) Postgresql에서 get_pdf가 안 된 날짜만 가져옴
    logger.info("4-1 | Postgres에 본회의 일정 읽어오기")
    congress_meeting_dates = postgres_manager.schedule_from_postgresql(
        congress_schedule_colleciton_name
    )
    logger.info("4-1 | Postgres에 위원회 일정 읽어오기")
    committee_meeting_dates = postgres_manager.schedule_from_postgresql(
        committee_schedule_colleciton_name
    )

    # 4-2) Postgresql에서 안 읽힌 날짜만 speak pdf url api 가져오기
    logger.info("4-2 | Postgres에 본회의 pdf_url api 가져오기")
    congress_speak_pdfs, congress_get_pdf_dates = fetch_pdf_url(
        congress_speak_url, congress_meeting_dates, unit_cd="22", page_size=100
    )
    logger.info("4-2 | Postgres에 위원회 pdf_url api 가져오기")
    committee_speak_pdfs, committee_get_pdf_dates = fetch_pdf_url(
        committee_speak_url, congress_meeting_dates, unit_cd="22", page_size=100
    )

    # 4-3) Postgresql에 pdf url incremental하게 저장하기
    logger.info("4-3 | Postgres에 본회의 pdf_url 저장")
    postgres_manager.speak_pdf_url_to_postgresql(
        congress_speak_pdfs, congress_pdfs_colleciton_name
    )
    logger.info("4-3 | Postgres에 위원회 pdf_url 저장")
    postgres_manager.speak_pdf_url_to_postgresql(
        committee_speak_pdfs, committee_pdfs_colleciton_name
    )

    # 4-4) Postgresql에 저장된 날짜 Postgresql에서 상태 바꾸기
    logger.info("4-4 | Postgresql에서 본회의 get_pdf 상태 변환")
    postgres_manager.change_get_status(
        congress_get_pdf_dates, congress_schedule_colleciton_name
    )
    logger.info("4-4 | Postgresql에서 위원회 get_pdf 상태 변환")
    postgres_manager.change_get_status(
        committee_get_pdf_dates, committee_schedule_colleciton_name
    )

    # 4-5) pdf url을 Postgresql에 정리해서 저장하기 (이미 위에서 저장했으므로 생략 또는 아래와 같이 사용)
    logger.info("4-5 | Postgresql에서 본회의 pdf_url 저장")
    postgres_manager.insert_pdf_urls(
        congress_pdfs_colleciton_name, congress_pdfs_colleciton_name
    )
    logger.info("4-5 | Postgresql에서 위원회 pdf_url 저장")
    postgres_manager.insert_pdf_urls(
        committee_pdfs_colleciton_name, committee_pdfs_colleciton_name
    )

    # 5) 발언 데이터 Postgresql에 저장하기
    # db_manager.find_distinct_documents(pdf_url)
    # pdf_text = get_api.get_pdf_content_in_url(pdf_url)

    # pdf url에서 text Postgresql로 전환하기
