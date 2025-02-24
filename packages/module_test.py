from get_api import GET_API
from db_manager import DatabaseManager

if __name__ == "__main__":
    
    # api 가져오기
    get_api = GET_API()
    congress_schedule_url = "https://open.assembly.go.kr/portal/openapi/nekcaiymatialqlxr"
    committee_schedule_url = "https://open.assembly.go.kr/portal/openapi/nrsldhjpaemrmolla"
    
    # 본회의, 위원회 일정 가져오기
    congress_schedules = get_api.get_schedule(congress_schedule_url)
    committee_schedules = get_api.get_schedule(committee_schedule_url)
    
    db_manager = DatabaseManager(database_name="government")
    
    congress_schedule_colleciton_name = "congress_schedule"
    committee_schedule_colleciton_name = "committee_schedule"
    
    # MongoDB에 있는 schedule 삭제
    db_manager.drop_collection(congress_schedule_colleciton_name)
    db_manager.drop_collection(committee_schedule_colleciton_name)

    # MongoDB에 데이터 저장
    for i in range(len(congress_schedules)):
        print(f"리스트 개수 {i}")
        for j in range(len(congress_schedules[i])):
            print(f"총 크기 {i} 리스트 개수 {j}")
            congress_data = congress_schedules[i][j]
            congress_data["MEETING_DATE"] = congress_data.pop("MEETTING_DATE") # 데이터가 MEETTING_DATE로 오타나있음 MEETING_DATE로 수정
            db_manager.insert_document(congress_schedule_colleciton_name, congress_data)
    
    for i in range(len(committee_schedules)):
        print(f"리스트 개수 {i}")
        for j in range(len(committee_schedules[i])):
            print(f"총 크기 {i} 리스트 개수 {j}")
            committee_data = committee_schedules[i][j]
            committee_data['MEETING_DATE'] = '-'.join(committee_data['MEETING_DATE'][:10].split('.')) # 데이터 형식을 - 형태로 바꿔줌
            db_manager.insert_document(committee_schedule_colleciton_name, committee_data)
    
            
    # MongoDB에 저장된 데이터 읽어오기    
    congress_dates = db_manager.find_distinct_documents(congress_schedule_colleciton_name,"MEETING_DATE")
    committee_dates = db_manager.find_distinct_documents(committee_schedule_colleciton_name,"MEETING_DATE")
    print(congress_dates)
    print(committee_dates)
    
    # 읽어 온 뒤에 이제 Postgres에 없는 날짜만 Postgres에 저장하기
    congress_scehdules = db_manager.schedule_from_postgresql(congress_schedule_colleciton_name)
    committee_scehdules = db_manager.schedule_from_postgresql(committee_schedule_colleciton_name)
    
    db_manager.schedule_from_postgresql(congress_schedule_colleciton_name)
    # Postgres에서 안 읽힌 날짜만 pdf url 일어오기
    
    
    
    # pdf url에서 text mongo db로 전환하기
