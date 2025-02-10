from common.api_utils import load_api_key, fetch_data
from common.db_utils import save_to_mongo, get_schedule, schedule_to_postgresql

# airflow에 추가해야함

if __name__ == "__main__":
    url = "https://open.assembly.go.kr/portal/openapi/nekcaiymatialqlxr"
    key = load_api_key()

    if key is None:
        raise ValueError("❌ OPEN_GOVERMETN_API_KEY 환경 변수가 설정되지 않았습니다.")
    db_name = "government"
    collection_name = "congress_schedule"

    all_data = fetch_data(url, key)
    save_to_mongo(all_data[0], db_name,collection_name)
    schedule = get_schedule(db_name,collection_name)
    schedule_to_postgresql(schedule,collection_name)