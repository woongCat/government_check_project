from pymongo import MongoClient
from dotenv import load_dotenv
import psycopg2
import os
import re

def connect_to_mongo(db_name, collection_name):
    """
    MongoDB 연결하는 함수.
    """
    load_dotenv()
    pwd = os.getenv("MONGODB_PWD") # 나중에 암호를 추가해야할 수 있음
    client = MongoClient(f"mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    return client, db, collection

def save_to_mongo(data, db_name, collection_name):
    """
    데이터를 MongoDB에 저장하는 함수.

    :param data: 저장할 데이터 (리스트 또는 딕셔너리)
    :param db_name: 사용할 데이터베이스 이름
    :param collection_name: 사용할 컬렉션 이름
    :param mongo_uri: MongoDB 연결 URI (기본값: 로컬 MongoDB)
    """
    try:
        client, db, collection = connect_to_mongo(db_name,collection_name)

        # 데이터 저장
        if isinstance(data, list):  # 리스트 형태면 여러 개 저장
            collection.insert_many(data)
        else:  # 단일 딕셔너리면 하나만 저장
            collection.insert_one(data)

        print(f"✅ mongodb 날짜 데이터 저장 완료! (DB: {db_name}, Collection: {collection_name})")
    except Exception as e:
        print(f"❌ mongodb 날짜 데이터 저장 실패: {e}")
    finally:
        client.close()
        
def get_schedule(db_name, collection_name):
    """
    위원회 및, 국회의원회의 날짜를 가져오는 함수.
    """
    try:
        client, db, collection = connect_to_mongo(db_name,collection_name)
        
        result = collection.find({},{"MEETTING_DATE":1,"_id":0}).to_list()
        print("✅ MongoDB 날짜 데이터 가져옴:")
        
        return result

    except Exception as e:
        print(f"❌ mongodb 날짜 데이터 저장 실패: {e}")
    finally:
        client.close()
        
def connect_to_postgresql():
    """
    postgresql에 연결하는 함수.
    """
    load_dotenv()
    conn = psycopg2.connect(
        host = os.getenv("SQL_HOST"),
        dbname = os.getenv("SQL_DBNAME"),
        user = os.getenv("SQL_USER"),
        password = os.getenv("SQL_PWD"),
        port = os.getenv("SQL_PORT")
    )
    return conn

def schedule_to_postgresql(schedule_list,collection_name):
    """
    postgresql에 위원날짜를 저장하는 함수.
    """
    try:
        conn = connect_to_postgresql()
        cur = conn.cursor()
        for meetting_date in schedule_list: # 매번 데이터를 다 읽는게 좀 꼬롬하네
            date = meetting_date["MEETTING_DATE"]
            cur.execute(f"""
                        INSERT INTO {collection_name} (meeting_date)
                        VALUES ({date})
                        ON CONFLICT (meeting_date) DO NOTHING
                        """)
        conn.commit()
        print("✅ postgres 데이터 저장 성공")
    except Exception as e:
        conn.rollback()
        print(f"❌ postgres 데이터 저장 실패 : {e}")
    finally:
        conn.close()
        
###
        
def schedule_from_postgresql(collection_name):
    """
    postgresql에 있는 위원회의 날짜를 전부 가져오는 함수.
    """
    try:
        conn = connect_to_postgresql()
        cur = conn.cursor()
        cur.execute(f"SELECT meeting_date FROM {collection_name} WHERE get_pdf = FALSE")
        
        # meeting_date 가져오기
        rows = cur.fetchall()
        meeting_dates = [row[0] for row in rows]
        
        print(f"✅ postgres 데이터 가져오기 성공: {meeting_dates}")
        return meeting_dates  # 가져온 날짜 리스트 반환

    except Exception as e:
        conn.rollback()
        print(f"❌ postgres 데이터 저장 실패 : {e}")
        return None
    finally:
        conn.close()

def get_pdf_urls(file):
    """pdf_url을 중복 제거해서 가져오는 함수"""
    try:
        url_count = file['nzbyfwhwaoanttzje'][0]['head'][0]['list_total_count']
        url_set = set()
        for i in range(url_count):
            url_set.add(file['nzbyfwhwaoanttzje'][1]['row'][i]['PDF_LINK_URL'])
        print('url_set가 저장되었습니다.')
    except Exception as e:
        print(f"pdf에서 url가져오기 실패 : {e}")
    return url_set

def get_pdf_title(file):
    """pdf_title을 중복 제거해서 가져오는 함수"""
    url_count = file['nzbyfwhwaoanttzje'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file['nzbyfwhwaoanttzje'][1]['row'][i]['TITLE'])
    print('pdf_title이 저장되었습니다.')
    return url_set

def make_pdf_id(conf_date,title): #확장성을 고려해 어느 회의인지도 추가하는 걸로
    """elastic search에 필요한 id를 만들어주는 함수"""
    id_regex = re.compile("[0-9]*회 [0-9]*차 국회본회의")
    pdf_id = '-'.join(id_regex.findall(title)[0].split())+'-'+conf_date
    return pdf_id

def pdf_urls_to_csv(collection_name): # csv가 나중에 postgrs여도 괜찮을 것 같음
    """conf_date와 url을 가져와서 csv로 저장하는 함수"""
    conf_date_list = schedule_from_postgresql(collection_name)
    # csv로 저장
    with open(write_path,'w') as wf:
        writer = csv.writer(wf)
        # id, title, date
        writer.writerow(["pdf_id", "title", "conf_date",  "url"])
        # 날짜별 회의제목, 회의록url 가져오기
        for conf_date in conf_date_list:
            read_path = f"congress_meeting/meetings/{conf_date}.json"
            file = read_json(read_path)
            title_set = get_pdf_title(file) 
            url_set = get_pdf_urls(file)
            # 중복이 제거되고 안에 여러개가 있을 수 있으니 분리를 위해 추가
            for title,url in zip(title_set, url_set):
                pdf_id = make_pdf_id(conf_date,title)
                title = title.split('(')[0].strip()
                writer.writerow([pdf_id, title, conf_date, url])
    print('csv 저장이 완료 되었습니다.')