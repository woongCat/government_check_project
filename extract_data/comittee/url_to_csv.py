import json
import csv
import re

# 나중에 airflow에 추가되야 하는 코드

def read_json(file_path):
    """json 파일 읽는 함수"""
    with open(file_path, 'r') as f:
        file = json.load(f)
    print('파일 읽기 완료')
    return file

def get_conf_dates():
    """meeting_dates.csv를 리스트로 가져오는 함수"""
    file_path = 'comittee/csv/meeting_dates.csv'
    conf_date_list = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            conf_date_list.append(row[0]) # row로 읽으면 리스트로 가져옴
    return conf_date_list

def get_pdf_urls(file):
    """pdf_url을 중복 제거해서 가져오는 함수"""
    url_count = file[0]['ncwgseseafwbuheph'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file[0]['ncwgseseafwbuheph'][1]['row'][i]['PDF_LINK_URL'])
    print('url_set가 저장되었습니다.')
    return url_set

def get_pdf_title(file):
    """pdf_title을 중복 제거해서 가져오는 함수"""
    url_count = file[0]['ncwgseseafwbuheph'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file[0]['ncwgseseafwbuheph'][1]['row'][i]['TITLE'])
    print('pdf_title이 저장되었습니다.')
    return url_set

def make_pdf_id(conf_date, title):
    """ElasticSearch에 저장할 ID를 생성"""
    id_regex = re.compile("[0-9]*회 [0-9]*차 .*위원회")
    try:
        pdf_id = '-'.join(id_regex.findall(title)[0].split()) + '-' + conf_date
    except IndexError:
        pdf_id = f"unknown-{conf_date}"
    return pdf_id

def make_committee_name(title):
    """ElasticSearch에 저장할 committee_name를 생성"""
    committee_name = title.split()[-1]
    return committee_name

def pdf_urls_to_csv(): # csv가 나중에 postgrs여도 괜찮을 것 같음
    """conf_date와 url을 가져와서 csv로 저장하는 함수"""
    conf_date_list = get_conf_dates()
    write_path = 'comittee/csv/meeting_url.csv'
    # csv로 저장
    with open(write_path,'w') as wf:
        writer = csv.writer(wf)
        # id, title, date
        writer.writerow(["pdf_id", "title", "committee_name","conf_date",  "url"])
        # 날짜별 회의제목, 회의록url 가져오기
        for conf_date in conf_date_list:
            read_path = f"comittee/comittee_meetings/{conf_date}.json"
            file = read_json(read_path)
            if file:
                title_set = get_pdf_title(file) 
                url_set = get_pdf_urls(file)
                # 중복이 제거되고 안에 여러개가 있을 수 있으니 분리를 위해 추가
                for title,url in zip(title_set, url_set):
                    pdf_id = make_pdf_id(conf_date,title)
                    title = title.split('(')[0].strip()
                    committee_name = make_committee_name(title)
                    writer.writerow([pdf_id, title, committee_name, conf_date, url])
            else:
                print("file에 값이 존재하지 않습니다.")
    print('csv 저장이 완료 되었습니다.')
            
if __name__ == "__main__":
    pdf_urls_to_csv()