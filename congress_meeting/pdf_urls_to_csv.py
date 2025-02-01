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
    """conf_dates.csv를 리스트로 가져오는 함수"""
    file_path = 'congress_meeting/conf_dates.csv'
    conf_date_list = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            conf_date_list.append(row[0]) # row로 읽으면 리스트로 가져옴
    return conf_date_list

def get_pdf_urls(file):
    """pdf_url을 중복 제거해서 가져오는 함수"""
    url_count = file['nzbyfwhwaoanttzje'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file['nzbyfwhwaoanttzje'][1]['row'][i]['CONF_LINK_URL'])
    print('url_set가 저장되었습니다.')
    return url_set

def get_pdf_title(file):
    """pdf_title을 중복 제거해서 가져오는 함수"""
    url_count = file['nzbyfwhwaoanttzje'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file['nzbyfwhwaoanttzje'][1]['row'][i]['TITLE'])
    print('pdf_title이 저장되었습니다.')
    return url_set

def make_pdf_id(conf_date,title):
    """elastic search에 id를 만들어주는 함수"""
    id_regex = re.compile("[0-9]*회 [0-9]*차")
    pdf_id = '-'.join(id_regex.findall(title)[0].split())+'-'+conf_date
    return pdf_id

def pdf_urls_to_csv():
    """conf_date와 url을 가져와서 csv로 저장하는 함수"""
    conf_date_list = get_conf_dates()
    write_path = 'congress_meeting/pdf_url.csv'
    with open(write_path,'w') as wf:
        writer = csv.writer(wf)
        for conf_date in conf_date_list:
            read_path = f"congress_meeting/meetings/{conf_date}.json"
            file = read_json(read_path)
            title_set = get_pdf_title(file) 
            url_set = get_pdf_urls(file)
            # 중복이 제거되고 안에 여러개가 있을 수 있으니 분리를 위해 추가
            for title,url in zip(title_set, url_set):
                pdf_id = make_pdf_id(conf_date,title)
                writer.writerow([pdf_id, conf_date, title, url])
    print('csv 저장이 완료 되었습니다.')
            
if __name__ == "__main__":
    pdf_urls_to_csv()