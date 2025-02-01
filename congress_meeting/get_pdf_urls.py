import json
import csv

# 나중에 airflow에 추가되야 하는 코드

def read_json(file_path):
    # json 파일 읽는 함수
    with open(file_path, 'r') as f:
        file = json.load(f)
    print('파일 읽기 완료')
    return file

def get_conf_dates():
    # conf_dates.csv를 리스트로 가져오는 함수
    file_path = 'congress_meeting/conf_dates.csv'
    conf_date_list = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            conf_date_list.append(row)
    return conf_date_list

def get_pdf_urls(file_path):
    # pdf_url을 중복 제거해서 가져오는 함수
    file = read_json(file_path)
    url_count = file['nzbyfwhwaoanttzje'][0]['head'][0]['list_total_count']
    url_set = set()
    for i in range(url_count):
        url_set.add(file['nzbyfwhwaoanttzje'][1]['row'][i]['CONF_LINK_URL'])
    print('url_set가 저장되었습니다.')
    return url_set

def pdf_urls_to_csv():
    # conf_date와 url을 가져와서 csv로 저장하는 함수
    conf_date_list = get_conf_dates()
    write_path = 'congress_meeting/pdf_url.csv'
    with open(write_path,'w') as wf:
        writer = csv.writer(wf)
        for conf_date in conf_date_list:
            read_path = f"congress_meeting/meetings/{conf_date[0]}.json"
            writer.writerow([conf_date,get_pdf_urls(read_path)])
    print('csv 저장이 완료 되었습니다.')
            
        
if __name__ == "__main__":
    pdf_urls_to_csv()