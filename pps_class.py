import pandas as pd
import requests
import time

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def unit_api(api_url,serviceKey,requests_url,pageNo,numOfRows):
    unit_url = f'{requests_url}/{api_url}?type=json&pageNo={pageNo}&numOfRows={numOfRows}&ServiceKey={serviceKey}'
    res = requests.get(unit_url, verify=False)
    time.sleep(10)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    api_tb = pd.DataFrame(item_list)
    api_tb.columns=['PRDCTCLSFCNO','PRDCTCLSFCNONM','PRDCTCLSFCNOENGNM','PRDCTCLSFCNONMDSCRPT','USEYN','CHGDATE']
    api_tb.to_sql(name='class_tb', con=db_connection, if_exists='append', index=False)
    if int(total_count)<int(numOfRows)*int(pageNo):
        return None
    else:
        pageNo += 1
        unit_api(api_url,serviceKey,requests_url,pageNo,numOfRows)
    

if __name__ == '__main__':
    serviceKey = "lXysDvYOCh6QXuka88ofyLY8joQoc1JeQgAb%2BoeGcVCgBXcoUQSAJcLk8hTlWsccOAYEEF%2BPJmE5G2M0zgRxRQ%3D%3D"
    requests_url = "http://apis.data.go.kr/1230000/ThngListInfoService03"
    pageNo = 1
    numOfRows = 999

    level1_api_url = 'getPrdctClsfcNoUnit2Info01'
    level2_api_url = 'getPrdctClsfcNoUnit4Info01'
    level3_api_url = 'getPrdctClsfcNoUnit6Info01'
    level4_api_url = 'getPrdctClsfcNoUnit8Info01'
    level5_api_url = 'getPrdctClsfcNoUnit10Info01'
    
    unit_api(level1_api_url,serviceKey,requests_url,pageNo,numOfRows)
    unit_api(level2_api_url,serviceKey,requests_url,pageNo,numOfRows)
    unit_api(level3_api_url,serviceKey,requests_url,pageNo,numOfRows)
    unit_api(level4_api_url,serviceKey,requests_url,pageNo,numOfRows)
    unit_api(level5_api_url,serviceKey,requests_url,pageNo,numOfRows)