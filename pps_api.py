import requests
import math
import pandas as pd

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt):
    serviceKey = "lXysDvYOCh6QXuka88ofyLY8joQoc1JeQgAb%2BoeGcVCgBXcoUQSAJcLk8hTlWsccOAYEEF%2BPJmE5G2M0zgRxRQ%3D%3D"
    requests_url = f"http://apis.data.go.kr/1230000/ThngListInfoService03/getThngPrdnmLocplcAccotListInfoInfoPrdlstSearch01?type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDt={inqryBgnDt}&inqryEndDt={inqryEndDt}&serviceKey={serviceKey}"
    res = requests.get(requests_url, verify = False)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))

    api_list = list()
    for item in item_list:
        api_dict = dict()
        api_dict['PRDCTCLSFCNO'] = item['prdctClsfcNo']
        api_dict['PRDCTIDNTNO'] = item['prdctIdntNo']
        api_dict['PRDCTIMGLRGE'] = item['prdctImgLrge']
        api_dict['DTILPRDCTCLSFCNO'] = item['dtilPrdctClsfcNo']
        api_dict['PRDCTCLSFCNONM'] = item['prdctClsfcNoNm']
        api_dict['PRDCTCLSFCNOENGNM'] = item['prdctClsfcNoEngNm']
        api_dict['KRNPRDCTNM'] = item['krnPrdctNm']
        api_dict['DLTYN'] = item['dltYn']
        api_dict['USEYN'] = item['useYn']
        api_dict['PRCRMNTCORPRGSTNO'] = item['prcrmntCorpRgstNo']
        api_dict['MNFCTCORPNM'] = item['mnfctCorpNm']
        api_dict['RGSTDT'] = item['rgstDt']
        api_dict['CHGDT'] = item['chgDt']
        api_dict['PRODCTCERTLIST'] = item['prodctCertList']
        api_dict['PRDLSTDIV'] = item['prdlstDiv']
        api_dict['CMPNTYN'] = item['cmpntYn']
        api_list.append(api_dict)
    TB_PPS_API = pd.DataFrame(api_list)
    TB_PPS_API.to_sql(name='TB_PPS_API', con=db_connection, if_exists='append', index=False)
    return repeat_count


if __name__ == '__main__':
    pageNo = 1
    numOfRows = "999"
    inqryBgnDt = "202401010000"
    inqryEndDt = "202404302359"
    
    cnt = get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)