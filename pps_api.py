import requests
import math
import pandas as pd

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt):
    serviceKey = "%2FlZ8wIKM8vHNKqzLdtZE4by4zTQM8ZZ7a8m6jyvG2%2B6x9IHutt%2FJyj5U2nkMyyQKGB%2F%2Fad%2FgXlRT28CUIzlkEQ%3D%3D"
    requests_url = f"http://apis.data.go.kr/1230000/ThngListInfoService03/getThngPrdnmLocplcAccotListInfoInfoPrdlstSearch01?type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDt={inqryBgnDt}&inqryEndDt={inqryEndDt}&serviceKey={serviceKey}"
    res = requests.get(requests_url, verify = False, timeout=60)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))

    api_list = list()
    for item in item_list:
        api_dict = dict()
        api_dict['prdctClsfcNo'] = item['prdctClsfcNo']
        api_dict['prdctIdntNo'] = item['prdctIdntNo']
        api_dict['prdctImgLrge'] = item['prdctImgLrge']
        api_dict['dtilPrdctClsfcNo'] = item['dtilPrdctClsfcNo']
        api_dict['prdctClsfcNoNm'] = item['prdctClsfcNoNm']
        api_dict['prdctClsfcNoEngNm'] = item['prdctClsfcNoEngNm']
        api_dict['krnPrdctNm'] = item['krnPrdctNm']
        api_dict['dltYn'] = item['dltYn']
        api_dict['useYn'] = item['useYn']
        api_dict['prcrmntCorpRgstNo'] = item['prcrmntCorpRgstNo']
        api_dict['mnfctCorpNm'] = item['mnfctCorpNm']
        api_dict['rgstDt'] = item['rgstDt']
        api_dict['chgDt'] = item['chgDt']
        api_dict['prodctCertList'] = item['prodctCertList']
        api_dict['prdlstDiv'] = item['prdlstDiv']
        api_dict['cmpntYn'] = item['cmpntYn']
        api_list.append(api_dict)
    TB_PPS_API = pd.DataFrame(api_list)
    TB_PPS_API.to_sql(name='TB_PPS_API', con=db_connection, if_exists='append', index=False)
    return repeat_count


if __name__ == '__main__':
    pageNo = 1
    numOfRows = "999"
    inqryBgnDt = "202405010000"
    inqryEndDt = "202405312359"
    
    cnt = get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)