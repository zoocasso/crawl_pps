import requests
import math
import pandas as pd

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt):
    serviceKey = "%2FlZ8wIKM8vHNKqzLdtZE4by4zTQM8ZZ7a8m6jyvG2%2B6x9IHutt%2FJyj5U2nkMyyQKGB%2F%2Fad%2FgXlRT28CUIzlkEQ%3D%3D"
    
    requests_url = f"http://apis.data.go.kr/1230000/ShoppingMallPrdctInfoService06/getShoppingMallPrdctInfoList01?inqryDiv=1&type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDt={inqryBgnDt}&inqryEndDt={inqryEndDt}&serviceKey={serviceKey}"
    res = requests.get(requests_url, verify = False)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))
    # api_list = list()
    # for item in item_list:
    #     api_dict = dict()
    #     api_dict['prdctImgUrl'] = item['prdctImgUrl']
    #     api_dict['cntrctCorpNm'] = item['cntrctCorpNm']
    #     api_dict['entrprsDivNm'] = item['entrprsDivNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctMthdNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['exclncPrcrmntPrdctYn']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['masYn']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['smetprCmptProdctYn']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctPrceAmt']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctUnit']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctMakrNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctDlvrPlceNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctDlvryCndtnNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctSplyRgnNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['dlvrTmlmtDaynum']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctLrgclsfcCd']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctLrgclsfcNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctMidclsfcCd']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctMidclsfcNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctClsfcNo']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctClsfcNoNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['dtilPrdctClsfcNo']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['dtilPrdctClsfcNoNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctIdntNo']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prdctSpecNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['shopngCntrctNo']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['shopngCntrctSno']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctDate']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctBgnDate']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctEndDate']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctDeptNm']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['prodctCertList']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['rgstDt']
    #     api_dict['DTILPRDCTCLSFCNO'] = item['cntrctCorpBizno']
    #     api_list.append(api_dict)
    TB_PPS_API = pd.DataFrame(item_list)
    TB_PPS_API.to_sql(name='TB_PPS_MALL_API_TEST', con=db_connection, if_exists='append', index=False)
    return repeat_count


if __name__ == '__main__':
    pageNo = 1
    numOfRows = "999"
    inqryBgnDt = "202405010000"
    inqryEndDt = "202405012359"
    cnt = get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            get_api(pageNo,numOfRows,inqryBgnDt,inqryEndDt)