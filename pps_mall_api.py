import requests
import math
import pandas as pd

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def get_api(pageNo,numOfRows,inqryBgnDate,inqryEndDate):
    serviceKey = "%2FlZ8wIKM8vHNKqzLdtZE4by4zTQM8ZZ7a8m6jyvG2%2B6x9IHutt%2FJyj5U2nkMyyQKGB%2F%2Fad%2FgXlRT28CUIzlkEQ%3D%3D"
    
    requests_url = f"http://apis.data.go.kr/1230000/ShoppingMallPrdctInfoService06/getShoppingMallPrdctInfoList01?inqryDiv=1&type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDate={inqryBgnDate}&inqryEndDate={inqryEndDate}&serviceKey={serviceKey}"
    res = requests.get(requests_url, verify = False, timeout=60)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))
    api_list = list()
    for item in item_list:
        api_dict = dict()
        api_dict['prdctImgUrl'] = item['prdctImgUrl']
        api_dict['cntrctCorpNm'] = item['cntrctCorpNm']
        api_dict['entrprsDivNm'] = item['entrprsDivNm']
        api_dict['cntrctMthdNm'] = item['cntrctMthdNm']
        api_dict['exclncPrcrmntPrdctYn'] = item['exclncPrcrmntPrdctYn']
        api_dict['masYn'] = item['masYn']
        api_dict['smetprCmptProdctYn'] = item['smetprCmptProdctYn']
        api_dict['cntrctPrceAmt'] = item['cntrctPrceAmt']
        api_dict['prdctUnit'] = item['prdctUnit']
        api_dict['prdctMakrNm'] = item['prdctMakrNm']
        api_dict['prdctDlvrPlceNm'] = item['prdctDlvrPlceNm']
        api_dict['prdctDlvryCndtnNm'] = item['prdctDlvryCndtnNm']
        api_dict['prdctSplyRgnNm'] = item['prdctSplyRgnNm']
        api_dict['dlvrTmlmtDaynum'] = item['dlvrTmlmtDaynum']
        api_dict['prdctLrgclsfcCd'] = item['prdctLrgclsfcCd']
        api_dict['prdctLrgclsfcNm'] = item['prdctLrgclsfcNm']
        api_dict['prdctMidclsfcCd'] = item['prdctMidclsfcCd']
        api_dict['prdctMidclsfcNm'] = item['prdctMidclsfcNm']
        api_dict['prdctClsfcNo'] = item['prdctClsfcNo']
        api_dict['prdctClsfcNoNm'] = item['prdctClsfcNoNm']
        api_dict['dtilPrdctClsfcNo'] = item['dtilPrdctClsfcNo']
        api_dict['dtilPrdctClsfcNoNm'] = item['dtilPrdctClsfcNoNm']
        api_dict['prdctIdntNo'] = item['prdctIdntNo']
        api_dict['prdctSpecNm'] = item['prdctSpecNm']
        api_dict['shopngCntrctNo'] = item['shopngCntrctNo']
        api_dict['shopngCntrctSno'] = item['shopngCntrctSno']
        api_dict['cntrctDate'] = item['cntrctDate']
        api_dict['cntrctBgnDate'] = item['cntrctBgnDate']
        api_dict['cntrctEndDate'] = item['cntrctEndDate']
        api_dict['cntrctDeptNm'] = item['cntrctDeptNm']
        api_dict['prodctCertList'] = item['prodctCertList']
        api_dict['rgstDt'] = item['rgstDt']
        api_dict['cntrctCorpBizno'] = item['cntrctCorpBizno']
        api_list.append(api_dict)
    TB_PPS_API = pd.DataFrame(api_list)
    TB_PPS_API.to_sql(name='TB_PPS_MALL_LIST', con=db_connection, if_exists='append', index=False)
    return repeat_count


if __name__ == '__main__':
    pageNo = 1
    numOfRows = "999"
    inqryBgnDate = "20240501"
    inqryEndDate = "20240531"
    cnt = get_api(pageNo,numOfRows,inqryBgnDate,inqryEndDate)
    while True:
        print(cnt)
        if cnt == 0:
            break
        else:
            cnt -= 1
            pageNo += 1
            get_api(pageNo,numOfRows,inqryBgnDate,inqryEndDate)