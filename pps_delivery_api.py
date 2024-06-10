import requests
import math
import pandas as pd

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def get_api(pageNo,numOfRows,inqryBgnDate,inqryEndDate):
    serviceKey = "%2FlZ8wIKM8vHNKqzLdtZE4by4zTQM8ZZ7a8m6jyvG2%2B6x9IHutt%2FJyj5U2nkMyyQKGB%2F%2Fad%2FgXlRT28CUIzlkEQ%3D%3D"
    
    requests_url = f"http://apis.data.go.kr/1230000/ShoppingMallPrdctInfoService06/getDlvrReqDtlInfoList01?inqryDiv=1&type=json&numOfRows={numOfRows}&pageNo={str(pageNo)}&inqryBgnDate={inqryBgnDate}&inqryEndDate={inqryEndDate}&serviceKey={serviceKey}"
    res = requests.get(requests_url, verify = False, timeout=60)
    item_list = res.json()['response']['body']['items']
    total_count = res.json()['response']['body']['totalCount']
    repeat_count = math.floor(total_count/int(numOfRows))
    api_list = list()
    for item in item_list:
        api_dict = dict()
        api_dict['dlvrReqNo'] = item['dlvrReqNo']
        api_dict['dlvrReqChgOrd'] = item['dlvrReqChgOrd']
        api_dict['dlvrReqRcptDate'] = item['dlvrReqRcptDate']
        api_dict['prdctSno'] = item['prdctSno']
        api_dict['prdctClsfcNo'] = item['prdctClsfcNo']
        api_dict['prdctClsfcNoNm'] = item['prdctClsfcNoNm']
        api_dict['dtilPrdctClsfcNo'] = item['dtilPrdctClsfcNo']
        api_dict['dtilPrdctClsfcNoNm'] = item['dtilPrdctClsfcNoNm']
        api_dict['prdctIdntNo'] = item['prdctIdntNo']
        api_dict['prdctIdntNoNm'] = item['prdctIdntNoNm']
        api_dict['prdctUprc'] = item['prdctUprc']
        api_dict['prdctUnit'] = item['prdctUnit']
        api_dict['prdctQty'] = item['prdctQty']
        api_dict['prdctAmt'] = item['prdctAmt']
        api_dict['dlvrTmlmtDate'] = item['dlvrTmlmtDate']
        api_dict['cntrctCnclsStleNm'] = item['cntrctCnclsStleNm']
        api_dict['exclcProdctYn'] = item['exclcProdctYn']
        api_dict['optnDivCdNm'] = item['optnDivCdNm']
        api_dict['dminsttCd'] = item['dminsttCd']
        api_dict['dminsttNm'] = item['dminsttNm']
        api_dict['dmndInsttDivNm'] = item['dmndInsttDivNm']
        api_dict['dminsttRgnNm'] = item['dminsttRgnNm']
        api_dict['corpNm'] = item['corpNm']
        api_dict['fnlDlvrReqYn'] = item['fnlDlvrReqYn']
        api_dict['incdecQty'] = item['incdecQty']
        api_dict['incdecAmt'] = item['incdecAmt']
        api_dict['cntrctCorpBizno'] = item['cntrctCorpBizno']
        api_dict['dlvrReqNm'] = item['dlvrReqNm']
        api_dict['cntrctNo'] = item['cntrctNo']
        api_dict['cntrctChgOrd'] = item['cntrctChgOrd']
        api_dict['masYn'] = item['masYn']
        api_dict['cnstwkMtrlDrctPurchsObjYn'] = item['cnstwkMtrlDrctPurchsObjYn']
        api_dict['IntlCntrctDlvrReqDate'] = item['IntlCntrctDlvrReqDate']
        api_dict['dlvrReqQty'] = item['dlvrReqQty']
        api_dict['dlvrReqAmt'] = item['dlvrReqAmt']
        api_dict['smetprCmptProdctYn'] = item['smetprCmptProdctYn']
        api_dict['corpEntrprsDivNmNm'] = item['corpEntrprsDivNmNm']
        api_dict['brnofceNm'] = item['brnofceNm']
        api_list.append(api_dict)
    TB_PPS_API = pd.DataFrame(api_list)
    TB_PPS_API.to_sql(name='TB_PPS_SHOPPING_DELIVERY', con=db_connection, if_exists='append', index=False)
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