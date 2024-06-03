from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
from tqdm import tqdm

import pymysql
connection = pymysql.connect(host='127.0.0.1',
                            user='root',
                            password='vision9551',
                            database='pps_test')
cursor = connection.cursor()

from sqlalchemy import create_engine
db_connection_str = 'mysql+pymysql://root:vision9551@127.0.0.1/pps_test'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

def insert_db(table1_list, table2_list, table3_list):
    """
        공통속성정보 테이블 insert
    """
    공통속성정보1_tb = pd.DataFrame(table1_list)
    공통속성정보2_tb = pd.DataFrame(table2_list)
    공통속성정보_tb = pd.concat([공통속성정보1_tb,공통속성정보2_tb], axis=1)
    공통속성정보_tb = 공통속성정보_tb.loc[:,["물품목록번호","물품분류번호","물품식별번호","품명","세부품명번호","세부품명영문명","단위","내용연수","상품원산지국가명","품목구분","부품여부","품목등록일","모델명","품목명","제조업체명","제품설명"]]
    공통속성정보_tb.columns = ["PRDNO","PRDCTCLSFCNO","PRDCTIDNTNO","PRDCTCLSFCNONM","DTILPRDCTCLSFCNO","DTILPRDCTCLSFCNOENGNM","UNIT","USEFULLIFE","ORIGINCOUNTRY","PRDLSTDIV","CMPNTYN","RGSTDT","MODELNM","PRDNM","MNFCTCORPNM","PRDDESCRIPTION"]
    공통속성정보_tb.to_sql(name='TB_PPS_COMMON_ATTRIBUTE_INFORMATION',con=db_connection, if_exists='append', index=False)

    """
        개별속성정보 테이블 insert
    """
    개별속성정보_tb = pd.DataFrame(table3_list, columns=['NAME','VALUE','UNIT','PRDCTCLSFCNO','PRDCTIDNTNO'])
    개별속성정보_tb.to_sql(name='TB_PPS_INDIVIDUAL_ATTRIBUTE_INFORMATION',con=db_connection, if_exists='append', index=False)

def pps_crawl(rows):
    options = Options()
    driver = webdriver.Firefox(options=options)

    table1_list = list()
    table2_list = list()
    table3_list = list()
    for i in tqdm(rows):
        URL_ADDRESS = f"https://www.g2b.go.kr:8053/search/productSearchView.do?goodsClsfcNo={i['PRDCTCLSFCNO']}&goodsIdntfcNo={i['PRDCTIDNTNO']}"
        driver.get(URL_ADDRESS)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        table_list = soup.find_all('table', {'class':'tableType_ViewPop'})

        
        for table in table_list:
            table_name = table.find('caption').get_text()
            try:
                if table_name == "공통속성정보":
                    th_list = table.find_all('th')
                    td_list = table.find_all('td')
                    td_image = table.find('td',{'class':'txt-center'})

                    th_list = [v.get_text() for v in th_list]
                    td_list = [v.get_text() for v in td_list]

                    if td_image != None:
                        td_image = td_image.get_text()
                        td_list.remove(td_image)

                    th_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),th_list))
                    td_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),td_list))

                    
                    table1_dict = dict(zip(th_list, td_list))
                    table1_list.append(table1_dict)
            except:
                pass

            try:
                if table_name == "공통속성정보2":
                    th_list = table.find_all('th')
                    td_list = table.find_all('td')
                    td_image = table.find('td',{'class':'txt-center'})

                    th_list = [v.get_text() for v in th_list]
                    td_list = [v.get_text() for v in td_list]

                    if td_image != None:
                        td_image = td_image.get_text()
                        td_list.remove(td_image)

                    th_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),th_list))
                    td_list = list(map(lambda x : x.replace('\n','').replace('\t','').replace('  ',''),td_list))

                    table2_dict = dict(zip(th_list, td_list))
                    table2_list.append(table2_dict)
            except:
                pass

            try:
                if table_name == "개별속성정보":
                    tr_list = table.find('tbody').find_all('tr')
                    for tr in tr_list:
                        table3_dict = dict()
                        td_list = tr.find_all('td')
                        td_list = list(map(lambda x : x.get_text(), td_list))
                        table3_dict['NAME'] = td_list[0]
                        table3_dict['VALUE'] = td_list[1] 
                        table3_dict['UNIT'] = td_list[2]
                        table3_dict['PRDCTCLSFCNO'] = table1_dict['물품분류번호']
                        table3_dict['PRDCTIDNTNO'] = table1_dict['물품식별번호']
                        table3_list.append(table3_dict)
            except:
                pass
            
    driver.close()
    return table1_list, table2_list, table3_list

def get_db_data(years,months):
    print(f'{years}-{str(months).zfill(2)}')
    cursor.execute(f"select PRDCTCLSFCNO,PRDCTIDNTNO from TB_PPS_API where RGSTDT like '%{years}-{str(months).zfill(2)}%'")
    rows = cursor.fetchall()
    temp_list = list()
    for i in rows:
        temp_dict = dict()
        temp_dict["PRDCTCLSFCNO"] = i[0]
        temp_dict["PRDCTIDNTNO"] = i[1]
        temp_list.append(temp_dict)
    return temp_list


if __name__ == '__main__':
    end_year = 2024
    start_year = 2024
    month_list = [5,6,7,8,9,10,11,12]
    for i in range(end_year+1-start_year):
        for j in month_list:
            years = i + start_year
            months = j
            rows = get_db_data(years,months)
            if rows != []:
                table1_list, table2_list, table3_list = pps_crawl(rows)
                insert_db(table1_list, table2_list, table3_list)