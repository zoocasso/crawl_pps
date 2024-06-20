import os
from dotenv import load_dotenv

load_dotenv()

from operator import itemgetter
import pandas as pd
import pymysql

mydb = pymysql.connect(host=os.getenv("host"),
                       user=os.getenv("user"),
                       password=os.getenv("password"),
                       database=os.getenv("database"),
                       cursorclass=pymysql.cursors.DictCursor)
cursor = mydb.cursor()
    
def load_tb_pps_mall_list(prdctClsfcNo):
    cursor.execute(f"""select cntrctCorpNm, prdctClsfcNo, prdctIdntNo, cntrctPrceAmt from TB_PPS_MALL_LIST where prdctClsfcNo = '{prdctClsfcNo}'""")
    pps_product_info_tb = cursor.fetchall()
    pps_product_info_tb = pd.DataFrame(pps_product_info_tb,columns =["cntrctCorpNm", "prdctClsfcNo", "prdctIdntNo", "cntrctPrceAmt"])
    return pps_product_info_tb