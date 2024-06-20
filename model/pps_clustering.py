import pps_load_data

import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from tqdm import tqdm

db_address = f"""mysql+pymysql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv("host")}/{os.getenv('database')}"""
db_conn = create_engine(db_address)
conn = db_conn.connect()

# dunn index
def delta(ck, cl):
    values = np.ones([len(ck), len(cl)])*10000
    
    for i in range(0, len(ck)):
        for j in range(0, len(cl)):
            values[i, j] = np.linalg.norm(ck[i]-cl[j])
            
    return np.min(values)
    
def big_delta(ci):
    values = np.zeros([len(ci), len(ci)])
    
    for i in range(0, len(ci)):
        for j in range(0, len(ci)):
            values[i, j] = np.linalg.norm(ci[i]-ci[j])
            
    return np.max(values)
    
def dunn(k_list):
    """ Dunn index [CVI]
    
    Parameters
    ----------
    k_list : list of np.arrays
        A list containing a numpy array for each cluster |c| = number of clusters
        c[K] is np.array([N, p]) (N : number of samples in cluster K, p : sample dimension)
    """
    deltas = np.ones([len(k_list), len(k_list)])*1000000
    big_deltas = np.zeros([len(k_list), 1])
    l_range = list(range(0, len(k_list)))
    
    for k in l_range:
        for l in (l_range[0:k]+l_range[k+1:]):
            deltas[k, l] = delta(k_list[k], k_list[l])
        
        big_deltas[k] = big_delta(k_list[k])

    di = np.min(deltas)/np.max(big_deltas)
    return di

def make_cluster_table(df):
    # 2차원(가격X가격) 설정 및 데이터정규화
    price = pd.DataFrame(df,columns = ['cntrctPrceAmt', 'cntrctPrceAmt'])

    scaler = MinMaxScaler()
    data_scale = scaler.fit_transform(price)

    # dunn index 계산(최대 가격 클러스터 수를 4로 설정)
    dunn_list = []
    for i in range(2, 6) :
        clus = []
        np.random.seed(100)
        kmeans = KMeans(n_clusters = i)
        y_pred = kmeans.fit_predict(data_scale)
        price['labels'] = kmeans.labels_
        
        for j in range(i) :
            clus.append(price[price['labels'] == j].values)
        dunn_list.append(dunn(clus))

    k = np.argmax(dunn_list) + 1
    # print(f'최적 클러스터 수 : {k}개')

     # K-means 클러스터링
    model = KMeans(n_clusters=k, random_state=10)
    model.fit(data_scale)
    df['cluster'] = model.fit_predict(data_scale)

    # 가격 범위
    for j in range(k):
        cluster_price = df.loc[df['cluster'] == j, 'cntrctPrceAmt']
        price_range_list.append(f"{min(cluster_price)};{max(cluster_price)}")    
    return df


if __name__ == '__main__':
    with open('./model/input.txt','r') as f:
        input_list = f.readlines()
        input_list = [v.strip("\n").strip("\ufeff") for v in input_list]

    for prdctClsfcNo in tqdm(input_list, desc="clustering", mininterval=1):
        try:
            price_range_list = list()

            #product_info db불러오기 (df형태)
            product_info = pps_load_data.load_tb_pps_mall_list(prdctClsfcNo)

            #cluster계산하여 저장 (df형태)
            cluster_product_info = make_cluster_table(product_info)

            #cluster별 price_range 계산하여 df로 저장하기
            cluster_price_range_list = list()
            for i in range(len(price_range_list)):
                cluster_price_range_dict = dict()
                cluster_price_range_dict['prdctClsfcNo'] = prdctClsfcNo
                cluster_price_range_dict['cluster'] = i
                cluster_price_range_dict['minPrice'] = price_range_list[i].split(";")[0]
                cluster_price_range_dict['maxPrice'] = price_range_list[i].split(";")[1]
                cluster_price_range_list.append(cluster_price_range_dict)
            cluster_price_range_df = pd.DataFrame(cluster_price_range_list)

            # cluster_product_info.to_csv("./clustering.csv")
            cluster_product_info.to_sql(name='pps_clustering',con=db_conn,if_exists='append',index=False)

            # cluster_price_range_df.to_csv("./price.csv")
            cluster_price_range_df.to_sql(name='pps_clustering_price',con=db_conn,if_exists='append',index=False)
        except:
            with open('error.txt','a') as f:
                f.write(prdctClsfcNo)
                f.write("\n")