import pandas as pd
import time
import os
import numpy as np
from basic_data.func import *
from manager.mysql_func import *
from config.config import *



db_name = "other_data" # 数据库
table_name = "coin_coinmarketcap" # 数据表
start_time = "2023-05-24" # 起始时间
# 替换 'folder_path' 为你想要查找CSV文件的文件夹路径
folder_path = r'C:\Users\Administrator\Desktop\数据\coin-coinmarketcap'



now_time = time.time()
csv_file_paths = get_csv_file_paths(folder_path)

df_list = []
sql = Mysql(db_addr, user_name, user_password)

try:
    sql.drop_talbe(db_name, [table_name])
except:
    print("可能已经删除")
# 输出所有找到的CSV文件路径
cnt = 0
first_list = []
for path in csv_file_paths:

    print(path)
    df = pd.read_csv(path, parse_dates=['candle_begin_time'], skiprows=[0],encoding="gbk")
    df = df[['id', 'name', 'symbol', 'num_market_pairs', 'date_added', 'tags', 'max_supply', 'circulating_supply', 'total_supply', 'infinite_supply', 'cmc_rank', 'self_reported_circulating_supply', 'self_reported_market_cap', 'tvl_ratio', 'candle_begin_time', 'usd_price', 'usd_volume', 'BTC_usd_price', 'max_mcap', 'circulating_mcap', 'total_mcap', 'turnover_rate', 'added_timedelta', 'usd_price_pct', 'usd_price_pct_next']]
    df['tags'] =  df['tags'].astype('str')
    df['symbol'] = df['symbol'].str.replace('-', '')

    # 这里没有加8小时了，这里是按天的，模糊匹配了
    df = df[df['candle_begin_time'] >= start_time ]
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
    df = df.sort_values(by='candle_begin_time')

    #
    if cnt == 0:
        first_list = df.columns.tolist()
        print(df.tail(5).to_markdown())
        time.sleep(5)
        creat_data_table(sql,db_name ,df,table_name)
    else:
        sql.create_talbe(df, db_name, table_name, if_exists="append")

    cnt = cnt+1

run_time(now_time)