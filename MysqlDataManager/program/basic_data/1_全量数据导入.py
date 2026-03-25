import pandas as pd
import time
import os
import numpy as np
from basic_data.func import *
from manager.mysql_func import *
from config.config import *



db_name = "coin_chain_basic_data"
table_name = "eth"
start_time = "2025-08-21 01:00:00"
# 替换 'folder_path' 为你想要查找CSV文件的文件夹路径
folder_path = r'C:\Users\Administrator\Desktop\数据\coin-chain-basic-data'



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


    df = pd.read_csv(path, parse_dates=['hour'], skiprows=[0],encoding="gbk")
    df['symbol'] = df['symbol'].str.replace('-', '')
    if len(df.columns.tolist())<21:
        print("是稳定币跳过")
        continue

    df['hour'] = df['hour'] + datetime.timedelta(hours=8)
    df = df[df['hour'] >= start_time ]

    df['gas_fee_total'] = df['gas_fee_total'].astype(float)

    if cnt == 0:
        first_list = df.columns.tolist()
        # print(df.tail(5).to_markdown())
        time.sleep(5)
        creat_data_table(sql,db_name ,df,table_name)
    else:
        sql.create_talbe(df, db_name, table_name, if_exists="append")

    cnt = cnt+1

run_time(now_time)