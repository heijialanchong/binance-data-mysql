import time
import os
import pandas as pd

def run_time( now_time ):
    cal_time = time.time() - now_time
    m = int(cal_time / 60)
    s = int(cal_time % 60)
    print("【 花费时间 : {}m {}s 】".format(m, s) + "\n")

def get_csv_file_paths(folder_path):
    csv_file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.csv'):
                csv_file_paths.append(os.path.join(root, file))
    return csv_file_paths


    # 创新新的数据表
def creat_data_table(sql,db_name ,df,table_name):
    # 保留还没收盘的创建一个新表，用于开单调用
    sql.create_talbe(df, db_name, table_name, if_exists="replace")


    # .先修改 symbol的长度，在设置复合主键，以免数据重复
    text = "alter table {} modify column symbol varchar(50);".format(table_name)
    sql.selet_from_table( db_name, db_table=table_name, text=text)
    text = "ALTER TABLE {} ADD CONSTRAINT PK_{} PRIMARY KEY(candle_begin_time,symbol);".format(table_name,table_name)
    sql.selet_from_table( db_name, db_table=table_name, text=text)

def save_data(df, path):
    # 保存数据
    group = df.groupby('symbol')
    for s, g in group:
        save_path = path + s + '.pkl'
        if os.path.exists(save_path):
            df = pd.read_pickle(save_path)
            df = pd.concat([df, g], ignore_index=True)
        else:
            df = g
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
        df.drop_duplicates(subset=['candle_begin_time', 'symbol'], keep='last', inplace=True)
        df.sort_values(by='candle_begin_time', inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_pickle(save_path)


# 逻辑不一样,需要修改
def save_data_1(df, path):
    """
    保存数据
    """
    group = df.groupby('symbol')
    for s, g in group:
        save_path = path + s + '.pkl'
        df = g
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
        df.drop_duplicates(subset=['candle_begin_time', 'symbol'], keep='last', inplace=True)
        df.sort_values(by='candle_begin_time', inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_pickle(save_path)