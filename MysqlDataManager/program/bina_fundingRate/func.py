import time
import os
import pandas as pd

def run_time( now_time ):
    cal_time = time.time() - now_time
    m = int(cal_time / 60)
    s = int(cal_time % 60)
    print("【 花费时间 : {}m {}s 】".format(m, s) + "\n")

    # 创新新的数据表
def creat_data_table(sql,db_name ,df,table_name):
    # 保留还没收盘的创建一个新表，用于开单调用
    sql.create_talbe(df, db_name, table_name, if_exists="replace")


    # .先修改 symbol的长度，在设置复合主键，以免数据重复
    text = "alter table {} modify column symbol varchar(50);".format(table_name)
    sql.selet_from_table( db_name, db_table=table_name, text=text)
    text = "ALTER TABLE {} ADD CONSTRAINT PK_{} PRIMARY KEY(fundingTime,symbol);".format(table_name,table_name)
    sql.selet_from_table( db_name, db_table=table_name, text=text)

