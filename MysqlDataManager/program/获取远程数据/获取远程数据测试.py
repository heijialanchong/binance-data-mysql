"""
    为了缩短获取时间，在整点前的五分钟先获取一次全部数据，在到点的时候再获取一次最新的数据
"""
import pymysql
from sqlalchemy import create_engine
import datetime
import time
from manager.mysql_func import *
import glob
import os
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import datetime
import pandas as pd

if __name__ == '__main__':

    print("【运行时间 : {}】".format(datetime.datetime.now()))
    """
        填写 sql = Mysql("", "", "")
        服务器地址，mysql用户名，mysql登入密码
    """
    sql = Mysql("43.155.11.151", "linxuan", "123456")
    data_df = sql.selet_from_table("bina") # 填写数据库
    print(data_df)
    df = sql.selet_from_table("bina", 'b_spot_1h',
                              "select*from {} where candle_begin_time >= '{}'".format('b_spot_1h', '2024-07-18 00:00:00'))
    df = pd.DataFrame(df)
    print(df.tail(10).to_markdown())