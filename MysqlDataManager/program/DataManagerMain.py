# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()
from config.config import *
from manager.exchange import binance_exchange
from manager.multi_manager_spot import *
from manager.multi_manager_swap import *
import time
import warnings
warnings.filterwarnings("ignore")
# 去掉报错
from warnings import filterwarnings
from pytz_deprecation_shim import PytzUsageWarning
filterwarnings('ignore', category=PytzUsageWarning)
from manager.mysql_func import *
import subprocess

def sync_windows_time():
    try:
        # 强制与 windows 时间服务器同步
        subprocess.run(["w32tm", "/resync"], check=True)
        print("✅ 时间同步成功")
    except Exception as e:
        print("❌ 时间同步失败:", e)

if __name__ == '__main__':

    # 创建数据库对象
    sql = Mysql(db_addr, user_name, user_password)

    spot_father_manager = SpotDataManagerFather( exchange=binance_exchange,needed_time_interval_list=needed_time_interval_list,
                                       sql = sql,db_name = db_name )

    swap_father_manager = SwapDataManagerFather( exchange=binance_exchange,needed_time_interval_list=needed_time_interval_list,
                                       sql = sql,db_name = db_name )


    # 这里跟前面没有关系,只是为了保持主线程不停止
    while True:
        """
        sync_windows_time()
        """
        time.sleep(60 * 5)  # 间隔60秒


# 这里获取的数据时间是上一个五分钟的