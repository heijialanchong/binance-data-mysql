"""
    为了缩短获取时间，在整点前的五分钟先获取一次全部数据，在到点的时候再获取一次最新的数据
"""
from manager.functions import *
from manager.mysql_func import *
import glob
import os
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import time
import numpy as np

current_directory = os.path.dirname(os.path.abspath(__file__))
# 获取当前程序的文件名
filename = os.path.basename(__file__)
target_folder = os.path.splitext(filename)[0]
target_folder_path = os.path.join(current_directory, target_folder)

# ==========  基础参数设置
debug = False # 调试
sum = 3 # 数据生成份数
get_hour = 1499 # 获取数据，当前时间往前推N个小时开获取
run_minute = 1 # 运行时间，每小时50开始执行
retry_times = 10 # 读取mysql数据容错次数
# ==========  mysql参数设置
db_name = "other_data"
table_name = "coin_coinmarketcap"
db_addr = "43.154.235.225"  # 远程ip
user_name = "linxuan"  # 访问账户
user_password = "123456"  # 访问密码


# 计算程序总的运行时间
def cal_time( now_time ):
    cal_time = time.time() - now_time
    m = int(cal_time / 60)
    s = int(cal_time % 60)
    print("【 花费时间 : {}m {}s 】".format(m, s) + "\n")

def main(sql):
    # 1.先获取全量数据
    # sleep_1() # 休眠到整点提前十分钟，先获取一次全量数据
    print("【 运行时间 : {} 】".format(datetime.datetime.now()))
    formatted_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now_time = time.time()

   # while True:

        # 9月24号程序出错了,导致都是空的,获取不到最新的时间,一直卡住,不是我的问题,重新观察
    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
    start_time = run_time - datetime.timedelta(hours=get_hour)

    #df = sql.selet_from_table(db_name, table_name, "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time))
    df = retry_wrapper_1(sql.selet_from_table,db_name=db_name, db_table=table_name,
                         text = "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time),
                         act_name='获取cmc_data', retry_times= retry_times)


    df = pd.DataFrame(df)

    # 价格次日涨跌
    df['usd_price_pct_next'] = df['usd_price_pct'].shift(-1)
    df.loc[df['id'] != df['id'].shift(-1), 'usd_price_pct_next'] = np.nan

    print(df.tail(5).to_markdown())
    print("获取历史数据")

    file_name_list = []
    for n in range(sum):
        # 生成最新的数据文件
        n = n+1
        file_name = str(run_time).replace(':', '-')+" 1h {}.pkl".format(n)
        df_1 = df.copy()
        df_1.to_pickle(target_folder_path + '\\'+file_name)
        time.sleep(3) # pkl出错，每生成一个之间休眠1秒，一直调用io满了
        file_name_list.append(file_name)

    # 使用glob匹配当前文件夹下的所有.pkl文件
    pkl_files = glob.glob(target_folder_path + '\*.pkl')

    # 打印所有.pkl文件的路径,同时不等于三个路径的都删除
    for file_path in pkl_files:
        if (os.path.basename(file_path) not in file_name_list) :
                os.remove(file_path)
                # print(f"文件 '{file_path}' 已成功删除。")

    print(datetime.datetime.now()) #跟中性实盘对比，看下什么原因导致的选币过快
    cal_time(now_time)

if __name__ == '__main__':

    print("【 运行时间 : {} 】".format(datetime.datetime.now()))

    sql = Mysql(db_addr , user_name,  user_password)

    if debug == False:


        scheduler = BackgroundScheduler(timezone=pytz.utc)
        # 新增容错时间 misfire_grace_time=120,20230709,之前延迟了一秒就报错了
        scheduler.add_job(main, 'cron', minute= run_minute, args=[sql], misfire_grace_time=120)
        scheduler.start()

        # 这里跟前面没有关系,只是为了保持主线程不停止
        while(True):
            time.sleep(60*30)

    else:
        main(sql)