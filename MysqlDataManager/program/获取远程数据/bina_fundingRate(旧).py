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

current_directory = os.path.dirname(os.path.abspath(__file__))
# 获取当前程序的文件名
filename = os.path.basename(__file__)
target_folder = os.path.splitext(filename)[0]
target_folder_path = os.path.join(current_directory, target_folder)

# ==========  基础参数设置

debug = False # 调试
sum = 2  # 数据生成份数
get_hour = 1499 # 获取数据，当前时间往前推N个小时开获取
run_minute = 0 # 运行时间，每小时50开始执行
retry_times = 10 # 读取mysql数据容错次数

"""
分段读取数据时间，因为太多的数据查询时间过长,容易导致卡住导致报错,并且报错重新读取之前查询浪费了过多时间，
分段读取，即使某段数据读取失败重新读取浪费的时间也有限
"""
read_days = 14

"""
分段读取数据时间，因为太多的数据查询时间过长,容易导致卡住导致报错,并且报错重新读取之前查询浪费了过多时间，
分段读取，即使某段数据读取失败重新读取浪费的时间也有限
"""
read_sleep_time = 3 # 每次读取后休眠时间，太短也会导致频繁读取数据库报错，重新查询浪费了更多时间

# ==========  mysql参数设置

db_name = "bina" # 数据库
table_name = "funding_rate" # 表
db_addr = "43.155.11.151"  # 远程ip
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

    # df = sql.selet_from_table(db_name, table_name, "select*from {} where hour >= '{}'".format(table_name, start_time))
    # df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
    #                     text="select*from {} where fundingTime >= '{}'".format(table_name, start_time),
    #                     act_name='获取币安资金费率', retry_times= retry_times,sleep_seconds=5)

    # 分段查询数据，避免每次查询过长导致短开
    all_data = pd.DataFrame()
    delta = datetime.timedelta( days=read_days )

    end_time = datetime.datetime.now()
    while start_time < end_time:
        segment_end_time = min(start_time + delta, end_time)
        #print()
        #print(start_time )
        #print(datetime.datetime.now())
        #print()
        query_text = f"SELECT * FROM {table_name} WHERE fundingTime >= '{start_time}' AND fundingTime < '{segment_end_time}'"
        df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
                             text=query_text, act_name='获取币安资金费率', retry_times=retry_times, sleep_seconds=5)
        segment_data = pd.DataFrame(df)
        all_data = pd.concat([all_data, segment_data], ignore_index=True)
        start_time = segment_end_time
        time.sleep(read_sleep_time)

    df = pd.DataFrame(all_data)
    # 去除重复数据
    df = df.drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
    df = df.sort_values(['fundingTime','symbol'])
    df = df.reset_index(drop=True)

    print("获取历史数据")
    print(df.head(2).to_markdown())
    print(df.tail(2).to_markdown())

    # print(df[df['symbol'] == 'ZKUSDT'])

    file_name_list = []
    for n in range(sum):
        # 生成最新的数据文件
        n = n+1
        file_name = str(run_time).replace(':', '-')+" 1h {}.pkl".format(n)
        df_1 = df.copy()
        df_1.to_pickle(target_folder_path + '\\' + file_name)
        time.sleep(5) # pkl出错，每生成一个之间休眠1秒，一直调用io满了
        file_name_list.append(file_name)


    # 使用glob匹配当前文件夹下的所有.pkl文件
    pkl_files = glob.glob(target_folder_path + '\\' +'*.pkl')

    # 打印所有.pkl文件的路径,同时不等于三个路径的都删除
    for file_path in pkl_files:
        if (os.path.basename(file_path) not in file_name_list) :
                os.remove(file_path)
                # print(f"文件 '{file_path}' 已成功删除。")

    print(datetime.datetime.now()) #跟中性实盘对比，看下什么原因导致的选币过快
    cal_time(now_time)

if __name__ == '__main__':

    print("【 开始时间 : {} 】".format(datetime.datetime.now()))
    sql = Mysql(db_addr, user_name, user_password)

    if debug == False:


        scheduler = BackgroundScheduler(timezone=pytz.utc)
        # 新增容错时间 misfire_grace_time=120,20230709,之前延迟了一秒就报错了
        scheduler.add_job(main, 'cron', minute=run_minute, args=[sql], misfire_grace_time=120,  max_instances=3)  # 增加最大实例数)
        scheduler.start()

        # 这里跟前面没有关系,只是为了保持主线程不停止
        while(True):
            time.sleep(60*30)

    else:
        main(sql)