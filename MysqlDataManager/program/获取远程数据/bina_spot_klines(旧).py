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

debug = True # 调试

sum = 2 # 数据生成份数
get_hour = 1499 # 获取数据，当前时间往前推N个小时开获取
run_minute = 40 # 运行时间，每小时50开始执行
retry_times = 10 # 读取mysql数据容错次数

# ==========  mysql参数设置

db_name = "bina" # 数据库
table_name = 'b_spot_1h' # 表b_spot_1h b_swap_1h
db_addr = "43.155.11.151" # 远程ip
user_name = "linxuan" # 访问账户
user_password = "123456" # 访问密码

# 计算程序总的运行时间
def cal_time( now_time ):
    cal_time = time.time() - now_time
    m = int(cal_time / 60)
    s = int(cal_time % 60)
    print("【 花费时间 : {}m {}s 】".format(m, s) + "\n")

def sleep_1():

    # 获取当前时间
    now = datetime.datetime.now()
    # 计算下一个整点的时间
    next_hour = now.replace(second=0, microsecond=0, minute=0) + datetime.timedelta(hours=1)

    # 计算休眠时间（减去五分钟），minutes 调成 原先10测试调成20
    sleep_time = (next_hour - datetime.timedelta(minutes=10) - now).total_seconds()

    # 休眠到下一个整点
    print("首次休眠 {}s".format(sleep_time))
    time.sleep(sleep_time)



def sleep_2():

    # 获取当前时间
    now = datetime.datetime.now()
    # 计算下一个整点的时间,minute=0 测试 调成10
    next_hour = now.replace(second=0, microsecond=0, minute=0) + datetime.timedelta(hours=1)
    # 计算休眠时间
    sleep_time = (next_hour - now).total_seconds()

    # 休眠到下一个整点
    print("二次休眠 {}s".format(sleep_time))
    time.sleep(sleep_time)

def main(sql):

    # 1.先获取全量数据
    # sleep_1() # 休眠到整点提前十分钟，先获取一次全量数据
    print("【 运行时间 : {} 】".format(datetime.datetime.now()))

    formatted_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    now_time = time.time()
    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
    start_time = run_time - datetime.timedelta( hours = get_hour)
    # df = sql.selet_from_table(db_name , table_name, "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time))
    # df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
    #                     text = "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time),
    #                     act_name='获取币种K线数据', retry_times=retry_times)

    # 分段查询数据，避免每次查询过长导致短开

    all_data = pd.DataFrame()
    delta = datetime.timedelta(days=14)

    end_time = datetime.datetime.now()
    while start_time < end_time:
        segment_end_time = min(start_time + delta, end_time)
        print()
        print(start_time )
        print(datetime.datetime.now())
        print()
        query_text = f"SELECT * FROM {table_name} WHERE candle_begin_time >= '{start_time}' AND candle_begin_time < '{segment_end_time}'"
        df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
                             text=query_text, act_name='获取币种现货K线数据', retry_times=retry_times, sleep_seconds=5)
        segment_data = pd.DataFrame(df)
        all_data = pd.concat([all_data, segment_data], ignore_index=True)
        start_time = segment_end_time
        time.sleep(5)

    df = pd.DataFrame(df)
    # 去除重复数据
    df = df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')

    df.to_pickle(target_folder_path+ '\data.pkl')
    cal_time(now_time)

    if debug == False:
        sleep_2() # 休眠到整点,获取一个数据
    time.sleep(1) # 多休眠两秒免得时间没过去


    # 2.再次更新全量数据，获取最近的进行合并
    formatted_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(formatted_datetime)

    now_time = time.time()
    a = False
    while True:

        run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
        start_time = run_time - datetime.timedelta(hours=5)

        # 如果对面是首次运行会先删除数据表，这是i还要先进行判断
        # 还有一种情况，数据表先创建了但是数据还没 存进去，数据库是空的

        if(table_name not in sql.selet_from_table(db_name)):
            print("数据表 还未创建,休眠5秒")
            time.sleep(5)
            continue

        df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
                             text="select*from {} where candle_begin_time >= '{}'".format(table_name, start_time),
                             act_name='获取币种现货K线数据', retry_times=retry_times)

        df = pd.DataFrame(df)
        # 去除重复数据
        df = df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')

        if len(df) == 0:
            print("数据还未创建,休眠5秒")
            time.sleep(5)
            continue
        #print(df[df['symbol'] == "BTCUSDT"]['candle_begin_time'].iloc[-1])
        #print(run_time-datetime.timedelta(hours=1))

        # 对数据的最后一行时间和上一个小时的时间进行对比，直到对上
        if df[df['symbol'] == "BTCUSDT"]['candle_begin_time'].iloc[-1] == run_time-datetime.timedelta(hours=1):
            print("获取历史数据")
            break
        else:
            print("历史数据还未更新休眠5秒，再次获取")
            time.sleep(5)

    df_1 = pd.read_pickle(target_folder_path+'\data.pkl')
    df = pd.concat([df, df_1], ignore_index=True)
    # 根据"symbol"和"candle_begin_time"两列进行去重，并保留最后一次出现的重复项
    df = df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')
    # 进行排序
    df = df.sort_values(['candle_begin_time', 'symbol'])
    df = df.reset_index(drop=True)


    print(df[df['symbol'] == "BTCUSDT"].tail(3).to_markdown())

    file_name_list = []
    for n in range(sum):
        # 生成最新的数据文件
        n = n+1
        file_name = str(run_time).replace(':', '-')+" 1h {}.pkl".format(n)
        df_1 = df.copy()
        df_1.to_pickle(target_folder_path+ '\\'+file_name)

        time.sleep(1) # pkl出错，每生成一个之间休眠3秒，一直调用io满了
        file_name_list.append(file_name)

    # print(datetime.datetime.now()) #跟中性实盘对比，看下什么原因导致的选币过快

    # 使用glob匹配当前文件夹下的所有.pkl文件
    pkl_files = glob.glob(target_folder_path+'/*.pkl')

    # 打印所有.pkl文件的路径,同时不等于三个路径的都删除
    for file_path in pkl_files:
        if (os.path.basename(file_path) not in file_name_list) :
                os.remove(file_path)
                # print(f"文件 '{file_path}' 已成功删除。")

    cal_time(now_time)


if __name__ == '__main__':

    print("【 运行时间 : {} 】".format(datetime.datetime.now()))
    sql = Mysql(db_addr , user_name,  user_password)



    if debug == False:


        scheduler = BackgroundScheduler(timezone=pytz.utc)
        # 新增容错时间 misfire_grace_time=120,20230709,之前延迟了一秒就报错了
        scheduler.add_job(main, 'cron', minute=run_minute, args=[sql], misfire_grace_time=120,  max_instances=3)
        scheduler.start()

        # 这里跟前面没有关系,只是为了保持主线程不停止
        while(True):
            time.sleep(60*30)

    else:
        main(sql)