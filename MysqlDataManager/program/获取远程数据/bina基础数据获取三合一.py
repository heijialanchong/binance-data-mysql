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

# ==========  mysql参数设置

db_name = "bina" # 数据库
funding_rate_table_name = "funding_rate" # funding_rate读取资金费率，不填这个数据就不启用
spot_table_name = 'b_spot_1h' # b_spot_1h读取现货数据，不填这个数据就不启用
swap_table_name = 'b_swap_1h' # b_swap_1h读取合约数据，不填这个数据就不启用
db_addr = "127.0.0.1" # 远程ip 43.155.11.151
user_name = "root"# "linxuan" # 访问账户
user_password = "123456" # 访问密码


# ==========  基础参数设置
debug = False # 调试
sum = 2  # 数据生成份数
get_hour = 1499 # 获取数据，当前时间往前推N个小时开获取

"""

    运行时间，比如每小时50开始执行,提前获取历史数据，然后整点在获取最新更新的K线数据，这样节省大量的时间
    另外因为资金费率的数据在整点一开始的时候就能获取，比如11点就能获取到11点的资金费率数据，这样不用等到12点
    获取,为跟其他数据对齐，11点的数据，生成的文件名也是12点的，跟K线的数据文件名称对齐，方便其他程序调用
    
"""
run_minute = 40 # 运行时间
retry_times = 10 # 读取mysql数据容错次数

"""

    分段读取数据时间，因为太多的数据查询时间过长,容易导致卡住导致报错,并且报错重新读取之前查询浪费了过多时间，
    分段读取，即使某段数据读取失败重新读取浪费的时间也有限
    
"""
read_sleep_time = 1 # 每次读取后休眠时间，太短也会导致频繁读取数据库报错，重新查询浪费了更多时间

funding_rate_read_days = 14 # 分段读取数据时间，天数.不同的数据，一天内数据量不同
spot_read_days = 7 # 分段读取数据时间，天数.不同的数据，一天内数据量不同
swap_read_days = 7 # 分段读取数据时间，天数.不同的数据，一天内数据量不同

funding_rate_query_timeout = 30000 # 查询超时毫秒，跟上面的参数配合使用，以免查询卡住太长时间,根据不同数据的数据量设置
spot_query_timeout = 120000 # 查询超时毫秒，跟上面的参数配合使用，以免查询卡住太长时间,根据不同数据的数据量设置
swap_query_timeout = 180000 # 查询超时毫秒，跟上面的参数配合使用，以免查询卡住太长时间,根据不同数据的数据量设置


# 计算程序总的运行时间
def cal_time( now_time ):
    cal_time = time.time() - now_time
    m = int(cal_time / 60)
    s = int(cal_time % 60)
    print("【 花费时间 : {}m {}s at {}】".format(m, s,datetime.datetime.now()) + "\n")



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


def funding_rate(sql,table_name):

    print("【 {} 获取数据 at {} 】".format(table_name,datetime.datetime.now()))

    now_time = time.time()
    # 9月24号程序出错了,导致都是空的,获取不到最新的时间,一直卡住,不是我的问题,重新观察
    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
    start_time = run_time - datetime.timedelta(hours=get_hour)

    # df = sql.selet_from_table(db_name, table_name, "select*from {} where hour >= '{}'".format(table_name, start_time))
    # df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
    #                     text="select*from {} where fundingTime >= '{}'".format(table_name, start_time),
    #                     act_name='获取币安资金费率', retry_times= retry_times,sleep_seconds=5)

    # 分段查询数据，避免每次查询过长导致短开
    all_data = pd.DataFrame()
    delta = datetime.timedelta(days=funding_rate_read_days)

    end_time = datetime.datetime.now()
    while start_time < end_time:
        segment_end_time = min(start_time + delta, end_time)
        #print()
        #print(start_time )
        #print(datetime.datetime.now())
        #print()

        query_text = f"SELECT * FROM {table_name} WHERE fundingTime >= '{start_time}' AND fundingTime < '{segment_end_time}'"
        df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,query_timeout = funding_rate_query_timeout,
                             text=query_text, act_name='获取 {} 数据 {} - {}'.format(table_name,start_time,segment_end_time), retry_times=retry_times, sleep_seconds=5)

        segment_data = pd.DataFrame(df)
        all_data = pd.concat([all_data, segment_data], ignore_index=True)
        start_time = segment_end_time
        time.sleep(read_sleep_time)

    df = pd.DataFrame(all_data)
    # 去除重复数据
    df = df.drop_duplicates(subset=['symbol', 'fundingTime'], keep='last')
    df = df.sort_values(['fundingTime', 'symbol'])
    df = df.reset_index(drop=True)

    print(df.head(2).to_markdown())
    print(df.tail(2).to_markdown())

    cal_time(now_time)

    return df

# 合约现货首次提前获取历史数据
def swap_spot_first(sql,table_name,read_days,query_timeout):


    # 1.先获取全量数据
    # sleep_1() # 休眠到整点提前十分钟，先获取一次全量数据
    print("【 {} 获取历史数据 at {} 】".format(table_name,datetime.datetime.now()))

    now_time = time.time()
    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
    start_time = run_time - datetime.timedelta( hours = get_hour)
    # df = sql.selet_from_table(db_name , table_name, "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time))
    # df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,
    #                     text = "select*from {} where candle_begin_time >= '{}'".format(table_name, start_time),
    #                     act_name='获取币种K线数据', retry_times=retry_times)

    # 分段查询数据，避免每次查询过长导致短开

    all_data = pd.DataFrame()
    delta = datetime.timedelta(days=read_days)

    end_time = datetime.datetime.now()
    while start_time < end_time:
        segment_end_time = min(start_time + delta, end_time)
        #print()
        #print(start_time )
        #print(datetime.datetime.now())
        #print()
        query_text = f"SELECT * FROM {table_name} WHERE candle_begin_time >= '{start_time}' AND candle_begin_time < '{segment_end_time}'"
        df = retry_wrapper_1(sql.selet_from_table, db_name=db_name, db_table=table_name,query_timeout = query_timeout,
                             text=query_text, act_name='获取 {} 数据 {} - {}'.format(table_name,start_time,segment_end_time), retry_times=retry_times, sleep_seconds = 60)
        segment_data = pd.DataFrame(df)
        all_data = pd.concat([all_data, segment_data], ignore_index=True)
        start_time = segment_end_time
        time.sleep(read_sleep_time)

    df = pd.DataFrame(all_data)
    # 去除重复数据
    df = df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')
    df = df.sort_values(['candle_begin_time', 'symbol'])

    path = target_folder_path.replace(target_folder,table_name)
    if not os.path.exists(path):
        os.makedirs(path)
    df.to_pickle(path+ '\data.pkl')
    cal_time(now_time)


# 合约和现货整点补齐最新数据
def swap_spot_second(sql,table_name):

    print("【 {} 获取最新数据 at {} 】".format(table_name,datetime.datetime.now()))
    now_time = time.time()
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
            print("获取最新数据")
            break
        else:
            print("最新数据还未更新休眠5秒，再次获取")
            time.sleep(5)

    path = target_folder_path.replace(target_folder,table_name)
    df_1 = pd.read_pickle(path+'\data.pkl')
    df = pd.concat([df, df_1], ignore_index=True)
    # 根据"symbol"和"candle_begin_time"两列进行去重，并保留最后一次出现的重复项
    df = df.drop_duplicates(subset=['symbol', 'candle_begin_time'], keep='last')
    # 进行排序
    df = df.sort_values(['candle_begin_time', 'symbol'])
    df = df.reset_index(drop=True)
    print(df[df['symbol'] == "BTCUSDT"].head(2).to_markdown())
    print(df[df['symbol'] == "BTCUSDT"].tail(2).to_markdown())

    cal_time(now_time)

    return df

def pkl_data(df,table_name):

    path = target_folder_path.replace(target_folder,table_name)
    if not os.path.exists(path):
        os.makedirs(path)

    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级


    if table_name == funding_rate_table_name:

        """
        如果是资金费率的数据，文件名直接显示下一个小时的，因为12点使用11点的数据，在11点开头就已经产生了
        而12点大数据在12点是用不上的,所以可提早获取，名称跟其他数据统一起来，方便其他程序段i奥用     
        """
        run_time = run_time + datetime.timedelta(hours=1)

    file_name_list = []
    for n in range(sum):
        # 生成最新的数据文件
        n = n+1
        file_name = str(run_time).replace(':', '-')+" 1h {}.pkl".format(n)
        df_1 = df.copy()
        df_1.to_pickle(path + '\\'+file_name)

        time.sleep(1) # pkl出错，每生成一个之间休眠3秒，一直调用io满了
        file_name_list.append(file_name)

    # print(datetime.datetime.now()) #跟中性实盘对比，看下什么原因导致的选币过快

    # 使用glob匹配当前文件夹下的所有.pkl文件
    pkl_files = glob.glob(path+'/*.pkl')

    # 打印所有.pkl文件的路径,同时不等于三个路径的都删除
    for file_path in pkl_files:
        if (os.path.basename(file_path) not in file_name_list) :
                os.remove(file_path)
                # print(f"文件 '{file_path}' 已成功删除。")


def main(sql):


    print('\n'+'='*100 + '\n')

    if funding_rate_table_name!="":
        df = funding_rate(sql, funding_rate_table_name)
        pkl_data(df,funding_rate_table_name)

    if swap_table_name!= '':
        swap_spot_first(sql, swap_table_name,swap_read_days,swap_query_timeout)
    if spot_table_name != '':
        swap_spot_first(sql, spot_table_name,spot_read_days,spot_query_timeout)

    if debug == False:
        sleep_2()

    if swap_table_name!= '':
        df = swap_spot_second(sql, swap_table_name)
        pkl_data(df,swap_table_name)

    if spot_table_name != '':
        df = swap_spot_second(sql, spot_table_name)
        pkl_data(df, spot_table_name)




if __name__ == '__main__':

    print("【 程序运行 : {} 】".format(datetime.datetime.now()))
    sql = Mysql(db_addr, user_name, user_password)


    if debug == False:
        scheduler = BackgroundScheduler(timezone=pytz.utc)
        # 新增容错时间 misfire_grace_time=120,20230709,之前延迟了一秒就报错了
        scheduler.add_job(main, 'cron', minute=run_minute, args=[sql], misfire_grace_time=120, max_instances = 5)  # 增加最大实例数)
        scheduler.start()
    else:
        main(sql)

    if debug == False:
        # 这里跟前面没有关系,只是为了保持主线程不停止
        while (True):
            time.sleep(60 * 30)