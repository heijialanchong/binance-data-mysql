from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
from basic_data.func import *
from manager.mysql_func import *
from config.config import *


folder_path = r'C:\Users\Administrator\Desktop\数据\coin-chain-basic-data'
# 读取已经存储的货币的列表
table_name = "eth"
db_name = "coin_chain_basic_data"
sql = Mysql(db_addr, user_name, user_password)


def main():

    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快
    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)


    last_time = run_time - datetime.timedelta(hours=24*10)

    symbol_df = sql.selet_from_table(db_name, table_name, "select*from {} where hour >= '{}'"
                                           .format(table_name,last_time))

    symbol_df = pd.DataFrame(symbol_df)

    """
    symbol_df = symbol_df[symbol_df['symbol'] == "UNFI-USDT"]
    print(symbol_df.to_markdown())
    exit()
    """

    # 替换 'folder_path' 为你想要查找CSV文件的文件夹路径
    csv_file_paths = get_csv_file_paths(folder_path)

    df_list = []
    for path in csv_file_paths:
        # print(path)

        # 使用os.path.basename获取文件名部分
        file_name = os.path.basename(path)

        # 提取1000FLOKI-USDT
        symbol = str(file_name.split('.')[0]).replace("-","")  # 去掉文件后缀部分


        try:
            _df = symbol_df.copy() # 这个是原数据库的内容
            _df = _df[_df['symbol'] == symbol] # 对比文件夹，因为有新币的情况，这时候，_df长度为0,说明这个是新币没存在数据库里
        except:
            print("报错")
            print("-{}-".format(symbol))


        df = pd.read_csv(path, parse_dates=['hour'], skiprows=[0], encoding="gbk")
        df['symbol'] = df['symbol'].str.replace('-', '')
        df['hour'] = df['hour'] + datetime.timedelta(hours=timezone_offset)
        df['gas_fee_total'] = df['gas_fee_total'].astype(float)


        # 不是新币有最后的时间,筛选出最后一个数据后的时间，否则就是新币，不用筛选了，直接全部存入
        if len(_df)!=0 :

            last_hour_value = _df["hour"].tail(1).iloc[0]
            # print(last_hour_value)
            df = df[ df['hour']>last_hour_value ]

        else:
            df = df[ df['hour'] > last_time ]
            if len(df)!=0:
                print("新币 {}".format(path))
            # 如果读取24小时内没数据，有两种情况一种是已经下架了很久了，一种是新币，
            # 在对比最近的24小时为开始时间，如果有数据存入

        # print(df.tail(10).to_markdown())

        if len(df)!=0 :
            if len(df.columns.tolist()) >= 21:
                df_list.append(df)
            else:
                print(path + "是稳定币跳过")
            # sql.create_talbe(df, db_name, table_name, if_exists="append")


    if len(df_list)!=0:
        all_data = pd.concat(df_list, ignore_index=True)
        all_data.sort_values(by=['symbol', 'hour'], inplace=True)
        print(all_data.tail(2).to_markdown())
        if len(all_data)!=0:
            try:
                sql.create_talbe(all_data, db_name,table_name, if_exists="append")
                print("写入mysql成功")
            except:
                print("写入错误")
    else:
        print("没有数据合并")

if __name__ == '__main__':

    # 创建定时器以定时执行任务，使用格林威治标准时间（UTC）
    scheduler = BackgroundScheduler(timezone=pytz.utc)

    # 设置定时任务，在每小时的30分执行一次
    scheduler.add_job( main, 'cron', minute='50',misfire_grace_time=60)

    # 开始定时器
    scheduler.start()

    # 测试用
    # main()
    # 这里跟前面没有关系,只是为了保持主线程不停止

    while(True):
        time.sleep(60*30)