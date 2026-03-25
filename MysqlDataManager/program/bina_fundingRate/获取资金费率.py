from manager.exchange import *
from manager.functions import *
from manager.utility import *
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import pytz
from manager.mysql_func import *
from config.config import *
from bina_fundingRate.func import *

"""
    东八区要加8小时才能跟盘面对上，实际使用时候看情况要不要减回去
"""

table_name = "funding_rate" # 表
db_name = "bina" # 数据库
sql = Mysql(db_addr, user_name, user_password)

def get_data(symbol_list,limit):

    df_list = []
    for symbol in symbol_list:

        df = retry_wrapper(binance_exchange.fapiPublicGetFundingRate, act_name='获取币安历史资金费率',
                           params={'symbol': symbol, 'limit': limit})

        df = pd.DataFrame(df)


        if 'fundingTime' in df.columns:
            df['fundingTime'] = pd.to_datetime(df['fundingTime'].astype(float) // 1000 * 1000,
                                               unit='ms')  # 时间戳内容含有一些纳秒数据需要处理
            #df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
            df['fundingTime'] = df['fundingTime']  +  datetime.timedelta(hours=timezone_offset)
            df['fundingTime'] = pd.to_datetime(df['fundingTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df['fundingTime'] = pd.to_datetime(df['fundingTime'], format='%Y-%m-%d %H:%M:%S')
            df_list.append(df)
        else:
            print(" {} 品种数据列没有 fundingTime 列,跳过此次抓取，请查找原因 ( 合约可能是新品种还没上线 , 也有可能合约已经下架币 )".format(symbol) )


    df = pd.concat(df_list, ignore_index=True)

    df = df.sort_values(by='fundingTime')
    df = df.reset_index(drop=True)


    return df


def first_run():

    now_time = time.time()

    try:
        sql.drop_talbe(db_name, [table_name])
    except:
        print("表可能已经删除")

    exchange_info = robust(binance_exchange.fapiPublicGetExchangeInfo, )

    # 首次运行，下载全量数据包含未交易状态的币
    _symbol_list = [x['symbol'] for x in exchange_info['symbols']]  # if x['status'] == 'TRADING' 过滤出交易状态正常的币种
    _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # | symbol.endswith('BUSD'))]  # 过滤usdt合约
    symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单


    print("获取币种 {} ".format(len(symbol_list)))
    df = get_data(symbol_list,1000)

    print("起始数据 :")
    print(df[df['symbol'] == "BTCUSDT"].head(2).to_markdown())
    print("结束数据 :")
    print(df[df['symbol'] == "BTCUSDT"].tail(2).to_markdown())

    creat_data_table(sql,db_name ,df,table_name)

    time_elapsed = time.time() - now_time
    minutes = int(time_elapsed // 60)
    seconds = time_elapsed % 60

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("数据获取完毕 {}m {:.2f}s at {}\n".format(minutes, seconds, current_time))

def second_run():

    now_time = time.time()
    print()
    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快

    exchange_info = robust(binance_exchange.fapiPublicGetExchangeInfo, )

    # 第二次运行，只下载正处在交易状态的最近费率
    _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if x['status'] == 'TRADING']  #  过滤出交易状态正常的币种
    _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # | symbol.endswith('BUSD'))]  # 过滤usdt合约
    symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单
    print("获取可交易币种 {} ".format(len(symbol_list)))
    symbol_candle_data = get_data(symbol_list,10) #

    run_time = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
    last_time =  run_time - datetime.timedelta(hours= 48)
    mysql_data = sql.selet_from_table(db_name,table_name, "select*from {} where fundingTime >= '{}'"
                                        .format(table_name, last_time))

    mysql_data = pd.DataFrame(mysql_data)


    df_list = []
    for symbol in symbol_list:

        # 获取读取数据的最后一个时间节点
        _data_df = mysql_data[mysql_data['symbol'] == symbol]  # 这里数据库读取的，最后一条数据是还没收盘的
        # 这里有可能是空数据
        if len(_data_df) != 0:
            _data_df.sort_values(by=['fundingTime'], inplace=True)

            _last_time = _data_df['fundingTime'].tolist()[-1] + datetime.timedelta(seconds=1)

            # 增量数据,跟历史数据对比提取多出的部分
            symbol_df = symbol_candle_data[symbol_candle_data['symbol'] == symbol]
            # 发现数据有重复,last_time + 1秒
            symbol_df = symbol_df[symbol_df['fundingTime'] > _last_time]
            df_list.append(symbol_df)
        else: # 为空说明是新币,直接全部往里筛
            print("新币 {}".format(symbol))
            symbol_df = symbol_candle_data[symbol_candle_data['symbol'] == symbol]
            df_list.append(symbol_df)




        # ========存入新增数据
    df = pd.concat(df_list, ignore_index=True)
    df.drop_duplicates(subset=['fundingTime', 'symbol'], keep='last', inplace=True)  # 去重
    df.reset_index(drop=True, inplace=True)

    if len(df)!=0:
        # print(df[df['symbol'] == "BTCUSDT"].tail(3).to_markdown())
        print(df.head(5).to_markdown())
        print(df.tail(5).to_markdown())
    else:
        print("没有新数据")

    try:

        sql.create_talbe(df,db_name,table_name, if_exists="append")
    except:
        pass

    time_elapsed = time.time() - now_time
    minutes = int(time_elapsed // 60)
    seconds = time_elapsed % 60

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("数据获取完毕 {}m {:.2f}s at {}\n".format(minutes, seconds, current_time))

"""
def last_funding_rate():

    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快

    exchange_info = robust(binance_exchange.fapiPublic_get_exchangeinfo, )

    # 第二次运行，只下载正处在交易状态的最近费率
    _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if x['status'] == 'TRADING']  #  过滤出交易状态正常的币种
    _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # | symbol.endswith('BUSD'))]  # 过滤usdt合约
    symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单
    print("获取可交易币种 {} ".format(len(symbol_list)))
    df_list = []
    for symbol in symbol_list:
        df = retry_wrapper(binance_exchange.fapiPublicGetPremiumIndex, act_name='获取币安当前资金费率',
                           params={'symbol': symbol})

        df_list.append(df)


    df = pd.DataFrame(df_list)
    df = df.reset_index(drop=True)
    df['nextFundingTime'] = pd.to_datetime(df['nextFundingTime'], unit='ms')
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    print(df.to_markdown())
"""

if __name__ == '__main__':


    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快
    first_run() # 首次运行创建表，导入数据

    # 创建定时器以定时执行任务，使用格林威治标准时间（UTC）
    scheduler = BackgroundScheduler(timezone=pytz.utc)

    # 设置定时任务，在每小时的30分执行一次
    # scheduler.add_job( second_run, 'cron', hour='0,8,16',minute='0',misfire_grace_time=60)
    scheduler.add_job(second_run, 'cron', minute='5', misfire_grace_time=60)

    # 开始定时器
    scheduler.start()
    # 测试用
    # main()
    # 这里跟前面没有关系,只是为了保持主线程不停止

    while(True):
        time.sleep(60*30)