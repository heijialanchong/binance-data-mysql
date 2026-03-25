# -*- coding: utf-8 -*-
import gevent
import pandas as pd
import re
import time
from datetime import  timedelta
import datetime
import pytz
from icecream import ic
from config.config import *

def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 6000)  # 最多显示数据的行数

def get_beijing_time():
    # 这是本地时间等于是调度时候的时间，不用重复获取的，放这里试试,放这里应该跟更合理
    # 获取当前UTC时间
    utc_now = datetime.datetime.utcnow()
    # 将UTC时间转换为北京时间
    beijing_tz = pytz.timezone('Asia/Shanghai')  # 东八区的时区为Asia/Shanghai
    beijing_time = utc_now.replace(tzinfo=pytz.utc).astimezone(beijing_tz)

    return beijing_time

# ===重试机制
def retry_wrapper(func, params={}, act_name='', sleep_seconds=15, retry_times=100):
    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            ic(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
            if "418" in str(e):
                # 被ban了，暂停到被allow的时间
                
                ban_time = re.findall(r'\d{13}', str(e))
                time_now = int(time.time())
                if ban_time:
                    ban_time = int(int(ban_time[0]) / 1000)
                    if ban_time > time_now:
                        sleep_time = ban_time - time_now
                        if sleep_time < sleep_seconds:
                            sleep_time = sleep_seconds
                        ic(act_name, '被ban了，暂停到被allow的时间：', sleep_time)
                        time.sleep(sleep_time)
                        # ic('睡眠了' + str(sleep_time) + '秒')
    else:
        # send_dingding_and_raise_error(output_info)
        raise ValueError(act_name, '报错重试次数超过上限，程序退出。')


def retry_wrapper_1(func, act_name='', sleep_seconds=15, retry_times=5, bina_mode=False, read_data=False, *args,
                  **kwargs):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。
    :param func: 需要重试的函数名
    :param params: func的参数
    :param act_name: 本次动作的名称
    :param sleep_seconds: 报错后的sleep时间
    :param retry_times: 为最大的出错重试次数
    :param bina_mode: 是否与币安平台进行数据交互
    :return:
    """

    for _ in range(retry_times):
        try:
            result = func(*args, **kwargs)

            if (bina_mode == True) and ("code" in result):  # 用于币安交互数据被限制时的判断
                print(act_name, ' ' + str(result['code']) + " " + str(result['msg']))
                time.sleep(sleep_seconds)
            elif (read_data) == True and (result is None):  # 读取数据如果为空循环
                print(act_name, ' 读取数据为空')
                time.sleep(sleep_seconds)
            else:
                return result
        except Exception as e:
            #str(result['code'])
            print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
    else:
        # send_dingding_and_raise_error(output_info)
        raise ValueError(act_name, '报错重试次数超过上限，程序退出。')

def get_history_data_more_than_1500(exchange, symbol, time_interval, run_time, candle_num,method):


    count = 0
    while True: # 这个循环是为了验证是否收盘

        # ic('获取交易币种的全量历史K线数据',symbol)
        # 将结束时间改为UTC时间
        end_time_real = pd.to_datetime(run_time) - pd.Timedelta(hours=timezone_offset)
        # 用结束时间减k线时间计算开始时间
        if time_interval.find('m') >= 0:
            start_time_real = end_time_real - timedelta(minutes=int(time_interval.split('m')[0]) * candle_num)
            min_timedelta = timedelta(minutes=int(time_interval.split('m')[0]))  # 最小时间偏差
        elif (time_interval.find('h') >= 0):
            start_time_real = end_time_real - timedelta(hours=int(time_interval.split('h')[0]) * candle_num)
            min_timedelta = timedelta(hours=int(time_interval.split('h')[0]))
        elif time_interval.find('d') >= 0:  # <-- 支持日线数据获取
            start_time_real = end_time_real - timedelta(days=int(time_interval.split('d')[0]) * candle_num)
            min_timedelta = timedelta(days=int(time_interval.split('d')[0]))
        else:  # 注意暂时未判断按天的策略
            ic(time_interval, '{} 时间间隔格式错误，请修改'.format(method))
            raise ValueError

        # 将时间parse，由于api参数规定，必须用parse的时间，该行代码用于测试，本代码以startTime作为
        # 每次修改的参数，如果读者想用endTime进行开发则需要parse endTime。
        end_time_real_parse = exchange.parse8601(str(end_time_real))


        count = count + 1

        df_all = []
        while pd.to_datetime(start_time_real) < pd.to_datetime(end_time_real): # -min_timedelta
            # parse startTime,这里估计是不管哪个时区exchange.parse8601解析成了相应失去的时间
            start_time_real_parse = exchange.parse8601(str(start_time_real))

            if method == "swap":
                kline = retry_wrapper(exchange.fapiPublicGetKlines, act_name=symbol +' swap获取交易币种的全量历史K线数据',
                                      params={'symbol': symbol, 'interval': time_interval, 'startTime': start_time_real_parse,
                                              'limit': 1500})
            elif method == "spot":
                kline = retry_wrapper(exchange.publicGetKlines, act_name=symbol + ' spot获取交易币种的全量历史K线数据',
                                      params={'symbol': symbol, 'interval': time_interval,
                                              'startTime': start_time_real_parse,
                                              'limit': 1500})


            columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                       'trade_num',
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

            df = pd.DataFrame(kline, columns=columns, dtype='float')
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=timezone_offset)  # 时间转化为服务器所在区域
            df = df[columns]

            if len(df) == 0:
                #start_time_real = start_time_real + timedelta(hours=time_rule * candle_num)
                # 这里没数据了不应该跳过，数据是从早往后抓的，没数据了不应该跳过循环，不然还会重复抓,应该退出循环了
                break

            # +1min 是为了从下一根k线开始
            start_time_real = df.iloc[-1]['candle_begin_time'] - pd.Timedelta(hours=timezone_offset) + pd.Timedelta(minutes=1)
            df_all.append(df)

        if len(df_all)==0:
            # 创建一个空的DataFrame
            # print("{} {} 数据为空".format(method,symbol))
            df_all = pd.DataFrame(columns=columns)
            return df_all

        df_all = pd.concat(df_all)
        df_all.sort_values(by='candle_begin_time', inplace=True)
        df_all.drop_duplicates(subset=['candle_begin_time'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)

        t_df = df_all[df_all['candle_begin_time'] == run_time]

        if t_df.empty:

            # print(method , symbol, " 重新获取 {}".format(count))

            if count>=15:
                print(symbol + '-' + time_interval + '在时间' + str(run_time) +
                      " {} 全量 没获取到最新K线 {}次,可能 1.新币 2.暂停交易 3.K线一直未收盘,跳过本次数据抓取".format(method,count))

            else:
                # print(symbol + '-' + time_interval + '在时间' + str(run_time) + "全量 没获取到最新K线 {}次".format(count))
                time.sleep(1)
                continue

        # 删除runtime那行的数据，如果有的话
        df_all = df_all[df_all['candle_begin_time'] != run_time]
        # 这个函数是获取历史数据的，去掉最后一行最新的数据。如果将来需要改为实时获取，修改删除这一行
        # df_all = df_all[:-1]

        return df_all


def ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, run_time, limit=1000 , method ='swap'):
    """
    获取指定币种的K线信息
    :param exchange:
    :param symbol:
    :param time_interval:
    :param limit:
    :return:
    """


    count = 0
    while True: # 这个循环是为了验证是否收盘

        count = count + 1

        # 获取数据
        # data = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': time_interval, 'limit': limit})
        if method == "swap":
            kline = retry_wrapper(exchange.fapiPublicGetKlines, act_name=symbol +' swap获取币种K线数据',
                                  params={'symbol': symbol, 'interval': time_interval, 'limit': limit})
        elif method == "spot":
            kline = retry_wrapper(exchange.publicGetKlines, act_name=symbol +' spot获取币种K线数据',
                                  params={'symbol': symbol, 'interval': time_interval, 'limit': limit})


        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

        df = pd.DataFrame(kline, columns=columns, dtype='float')

        # 整理数据
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=timezone_offset)  # 时间转化为服务器所在区域
        df = df[columns]
        t_df = df[df['candle_begin_time'] == run_time]

        if t_df.empty:

            # print(method , symbol, " 重新获取 {}".format(count))
            if count>=15:
                print(symbol + '-' + time_interval + '在时间' + str(run_time) +
                      " {} 没获取到最新K线 {}次,可能 1.新币 2.暂停交易 3.K线一直未收盘，跳过本次数据抓取".format(method,count))

            else:
                # print(symbol + '-' + time_interval + '在时间' + str(run_time) + "没获取到最新K线 {}次".format(count))
                time.sleep(1)
                continue



        # 删除runtime那行的数据，如果有的话
        df = df[df['candle_begin_time'] != run_time]

        return df


def get_data(symbol, exchange, candle_num, time_interval, run_time , method):
    # 获取symbol该品种最新的K线数据
    # 当获取超过1500根k线
    if candle_num > 1500:
        df = get_history_data_more_than_1500(exchange, symbol, time_interval, run_time, candle_num , method)
    else:
        df = ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, run_time, candle_num , method)

    df['symbol'] = symbol
    df['symbol_type'] = method
    df = df[['candle_begin_time','open','high',	'low','close','volume','close_time','quote_volume','trade_num',
             'taker_buy_base_asset_volume','taker_buy_quote_asset_volume','ignore','symbol','symbol_type']]

    return symbol, df


# ===获取需要的币种的历史K线数据。
def get_binance_history_candle_data(exchange, symbol_list, time_interval, run_time, candle_num,method):
    ic('{} 获取交易币种的历史K线数据'.format(method))
    result = {}
    for symbol in symbol_list:


        # 直接调用 get_data 函数获取数据
        _,data = get_data(symbol, exchange, candle_num, time_interval, run_time , method)
        result[symbol] = data

    return dict(result)