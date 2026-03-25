from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
from basic_data.func import *
from manager.mysql_func import *
from config.config import *
import pandas as pd
import numpy as np
import requests
import json

table_name = "coin_coinmarketcap" #数据库
db_name = "other_data" # 数据表
_days = 8   # 获取最近N天的数据
proxies = {}


def req_data(f, n=10):
    for t in range(n):
        try:
            time.sleep(np.random.rand() * 2)
            data = f()
            return data
        except Exception as e:
            print('%s\nRetry...' % e)
            if t == n - 1:
                raise e


def req_data_daily_5000(dtf, start, url='https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/historical'):
    # dtf: datetime format time
    # start: cmc rank, initial int is 1

    return req_data(
        lambda: requests.get(
            url, params={
                'convert': 'USD,BTC',
                'date': dtf.strftime('%Y-%m-%d'),
                'limit': 200,
                'start': start,
            }, proxies=proxies
        ).content
    )


def wash_result_data(df):
    df = df.sort_values(by=['id', 'candle_begin_time']).rename(columns={'usd_volume_24h': 'usd_volume'})

    # 数据清洗
    # 原始数据仅做了去重处理，未进行其它清洗
    # 无效数据赋值为空
    df.loc[:, 'num_market_pairs'] = df['num_market_pairs'].fillna(0).replace(0, np.nan).astype('float64')
    df.loc[:, 'max_supply'] = df['max_supply'].fillna(0).replace(0, np.nan).astype('float64')
    df.loc[df['max_supply'] < 0, 'max_supply'] = np.nan
    df.loc[:, 'circulating_supply'] = df['circulating_supply'].fillna(0).replace(0, np.nan).astype('float64')
    df.loc[df['circulating_supply'] < 0, 'circulating_supply'] = np.nan
    df.loc[:, 'total_supply'] = df['total_supply'].fillna(0).replace(0, np.nan).astype('float64')
    df.loc[df['total_supply'] < 0, 'total_supply'] = np.nan
    df.loc[df['usd_price'] <= 0, 'usd_price'] = np.nan
    df.loc[df['usd_volume'] <= 0, 'usd_volume'] = np.nan

    # 部分币种成交极不活跃，或者市值排名靠后，其价格波动过大，或可能是错误录入的数据，最终回测时可以筛除，例如 TNT
    # 部分币种拆分数据，例如 Polkadot，原始数据赋予了不同的id，无需处理

    # 排除稳定币、Wrapped Tokens
    # 其它锚定性质币种如杠杆代币无标签，暂不排除，但其市值排名较靠后，可以在回测数据中筛除
    stable_coin_ids = df.loc[(df['tags'].transform(lambda x: len([_ for _ in x if 'stablecoin' in _]) > 0)) & (
            (df['usd_price'] - 1).abs() < 0.01), 'id'].unique()
    wrapped_tokens_ids = df.loc[df['tags'].transform(lambda x: 'wrapped-tokens' in x), 'id'].unique()
    df = df.loc[~(df['id'].isin(stable_coin_ids) | df['id'].isin(wrapped_tokens_ids))].reset_index(drop=True)

    # 基础因子计算
    # 同期BTC价格
    df.loc[df['id'] == 1, 'BTC_usd_price'] = df['usd_price']
    df.loc[:, 'BTC_usd_price'] = df.groupby('candle_begin_time')['BTC_usd_price'].transform(lambda x: x.dropna().iat[0])

    # 满市值，流通市值，总市值
    df['max_mcap'] = df['max_supply'] * df['usd_price']
    df['circulating_mcap'] = df['circulating_supply'] * df['usd_price']
    df['total_mcap'] = df['total_supply'] * df['usd_price']

    # 换手率
    df['turnover_rate'] = df['usd_volume'] / df['circulating_mcap']

    # 上币后时间（按秒计算）
    df['added_timedelta'] = (pd.to_datetime(
        df['candle_begin_time'] + pd.to_timedelta('1D'), utc=True
    ) - df['date_added']).dt.total_seconds()

    # 价格日涨跌
    df['usd_price_pct'] = df['usd_price'] / df['usd_price'].shift()
    df.loc[df['id'] != df['id'].shift(), 'usd_price_pct'] = np.nan

    # 流通市值日涨跌
    df['circulating_mcap_pct'] = df['circulating_mcap'] / df['circulating_mcap'].shift()
    df.loc[df['id'] != df['id'].shift(), 'circulating_mcap_pct'] = np.nan

    # 币种拆分、缩量处理
    # 同一id，存在拆分、缩量的币种，其流通市值变化比例会严重偏离价格变化比例，例如 COCOS 缩量；另有拆分前后分别记录为不同id的数据，例如 DOT 拆分
    # 定义：max(circulating_mcap_pct, usd_price_pct) / min(circulating_mcap_pct, usd_price_pct) > 2
    # 此时需将价格涨跌替换为流通市值涨跌
    df.loc[
        ((df['usd_price_pct'] > 2) | (df['usd_price_pct'] < 0.5)) &
        (df[['circulating_mcap_pct', 'usd_price_pct']].max(axis=1) / df[['circulating_mcap_pct', 'usd_price_pct']].min(
            axis=1) > 2),
        'usd_price_pct',
    ] = df[['circulating_mcap_pct', 'usd_price_pct']].min(axis=1)

    # 价格次日涨跌
    df['usd_price_pct_next'] = df['usd_price_pct'].shift(-1)
    df.loc[df['id'] != df['id'].shift(-1), 'usd_price_pct_next'] = np.nan
    df.drop(columns=['circulating_mcap_pct'], inplace=True)

    return df


def cmc_base_data(params, kwargs):
    days = params
    dfs = []
    today = pd.to_datetime((datetime.datetime.today() - pd.to_timedelta('8H')).date())
    end_date = today - pd.to_timedelta('1D')
    # end_date = today
    start_date = today - pd.to_timedelta('%sD' % days)

    while True:
        start = 1
        if start_date > end_date:
            break
        while True:
            print(start_date)
            raw_content = json.loads(req_data_daily_5000(start_date, start=start))
            em = raw_content['status']['error_message']
            if em:
                if 'Search query is out of range' in em:
                    print('skip date: %s' % start_date.strftime('%Y-%m-%d'), em)
                    df = pd.DataFrame()

            try:
                df = pd.DataFrame(raw_content['data'])
                df['candle_begin_time'] = start_date
            except KeyError as e:
                print('error date: %s' % start_date.strftime('%Y-%m-%d'), em)
                # raise

            dfs.append(df)
            start += df.shape[0]
            if df.shape[0] < 5000:
                break
            if start > 5000:
                break
        time.sleep(2)
        start_date += pd.to_timedelta('1D')

    all_df = pd.concat(dfs, sort=False, axis=0).reset_index(drop=True)
    if not all_df.empty:
        all_df.loc[:, 'date_added'] = pd.to_datetime(all_df['date_added'])
        all_df['usd_price'] = all_df['quote'].apply(lambda x: x['USD']['price'])
        all_df['usd_volume_24h'] = all_df['quote'].apply(lambda x: x['USD']['volume_24h'])
        all_df.drop(columns=['platform', 'last_updated', 'slug', 'quote'], inplace=True)
        all_df['symbol'] = all_df['symbol'].apply(lambda x: x + 'USDT')
    all_df = wash_result_data(all_df)
    return all_df

def main():

    print()
    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快
    df = cmc_base_data(_days,_days)
    df['tags'] = df['tags'].astype('str')
    df = df.sort_values(by='candle_begin_time')

    sql = Mysql(db_addr, user_name, user_password)
    mysql_df = sql.selet_from_table(db_name, table_name, "SELECT * FROM {} WHERE {} = (SELECT MAX({}) FROM {});"
                                     .format(table_name,'candle_begin_time','candle_begin_time',table_name))
    mysql_df  = pd.DataFrame(mysql_df)

    last_time = mysql_df .iloc[-1]['candle_begin_time']
    print("数据库里的最后时间 {}".format(last_time))

    df = df[df['candle_begin_time'] > last_time]

    # 使用df_a的列数据类型修改df_b的列数据类型
    for column_name, data_type in mysql_df.dtypes.iteritems():
        df[column_name] = df[column_name].astype(data_type)

    if len(df)!=0:
        print(df.tail(5).to_markdown())
    else:
        print("没有新数据")
    sql.create_talbe(df, db_name, table_name, if_exists="append")



if __name__ == '__main__':

    print("运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快
    # 创建定时器以定时执行任务，使用格林威治标准时间（UTC）
    scheduler = BackgroundScheduler(timezone=pytz.utc)

    # 设置定时任务，在每小时的30分执行一次
    scheduler.add_job( main, 'cron', minute='0',misfire_grace_time=60)

    # 开始定时器
    scheduler.start()
    # 测试用
    # main()
    # 这里跟前面没有关系,只是为了保持主线程不停止


    while(True):
        time.sleep(60*30)




