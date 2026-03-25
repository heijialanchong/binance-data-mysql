# -*- coding: utf-8 -*-
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler  # 后台定时器不能用时可以用阻塞版的
from manager.functions import *
from manager.utility import *
from config.config import *
import datetime
import time
import pytz
"""
这个文档在一开始的时候获取了有500根K线，之后又获取了10K线，然后在第二次运行的时候只获取10K线了

"""
from icecream import ic
import glob

def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 6000)  # 最多显示数据的行数


class SwapDataManagerFather():
    print("swap 运行时间 : 【{}】".format(datetime.datetime.now()))  # 跟中性实盘对比，看下什么原因导致的选币过快
    def __init__(self,exchange,needed_time_interval_list,sql,db_name):
        self.exchange = exchange 
        self.time_interval_list = needed_time_interval_list
        self.son = []  # 存放所有子数据管理类
        self.scheduler = BackgroundScheduler(timezone=pytz.utc)  # 创建定时器以定时执行任务,格林格林威治时间
        self.sql = sql
        self.db_name = db_name # 数据库名称
        # 通过配置文件创建子策略实例
        for time_interval in self.time_interval_list:
            exec("self.data_%s=DataManagerSon(time_interval,self.exchange,self.sql,self.db_name)" % time_interval)  # 通过配置创建子类
            exec("self.son.append(self.data_%s)" % time_interval)  # 将子类保存在类变量son中

        #  max_instances 可同时运行的实例数量,假设1分钟运行一个，一个需要5分钟，那运行三分钟后，4，5分钟后面的都不会运行了
        #  misfire_grace_time调度器允许的延迟时间
        for son in self.son:
            if son.time_interval.find('m') >= 0:  # 添加循环间隔是分钟的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', minute='*/' + son.time_interval.split('m')[0],
                                       misfire_grace_time=60, max_instances=3, id=son.name)
            elif son.time_interval.find('h') >= 0:  # 添加循环间隔是小时的子类的定时任务

                if test == False:
                    self.scheduler.add_job(son.scheduler, trigger='cron', hour='*/' + son.time_interval.split('h')[0],
                                           misfire_grace_time=60, max_instances=3, id=son.name)
                else:
                    son.scheduler()
                    exit()

            elif son.time_interval.find('d') >= 0:  # 添加循环间隔是天的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', day='*/' + son.time_interval.split('d')[0],
                                     misfire_grace_time=60, max_instances=3, id=son.name)
                # son.scheduler()
                # exit()
            else:  # 注意暂时未判断按天的策略
                ic(son.name, '时间间隔格式错误，请修改')
                raise ValueError

        # 移除日志文件
        # self.scheduler.add_job(self.clean_outrange_data, trigger='cron', hour='12')
        self.scheduler.start()  # 定时器开始工作


    # 清理过期文件，只保留今天的数据
    def clean_outrange_data(self):
        '''
        清理过期文件
        '''
        flag_file_list = glob.glob(flag_path_root+'/*.flag')
        for file in flag_file_list:
            today = time.strftime("%Y-%m-%d", time.localtime()) 
            if today not in file:
                os.remove(file)


class DataManagerSon():


    def __init__(self, time_interval, exchange,sql,db_name):
        self.exchange = exchange  # 从母类继承交易所实例
        self.time_interval = time_interval
        self.name = 'b_swap_' + time_interval  # 子类名称，用于区分
        self.sql = sql # 数据库
        self.db_name = db_name
        self.re_download_all_his_coin_data = True # 下载全币种历史数据标志

    # 将时间戳转成字符串
    def time_to_timestamp(self,timeNum):

        timeTemp = float(timeNum / 1000)
        tupTime = time.localtime(timeTemp)
        stadardTime = time.strftime("%Y-%m-%d %H:%M:%S", tupTime)
        return pd.to_datetime(stadardTime).replace(second=0, microsecond=0)

    # 创新新的数据表
    def creat_data_table(self,df,table_name):
        # 保留还没收盘的创建一个新表，用于开单调用
        self.sql.create_talbe(df, self.db_name, table_name, if_exists="replace")
        # .先修改 symbol的长度，在设置复合主键，以免数据重复
        text = "alter table {} modify column symbol varchar(50);".format(table_name)
        self.sql.selet_from_table(self.db_name, db_table=table_name, text=text)
        text = "ALTER TABLE {} ADD CONSTRAINT PK_{} PRIMARY KEY(candle_begin_time,symbol);".format(table_name,table_name)
        self.sql.selet_from_table(self.db_name, db_table=table_name, text=text)


    def scheduler(self):  # 供定时器调用


        now_time = time.time()

        exchange_info = robust(self.exchange.fapiPublicGetExchangeInfo, )
        # _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
        #
        # if x['status'] == 'TRADING' 过滤出交易状态正常的币种
        # 这里现在有两种情况 ， 一种能抓取到币种但是不处于交易状态但是有交易接口，一种是已经没有交易接口导致报错
        # 好像直接过滤正在交易的问题也不大，后期能交易了暂停在放出来，后面的代码有补全数据 , 不然会出现一直卡这里
        # 下架的币没有历史数据可能资金曲线计算会有点问题，但是一个币种影响不大。一直运行就不会了，就一开始跑的时候可能出现
        # 刚刚下架，币种接口调用不了的情况卡住。首次加上if x['status'] == 'TRADING'过滤，数据问题差问题很小

        # 主要问题应该是下架的币接口没了无法调用到了API，所以加上if x['status'] == 'TRADING',比如spot BTCSTUSDT
        _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if x['status'] == 'TRADING']  # if x['status'] == 'TRADING' 过滤出交易状态正常的币种
        _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]# | symbol.endswith('BUSD'))]  # 过滤usdt合约
        symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单


        # 这是本地时间等于是调度时候的时间，不用重复获取的，放这里试试,放这里应该跟更合理
        # 这里有时候 2023-04-19 16:59:00,而不是2023-04-19 17:00:00,导致后面程序出错
        # 干脆停一秒，看下能否解决这个问题
        time.sleep(2)

        # 获取当前时间
        now = datetime.datetime.now()
        rounded_minute = (now.minute // 5) * 5
        run_time = now.replace(minute=rounded_minute, second=0, microsecond=0)
        if ('h' in self.time_interval) | ('d' in self.time_interval):
            run_time = now.replace(minute=0 ,second=0, microsecond=0)  # aps调度 ,误差基本是毫秒级
        print(run_time)

        # re_download_all_his_coin_data 如果全部下载历史数据，在第一次创建的时候是True
        if self.re_download_all_his_coin_data:

            # 这里获取(1500条)数据
            symbol_candle_data = get_binance_history_candle_data(self.exchange, symbol_list, self.time_interval, run_time,
                                                                 MAX_KEEP_LEN,'swap')

            # run_time是整点时间是多少，这个看下过了多长时间更新完毕,并且打印出BTCUSDT的数据
            ic('swap 数据实时更新完毕', (datetime.datetime.now() - run_time).seconds, symbol_candle_data['BTCUSDT'].head(2))
            ic('swap 数据实时更新完毕',(datetime.datetime.now()- run_time).seconds,symbol_candle_data['BTCUSDT'].tail(2))

            # mysql版本
            df_list = []
            for symbol in symbol_list:
               df_list.append(symbol_candle_data[symbol])
            df = pd.concat(df_list, ignore_index=True)
            df.drop_duplicates(subset=['candle_begin_time','symbol'], keep='last', inplace=True)  # 去重
            df.reset_index(drop=True, inplace=True)
            self.creat_data_table(df, self.name)


            # 在更新一个表，币种表用于比对,这里是最早的时候更新一次,之后这里,在最后新增币种的时候再次更新
            symbol_df = pd.DataFrame()
            if df['symbol'].dtype == 'object':  # 如果是字符串,内存爆了
                df['symbol'] = df['symbol'].astype('category')  # 转换为category类型
            symbol_df['symbol'] = df['symbol']

            symbol_df.drop_duplicates(subset=['symbol'], keep='last', inplace=True)  # 去重
            self.sql.create_talbe(symbol_df, self.db_name , self.name+"_symbol", if_exists="replace")

            # 然后这个调整成Flase,这样下一次就只进行增量操作
            self.re_download_all_his_coin_data = False

        else:
            # 读取已经存储的货币的列表
            symbol_df = self.sql.selet_from_table(self.db_name , self.name + "_symbol", "select*from {};".format(self.name + "_symbol"))
            symbol_df = pd.DataFrame(symbol_df)
            data_symbol_list = symbol_df['symbol'].to_list()

            # 第一次获取的时候需要获取没交易的币种
            # 之后每次补的时候只需要补正在交易的币种就可以了
            _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if x['status'] == 'TRADING']  #  过滤出交易状态正常的币种
            _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]# | symbol.endswith('BUSD'))]  # 过滤usdt合约
            symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单

            # symbol_list在程序一开始的时候获取了一次，但是中间可能有新币没获取到数据文件
            for symbol in symbol_list:# 这个代码好像有点问题，除非在获取一次symbol_list不然好像没有意义,但是只有刚刚上架的币
                # 如果原先的没有存储
                if symbol not in data_symbol_list:
                    # 新上线的币的K线数量,将数据追加进去,
                    # 因为有的时候币种名称已经能抓到了，但是实际上数据还是空的,这里做个容错
                    print("补全数据 {}".format(symbol))
                    _, _df = get_data(symbol, self.exchange, MAX_KEEP_LEN, self.time_interval, run_time,'swap')
                    self.sql.create_talbe(_df, self.db_name , self.name, if_exists="append",text = "swap 补全数据 {}".format(symbol))
                    # 这里存入新的数据，但是不更新数字币列表了，在后面在进行更新


            # 在第一次运行完会再次执行下面的内容，直到第二次才是单独获取
            # =====并行获取所有币种的(1小时K线),增量更新(10)条数据
            symbol_candle_data = get_binance_history_candle_data(self.exchange, symbol_list, self.time_interval, run_time, 10,'swap')

            # run_time是整点时间是多少，这个看下过了多长时间更新完毕,并且打印出BTCUSDT的数据
            ic('swap 数据实时更新完毕',(datetime.datetime.now()- run_time).seconds,symbol_candle_data['BTCUSDT'].tail(2))
            # =============

            # =========== 读取部分数据.根据时间读取当前时间往回推19根K线
            last_time = None
            if "m" in self.name:
                n = int(self.name.split("_")[-1].split("m")[0])
                last_time = run_time - datetime.timedelta(minutes = n*20)

            if "h" in self.name:
                n = int(self.name.split("_")[-1].split("h")[0])
                last_time = run_time - datetime.timedelta(hours= n*20)

            if "d" in self.name:
                n = int(self.name.split("_")[-1].split("d")[0])
                last_time = run_time - datetime.timedelta(days = n *20)


            data_df = self.sql.selet_from_table(self.db_name, self.name, "select*from {} where candle_begin_time >= '{}'"
                                           .format(self.name,last_time))
            data_df = pd.DataFrame(data_df)

            df_list = []
            for symbol in symbol_list:

                # 获取读取数据的最后一个时间节点
                _data_df = data_df[data_df['symbol'] == symbol] # 这里数据库读取的，最后一条数据是还没收盘的
                _data_df.sort_values(by=['candle_begin_time'], inplace=True)

                # 有可能有币，但是是新币数据为空导致，下面代码运行报错
                # 20240411修改，之前是data_df应该是错误的，应该要_data_df才对,因为判断的是这个币的最后一个时间
                # 如果这个币为空后面不运行
                if len(_data_df)!=0:
                    _last_time = _data_df['candle_begin_time'].tolist()[-1] + datetime.timedelta(seconds=1)

                    # 增量数据,跟历史数据对比提取多出的部分
                    symbol_df = symbol_candle_data[symbol]
                    # 发现数据有重复,last_time + 1秒
                    symbol_df = symbol_df[symbol_df['candle_begin_time'] > _last_time ]
                    df_list.append(symbol_df)
                else:
                    print("{} 没有数据可能是新币还未上线 ".format(symbol))

            # ========存入新增数据
            df = pd.concat(df_list, ignore_index=True)
            df.drop_duplicates(subset=['candle_begin_time', 'symbol'], keep='last', inplace=True)  # 去重
            df.reset_index(drop=True, inplace=True)

            try:
                self.sql.create_talbe(df, self.db_name, self.name, if_exists="append",text = 'swap 存入新增数据')
            except:
                pass

            # 这里这个存储是不能去掉的,因为之前的一次更新只运行了一次
            symbol_df = pd.DataFrame()
            symbol_df['symbol'] = df['symbol']
            symbol_df.drop_duplicates(subset=['symbol'], keep='last', inplace=True)  # 去重
            # 第一次运行，先下载一次，又下载一次，第二次下载排除之前下载过的时间的数据，内容可能为0，为0的时候就不更新

            if len(symbol_df) != 0 :
                self.sql.create_talbe(symbol_df, self.db_name , self.name + "_symbol", if_exists="replace",text = "swap 更新 symbol_list 表")

        time_elapsed = time.time() - now_time
        minutes = int(time_elapsed // 60)
        seconds = time_elapsed % 60

        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(self.name + " swap 数据获取完毕 {}m {:.2f}s at {}\n".format(minutes, seconds, current_time))


# 不知道为啥还有点问题,还没村的数据会先被调用出来，但是不影响最终结果，是不是因为调度器打印的前后时差。