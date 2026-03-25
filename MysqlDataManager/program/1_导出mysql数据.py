from manager.mysql_func import *
import shutil
import os
import pandas as pd


time_interval_list = ['1h']
start_time =  '2024-11-19 08:00:00'
symbol_type = 'spot'

if __name__ == '__main__':

    # 创建数据库对象
    sql = Mysql("127.0.0.1", "root", "123456")

    data_df = sql.selet_from_table("bina")

    print(data_df)


    for interval in time_interval_list:

        # 生成文件夹
        path = "C:\\Users\Administrator\Desktop\MysqlDataManager\data\coin\{}\{}".format(symbol_type,interval)
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            shutil.rmtree(path)
            os.makedirs(path)

        #
        data_df = sql.selet_from_table("bina", 'b_{}_{}'.format(symbol_type,interval), "select*from {} where candle_begin_time >= '{}'"
                                            .format('b_{}_{}'.format(symbol_type,interval), start_time ))

        data_df =  pd.DataFrame(data_df)
        symbol_list = list(set(data_df['symbol'].to_list())) # 去重获得总共多少币种

        print("品种数量 : {}".format(len(symbol_list)))
        for symbol in symbol_list:
            df = data_df.copy()
            df = df[df['symbol'] == symbol]
            # df.drop_duplicates(subset=['symbol','candle_begin_time'], keep='last', inplace=True)  # 去重
            df.reset_index(drop=True, inplace=True)
            print("\n"+symbol +" : ")
            print(df.tail(2).to_markdown())
            df.to_csv(path + "\\" + symbol + ".csv", encoding="gbk")


