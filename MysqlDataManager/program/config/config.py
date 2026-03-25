# -*- coding: utf-8 -*-
import os

# 交易设置 =====================================================================================
test = False # 开启测试

black_symbol_list = []  # 不参与交易 'BTCSTUSDT' 我将数据全部下载下来，用不用在系统里排除

# 因为币安最大获取是1500，超过1500的时候，程序循环获取,小于1500的时候一次性就能获取了
MAX_KEEP_LEN = 1501

# 需要的时间周期
needed_time_interval_list = ['1h'] # 支持1h,30m,15m,5m配置,别填1分钟了

# 时区偏移小时,东九区
timezone_offset = 8
# =====================================================================================

# 本地数据库设置 =====================================================================================

db_addr = "127.0.0.1" # 服务器

user_name = "root" # 账户

user_password = "123456" # 密码

db_name = "bina" # 数据库


# 用于生成文件夹路径
def creat_folders(*args):
    abs_path = os.path.abspath(os.path.join(*args))
    if not os.path.exists(abs_path):
        os.makedirs(abs_path)
    return abs_path

# 文件路径
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
data_path_root = creat_folders(_, os.pardir, os.pardir, 'data','coin')
flag_path_root = creat_folders(_, os.pardir, os.pardir, 'data','flag')


DINGDING_ROBOT_ID =  ''
DINGDING_SECRET = ''

TELEGRAM_TOKEN = '1859756410:AAEdmdCiXAajWA-'
TELEGRAM_CHAT_ID = -1001192236554

WECHAT_CORPID = ''
WECHAT_SECRET = ''
WECHAT_AGENT_ID = ''

DEFAULT_SLEEP_TIMES = 20
DEFAULT_TRY_TIMES = 10
TRADE_MARKET = 'DataManagerV1.1'
