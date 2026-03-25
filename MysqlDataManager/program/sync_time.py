# -*- coding: utf-8 -*-
"""
中性策略框架 | 邢不行 | 2024分享会
author: 邢不行
微信: xbx6660
"""
import traceback
import platform
import subprocess
import ntplib
import time
from time import ctime
from datetime import datetime
import pandas as pd
from datetime import timedelta

# ===下次运行时间
def next_run_time(time_interval, ahead_seconds=5):
    """
    根据time_interval，计算下次运行的时间。
    PS：目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间之间计算的间隙
    :return: 下次运行的时间

    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00

    10m  当前时间为：12:38:51  返回时间为：12:40:00
    10m  当前时间为：12:11:01  返回时间为：12:20:00

    5m  当前时间为：12:33:51  返回时间为：12:35:00
    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00
    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00
    """
    # 检测 time_interval 是否配置正确，并将 时间单位 转换成 可以解析的时间单位
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):  # 分钟兼容使用T配置，例如  15T 30T
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):  # 小时兼容使用H配置， 例如  1H  2H
        time_interval = time_interval.replace('H', 'h')
    else:
        print('time_interval格式不符合规范。程序exit')
        exit()

    # 将 time_interval 转换成 时间类型
    ti = pd.to_timedelta(time_interval)
    # 获取当前时间
    now_time = datetime.now()
    # 计算当日时间的 00：00：00
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    # 每次计算时间最小时间单位1分钟
    min_step = timedelta(minutes=1)
    # 目标时间：设置成默认时间，并将 秒，毫秒 置零
    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        # 增加一个最小时间单位
        target_time = target_time + min_step
        # 获取目标时间已经从当日 00:00:00 走了多少时间
        delta = target_time - this_midnight
        # delta 时间可以整除 time_interval，表明时间是 time_interval 的倍数，是一个 整时整分的时间
        # 目标时间 与 当前时间的 间隙超过 ahead_seconds，说明 目标时间 比当前时间大，是最靠近的一个周期时间
        if int(delta.total_seconds()) % int(ti.total_seconds()) == 0 and int((target_time - now_time).total_seconds()) >= ahead_seconds:
            break

    return target_time

# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True, cheat_seconds=120):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param time_interval: 时间周期配置，用于计算下个周期的时间
    :param if_sleep: 是否进行sleep
    :param ahead_time: 最小时间误差
    :param cheat_seconds: 相对于下个周期时间，提前或延后多长时间， 100： 提前100秒； -50：延后50秒
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)
    # 计算延迟之后的目标时间
    target_time = run_time
    # 配置 cheat_seconds ，对目标时间进行 提前 或者 延后
    if cheat_seconds != 0:
        target_time = run_time - timedelta(seconds=cheat_seconds)
    print('程序下次运行的时间：', target_time, '\n')

    # sleep
    if if_sleep:
        # 计算获得的 run_time 小于 now, sleep就会一直sleep
        _now = datetime.now()
        if target_time > _now:  # 计算的下个周期时间超过当前时间，直接追加一个时间周期
            time.sleep(max(0, (target_time - _now).seconds))
        while True:  # 在靠近目标时间时
            if datetime.now() > target_time:
                time.sleep(1)
                break

    return run_time

def sync_time_ntp():
    """
    用于Linux和Mac同步系统时间
    默认添加了一些较为常见的ntp时间同步服务器，可以自行修改
    """
    ntp_servers = ['time.apple.com', 'ntp.aliyun.com', 'pool.ntp.org', 'time.google.com', 'time.windows.com']
    for ntp_server in ntp_servers:
        c = ntplib.NTPClient()
        try:
            response = c.request(ntp_server, timeout=5, version=3)  # 请求并同步时间
            timestamp = response.tx_time  # 获取ntp服务器的时间
            datetime_object = datetime.fromtimestamp(timestamp)
            datetime_string = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
            current_time = ctime()  # 调用ctime函数来获取当前时间
            print(f'从NTP服务器{ntp_server}获取的时间为:', datetime_string)
            print('已同步到服务器时间:', current_time)
            break  # 如果成功同步一个服务器，退出循环
        except ntplib.NTPException as e:
            print(f'NTP服务器{ntp_server}请求超时或发生其他NTP异常:', str(e))
        except Exception as e:
            print(f'与NTP服务器{ntp_server}通信时发生错误:', str(e))


def sync_time_windows():
    """
    用于windows同步系统时间
    注意windows情况下，需要使用管理员身份运行脚本才可以。
    这里运行的时候，可能会存在乱码的情况，主要原因是window的命令行与pycharm的编码不匹配。可以直接使用命令行来操作。
    windows使用时，还需要打开 Windows Time服务，如果没有开启服务，w32tm命令无法使用。
    """
    try:
        subprocess.run(['w32tm', '/resync'], check=True, timeout=5)
        print('时间已同步')
    except subprocess.TimeoutExpired as e:
        print('时间同步超时:', str(e))
    except Exception as e:
        print('时间同步失败:', str(e))


def main():
    while True:
        # ===sleep直到该时间
        sleep_until_run_time('12h', if_sleep=True, cheat_seconds=0)  # 每12h运行一次

        # ===根据当前系统执行不同的同步时间操作
        os_type = platform.system()
        if os_type == 'Windows':  # win执行
            sync_time_windows()
        elif os_type == 'Linux' or os_type == 'Darwin':  # linux和mac执行
            sync_time_ntp()
        else:
            print('不支持的操作系统类型')


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as err:
            msg = '同步时间脚本出错，10s之后重新运行，出错原因: ' + str(err)
            print(msg)
            print(traceback.format_exc())
            time.sleep(10)
