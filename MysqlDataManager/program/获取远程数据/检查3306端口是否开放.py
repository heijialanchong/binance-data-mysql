import socket

host = 'localhost'
port = 3306

# 创建一个 socket 对象
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 设置超时时间为 0.5 秒
s.settimeout(0.5)

try:
    # 尝试连接主机和端口
    s.connect((host, port))
    print(f"Port {port} is open on {host}")
except:
    # 如果连接失败，表示端口未开放
    print(f"Port {port} is closed on {host}")

# 关闭 socket 连接
s.close()