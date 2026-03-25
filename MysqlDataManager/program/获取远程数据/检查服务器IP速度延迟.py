import subprocess

def ping(host, count=4):
    """
    使用ping命令检查网络稳定性
    :param host: 目标主机
    :param count: ping的次数
    :return: ping的结果
    """
    try:
        output = subprocess.check_output(
            ["ping", "-n", str(count), host],
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        return output
    except subprocess.CalledProcessError as e:
        return e.output

def tracert(host):
    """
    使用tracert命令检查网络延迟（Windows）
    :param host: 目标主机
    :return: tracert的结果
    """
    try:
        output = subprocess.check_output(
            ["tracert", host],
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        return output
    except subprocess.CalledProcessError as e:
        return e.output

def check_network_stability(host):
    """
    检查网络稳定性，结合ping和tracert
    :param host: 目标主机
    :return: ping和tracert的结果
    """
    print(f"Checking network stability for {host}...")

    print("\n--- Ping Results ---")
    ping_result = ping(host)
    print(ping_result)

    print("\n--- Tracert Results ---")
    tracert_result = tracert(host)
    print(tracert_result)

# 示例使用
host = "43.155.11.151"
check_network_stability(host)