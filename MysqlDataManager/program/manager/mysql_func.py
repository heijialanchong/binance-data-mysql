import pymysql
from sqlalchemy import create_engine
# 去掉报错
from warnings import filterwarnings
import warnings
warnings.filterwarnings("ignore")
from pytz_deprecation_shim import PytzUsageWarning
filterwarnings('ignore', category=PytzUsageWarning)
import time

class Mysql(object):

    def __init__(self,db_addr,user_name,user_password,connect_timeout = 120 , query_timeout = 120000):
        self.db_addr = db_addr # host 地址
        self.user_name = user_name # host 登录用户名
        self.user_password = user_password  # host 登录密码
        self.conn = None
        self.cursor = None
        self.connect_timeout = connect_timeout # 建立连接的时长,单位为s
        self.query_timeout = query_timeout  # 查询超时设置，单位为毫秒

    # 判断数据库是否关闭
    def get_conn_state(self):
        return self.conn.open


    # 关闭数据库
    def close_conn(self):

        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


    def conn_mysql(self, retry_times=10, sleep_seconds=5):
        attempt = 0
        while attempt < retry_times:
            try:
                # 连接信息：IP，用户名，密码，数据库，编码，端口号
                db = pymysql.connect(
                    host=self.db_addr,  # 连接的数据库服务器主机名
                    port=3306,  # 数据库端口号
                    user=self.user_name,  # 数据库登录用户名
                    passwd=self.user_password,
                    charset='utf8',  # 连接编码
                    cursorclass=pymysql.cursors.DictCursor,  # 如果要返回字典(dict)表示的记录
                    connect_timeout = self.connect_timeout   # 超过查询时长断开连接
                )

                self.conn = db
                self.cursor = self.conn.cursor()
                # print("Connected to MySQL database")
                # 设置查询超时
                self.cursor.execute(f"SET SESSION max_execution_time={self.query_timeout}")


                return db

            except pymysql.MySQLError as e:
                attempt += 1
                print(f"Error while connecting to MySQL: {e}. Attempt {attempt} of {retry_times}. Retrying in {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)

        raise Exception("Max retry attempts reached. Failed to connect to MySQL database")


    # 创建数据库
    def create_database(self,db_name):
        """

        :param db_name: 数据库名称
        :return:
        """
        "创建数据库"
        db = self.conn_mysql() # 连接数据库

        with db.cursor() as cursor:
            cursor.execute("show databases like '%s'" % db_name)     # 执行sql语句
            row = cursor.fetchone() # 获取第一行数据

            if row: # 数据存在
                is_continue = input("已经存在%s数据库，是否确定重建？\n注意原有的数据会丢失！\n[y/n]" % db_name)
                is_continue = is_continue.lower() # 输入的转成小写
                if is_continue == 'y':
                    cursor.execute('DROP DATABASE %s' % db_name)
                    db.commit() # 提交数据库
                else:
                    exit(1) # 异常退出

            cursor.execute("create database %s charset utf8;" % db_name)
            # 提交数据库执行
            db.commit()
            cursor.close()
        db.close()

    # 删除表
    def drop_talbe(self,db_name,db_table_list):
        """

        :param db_name: # 数据库名称
        :param db_table_list: # 要删除的列表[]
        :return:
        """
        db = self.conn_mysql()  # 连接数据库
        with db.cursor() as cursor:
            cursor.execute("use %s" % db_name)  # 执行sql语句,使用某个表
            for table in db_table_list: # 所有列表
                cursor.execute('DROP table %s' % table) # 删除所有表
                print("drop table {}".format(table))

            cursor.close()  # 关闭游标
        db.close()  # 关闭数据库

    # 创建表
    def create_talbe(self,df,db_name,db_table,if_exists = "replace",text = ""):
        """
        :param df : DateFrame数据
        :param db_name : 数据库名称
        :param db_name : 表名称
`   ``  :param if_exists: 当数据库中已经存在数据表时对数据表的操作，有replace替换、append追加，fail则当表存在时提示ValueError。
        :param df:
        :return:
        """

        conn = create_engine( 'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(self.user_name,self.user_password,self.db_addr,db_name))
        # print(df)
        # from sqlalchemy import create_engine
        try:
            df.to_sql(db_table, con = conn, index = False,if_exists = if_exists)
        except Exception as ee:
            print(text + "写入数据库失败")
        finally:
            # 关闭数据库
            conn.dispose()

    # 查询表数据，查询所有表名
    def selet_from_table(self, db_name, db_table=None, text=None,query_timeout = 1200000):
        """
        :param db_name: 数据库名称
        :param db_table: 查询的表，如果不填查询所存在的表
        :param text: sql查询语句
        :return:
        """

        def func(x):
            return x['Tables_in_' + db_name]

        db = self.conn_mysql()  # 连接数据库

        with db.cursor() as cursor:
            cursor.execute("USE %s" % db_name)  # 进入数据库
            if db_table is not None:
                cursor.execute("SHOW TABLES LIKE '%s'" % db_table)  # 执行sql语句
                row = cursor.fetchone()  # 获取第一行数据
                if row:  # 数据存在
                    query_with_timeout = f"/*+ MAX_EXECUTION_TIME({query_timeout}) */ {text}"
                    cursor.execute(query_with_timeout)
                    r1 = cursor.fetchall()
                else:
                    r1 = []  # 表不存在时返回空列表
                    print(f"Table '{db_table}' does not exist in the database '{db_name}'.")
            else:
                cursor.execute("SHOW TABLES")  # 执行sql语句
                r1 = cursor.fetchall()
                r1 = list(map(func, r1))

        db.close()  # 关闭数据库

        return r1



if __name__ == '__main__':


    pass
