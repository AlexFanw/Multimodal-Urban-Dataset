import pymysql
from sshtunnel import SSHTunnelForwarder


def connect_mysql():
    """ 通过SSH连接云服务器

    :return: pymysql连接通道
    """
    server = SSHTunnelForwarder(
        ssh_address_or_host=('1.15.248.241', 22),  # 云服务器地址IP和端口port
        ssh_username='root',  # 云服务器登录账号admin
        ssh_password='ALEx8802732',  # 云服务器登录密码password
        remote_bind_address=('localhost', 3306)  # 数据库服务地址ip,一般为localhost和端口port，一般为3306
    )
    server.start()
    # 连接数据库，创建连接对象connection
    # 连接对象作用是：连接数据库、发送数据库信息、处理回滚操作（查询中断时，数据库回到最初状态）、创建新的光标对象
    db = pymysql.connect(host='127.0.0.1',  # host属性"
                         port=server.local_bind_port,  # port属性
                         user='root',  # 用户名
                         password='ALEx8802732',  # 此处填登录数据库的密码
                         db='multimodal_urban',  # 数据库名
                         charset='utf8'
                         )
    return db
