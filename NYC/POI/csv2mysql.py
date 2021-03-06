import csv

from connect_mysql import connect_mysql


def insert(db, cur, sql, args):
    #
    try:
        cur.execute(sql, args)
    except Exception as e:
        print(e)
        db.rollback()


def load_csv(csv_file_path, table_name, database, part_num):
    """
    将csv文件导入线上数据库中

    :param csv_file_path: csv文件路径
    :param table_name: 数据库表名
    :param database: 数据库名
    :param part_num: csv part序号（1-6）
    :return:
    """
    conn = connect_mysql()
    cur = conn.cursor()
    # 使用数据库
    cur.execute('use %s' % database)
    # 设置编码格式
    cur.execute('SET NAMES utf8;')
    cur.execute('SET character_set_connection=utf8;')

    # 读取csv文件第一行字段名，创建表
    file = open(csv_file_path, 'r', encoding='utf-8')
    reader = file.readline()
    b = reader.split(',')
    b.append("part")
    colum = 'id int auto_increment,'
    for a in b:
        colum = colum + a + ' varchar(255),'
    colum = colum[:-1]
    # 编写sql，create_sql负责创建表，data_sql负责导入数据
    create_sql = 'create table if not exists ' + table_name + ' ' + \
                 '(' + colum + ",constraint " + table_name + " primary key (id)" + ')' \
                 + ' DEFAULT CHARSET=utf8'
    create_sql = create_sql.replace(".", "_").replace("photos varchar(255)", "photos varchar(2000)")
    # 执行create_sql，创建表
    print(create_sql)
    cur.execute(create_sql)

    # 逐行插入数据库
    insert_sql = 'insert into ' + table_name + '(' + ','.join(b).replace(".", "_") + ')' + ' values(' + ','.join(['%s']*len(b)) + ')'
    print(insert_sql)
    flag = 0
    for line in csv.reader(file):
        line.append("PART" + str(part_num))
        args = tuple(line)
        insert(cur=cur, sql=insert_sql, args=args, db=conn)
        flag += 1
        if flag % 100 == 0:
            print(flag)
            conn.commit()

    conn.commit()
    # 关闭连接
    conn.close()
    cur.close()


if __name__ == "__main__":
    for i in range(5, 7):
        file_path = "/Users/alexfan/Desktop/nyc_poi/output" + str(i) + ".csv"
        load_csv(file_path, "nyc_poi", "multimodal_urban", i)

