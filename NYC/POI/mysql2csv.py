import pandas as pd

from connect_mysql import connect_mysql


class MysqlSave:

    def __init__(self):
        self.content = connect_mysql()
        self.cursor = self.content.cursor()

    def search_and_save(self, sql, csv_file):
        """
        导出为csv的函数
        :param sql: 要执行的mysql指令
        :param csv_file: 导出的csv文件名
        :return:
        """
        # 执行sql语句
        self.cursor.execute(sql)

        # 拿到表头
        des = self.cursor.description
        title = [each[0] for each in des]

        # 拿到数据库查询的内容
        result_list = []
        for each in self.cursor.fetchall():
            result_list.append(list(each))

        # 保存成dataframe
        df_dealed = pd.DataFrame(result_list, columns=title)
        # 保存成csv 这个编码是为了防止中文没法保存，index=None的意思是没有行号
        df_dealed.to_csv(csv_file, index=None, encoding='utf_8_sig')


if __name__ == '__main__':
    mysql = MysqlSave()
    mysql.search_and_save('SELECT * FROM nyc_poi;', "nyc_poi.csv")
    mysql.search_and_save('SELECT * FROM nyc_poi_reviews;', "nyc_poi_reviews.csv")
