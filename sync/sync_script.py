from datetime import datetime
from collections import deque
from time import sleep

import pymysql

from sync.config import MYSQL_CONFIG_ONLINE
from sync.config import MYSQL_CONFIG_OUTLINE


class SyncDatabase():

    def __init__(self, data_division_num=10):

        self.conn_outline = self.create_conn(setting=MYSQL_CONFIG_OUTLINE)
        self.conn_online = self.create_conn(setting=MYSQL_CONFIG_ONLINE)
        self.tag_time = "2016-3-10 15:00:00"
        self.collect_urls = set()

        # 数据分组插入，分组数
        self.data = deque()
        self.data_division_num = data_division_num
        self.data_division = []

        # 装载urls
        sql = "select url from spiderdb"
        urls = self.query(sql, self.conn_online)
        if urls:
            for item in urls:
                self.collect_urls.add(item[0])

    def length(self):
        return len(self.data)

    @staticmethod
    def create_conn(setting):
        conn = pymysql.Connect(**setting)
        return conn

    def query(self, sql, conn):
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def insert(self, sql):
        sleep(0.5)
        try:
            with self.conn_online.cursor() as cursor:
                cursor.execute(sql)
            self.conn_online.commit()
        except Exception as e:
            print('数据插入错误')
            print(e)

    def process_flow(self, tag_time):
        # data太长，需要分割
        query_sql = """select * from spiderdb where collect_time > \'{tag_time}\' ORDER BY collect_time DESC""".format(tag_time=tag_time)
        query_data = self.query(query_sql, self.conn_outline)

        # 对data进行分组
        if query_data:
            self.data.extend(deque(query_data))
            for i in range(self.length()):
                self.data_division.append(self.data.popleft())
                if len(self.data_division) >= self.data_division_num:
                    insert_sql = self.format_insert_sql(self.data_division)
                    if insert_sql:
                        self.insert(insert_sql)
                        self.data_division = []

    def format_insert_sql(self, data):
        # 解析列表，替换值None和''为''Null, 字符串加上''
        data_target = []
        for item in data:
            sub_data_target = []
            if isinstance(item, tuple) and item[8] in self.collect_urls:
                continue
            else:
                self.collect_urls.add(item[8])
            # 格式化字段值
            for item_value in item[1:]:
                if item_value is None or '':
                    item_value = 'NULL'
                elif isinstance(item_value, str):
                    item_value = self.format_str(item_value)
                elif isinstance(item_value, datetime):
                    item_value = self.format_str(str(item_value))
                else:
                    item_value = str(item_value)
                sub_data_target.append(item_value)
            data_target.append(','.join(sub_data_target))

        if data_target == []:
            return None

        values = '), ('.join(data_target)

        insert_sql = """insert into spiderdb (source, citycode, type, create_time, title, contact, tel, url,
                      rent, rent_unit, area, district, business_center, address, neighborhood, industry_type, industry,
                      detail, img, engaged, minus_rent, shop_state, shop_name, suit, cost, cost_unit,
                      sub_area, collect_time) values ( {values} )
                      """.format(values=values)
        return insert_sql

    def format_str(self, str):
        return "\'" + str + "\'"
