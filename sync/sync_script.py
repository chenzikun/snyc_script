from datetime import datetime
from time import sleep

import pymysql

from sync.config import MYSQL_CONFIG_ONLINE
from sync.config import MYSQL_CONFIG_OUTLINE


class SyncDatabase():

    def __init__(self, data_division_num=10):

        self.conn_outline = self.create_conn(setting=MYSQL_CONFIG_OUTLINE)
        self.conn_online = self.create_conn(setting=MYSQL_CONFIG_ONLINE)
        self.tag_time = "2016-3-10 15:00:00"
        self.collect_ids = set()

        # 数据分组插入，分组数
        self.data = []
        self.data_division_num = data_division_num
        self.data_division = []


        # 装载ids
        sql = "select id from spiderdb"
        ids = self.query(sql, self.conn_online)
        if ids:
            for item in ids:
                self.collect_ids.add(item)

    def length(self):
        return len(self.data)

    def create_conn(self, setting):
        conn = pymysql.Connect(**setting)
        return conn

    def query(self, sql, conn):
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result

    def insert(self, sql):
        sleep(1)
        try:
            with self.conn_online.cursor() as cursor:
                cursor.execute(sql)
            self.conn_online.commit()
        except Exception as e:
            print(e)

    def process_flow(self, tag_time):
        # data太长，需要分割
        query_sql = """select * from spiderdb where collect_time > \'{tag_time}\' """.format(tag_time=tag_time)
        data = self.query(query_sql, self.conn_outline)

        # 对data进行分组
        if data:
            self.data.extend(list(data))
            for i in range(self.length()):
                self.data_division.append(self.data.pop())
                if len(self.data_division) >= self.data_division_num:
                    insert_sql = self.format_insert_sql(self.data_division)
                    self.insert(insert_sql)
                    self.data_division = []


    def format_insert_sql(self, data):
        # 解析列表，替换值None和''为''Null, 字符串加上''
        data_target = []
        for item in data:
            sub_data_target = []
            if isinstance(item, tuple) and item[0] in self.collect_ids:
                continue
            # 格式化字段值
            for item_value in item:
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
        values = '), ('.join(data_target)

        insert_sql = """insert into spiderdb (id, source, citycode, type, create_time, title, contact, tel, url,
                      rent, rent_unit, area, district, business_center, address, neighborhood, industry_type, industry,
                      detail, img, engaged, minus_rent, shop_state, shop_name, suit, cost, cost_unit,
                      sub_area, collect_time) values ( {values} )
                      """.format(values=values)
        return insert_sql

    def format_str(self, str):
        return "\'" + str + "\'"
