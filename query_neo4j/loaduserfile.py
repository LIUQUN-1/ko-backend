import os
from django.http import JsonResponse
from minio import Minio, InvalidResponseError, S3Error
import hashlib
from neo4j import GraphDatabase
from bs4 import BeautifulSoup
import errno
import json
import datetime
import neo4j
import re
from random import uniform
import jieba
import jieba.analyse
from cryptography.fernet import Fernet
import base64
import os
import pymysql
class MySQLDatabase:
    def __init__(self, host, user, password, database, charset="utf8mb4"):
        """
        初始化数据库连接
        """
        self.config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "charset": charset
        }
        self.connection = None

    def connect(self):
        """
        建立数据库连接
        """
        try:
            self.connection = pymysql.connect(**self.config)
            print("数据库连接成功！")
        except pymysql.MySQLError as e:
            print(f"数据库连接失败：{e}")
            raise

    def insert_data(self, table_name, data):
        try:
            # 先检查主键是否存在
            primary_key = list(data.keys())[0]  # 假设主键在第一个位置
            primary_key_value = data[primary_key]

            # 生成检查主键是否存在的 SQL 查询
            check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value,))
                result = cursor.fetchone()

                if result[0] > 0:
                    print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
                    return  # 主键已存在，跳过插入操作

            # 生成插入 SQL 语句
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 执行插入操作
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print("数据插入成功！")
        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            self.connection.rollback()  # 回滚事务

    def insert_relation(self, table_name, data):
        try:
            # 先检查主键是否存在
            primary_key = list(data.keys())[0]  # 假设主键在第一个位置
            last_key = list(data.keys())[-1]
            primary_key_value = data[primary_key]
            last_key_value = data[last_key]

            # 生成检查主键是否存在的 SQL 查询
            check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s AND {last_key} = %s "
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value, last_key_value, ))
                result = cursor.fetchone()

                if result[0] > 0:
                    print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
                    return  # 主键已存在，跳过插入操作

            # 生成插入 SQL 语句
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 执行插入操作
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print("数据插入成功！")
        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            self.connection.rollback()  # 回滚事务

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭！")
# def search_file_in_File(tx,userID):
#     query_word = ("MATCH (h:File) WHERE h.user_id=$name RETURN h,id(h)")
#     result = tx.run(query_word, name=int(userID))
#     return result.data()
def search_file_in_Strict(tx,userID):
    query_word = ("""MATCH (h:Strict) 
WHERE h.user_id = $name or h.user_id=$name1
RETURN h,id(h)
UNION
MATCH (h:File) 
WHERE h.user_id = $name or h.user_id=$name1
RETURN h,id(h)
""")
    result = tx.run(query_word, name=int(userID),name1=str(userID))
    return result.data()

def search_file_in_mysql(db,nodeID):
    query="""
        SELECT `private`,`name` FROM `file` 
        WHERE id IN %s
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (nodeID,))
            result = cursor.fetchall()  # 使用 fetchone() 获取单条记录
            if result:  # 如果有结果
                return result
            else:  # 无匹配记录
                return []
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def format_date(date_str):
    # 分割日期字符串
    parts = date_str.split('-')
    day = parts[2]

    # 检查日是否小于10
    if int(day) < 10:
        # 将日转换为两位数格式
        parts[2] = '0' + day

    # 重新组合日期字符串
    formatted_date = '-'.join(parts)
    return formatted_date
def main(request):
    userID = request.GET['userID']
    # result=
    File_data=[]
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session=driver.session()
    json_data_1=session.write_transaction(search_file_in_Strict, userID)
    all_node_id=[] #最多只能承受1万个
    if len(json_data_1)>0:
        # print(json_data_1)
        for i in json_data_1:
            temp={}
            temp_data=i["h"]
            temp["name"]=temp_data["name"]
            temp["path"]=temp_data["path"]
            temp["updata_time"]=format_date(temp_data["timestamp"])
            temp["node_id"]=i["id(h)"]
            all_node_id.append(str(temp["node_id"]))
            File_data.append(temp)
    print(all_node_id)
    print(File_data)
    all_node_private=search_file_in_mysql(db,all_node_id)
    new_File_data=[]
    for i in all_node_private:
        for j in File_data:
            if j["name"]==str(i[1]):
                if str(i[0])=="1":
                    j["shareLevel"]="自带文档"
                    new_File_data.append(j)
                elif str(i[0])=="0":
                    j["shareLevel"]="完全共享"
                    new_File_data.append(j)
    # print(File_data)
    # print(Strict_Data)
    sorted_data = sorted(new_File_data, key=lambda x: x['updata_time'],reverse=True)
    return json.dumps(sorted_data, ensure_ascii=False)
    # driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # session=driver.session()
    # json_data_1=session.write_transaction(search_file_in_File, userID)
    # File_data=[]
    # if len(json_data_1)>0:
    #     # print(json_data_1)
    #     for i in json_data_1:
    #         temp={}
    #         temp_data=i["h"]
    #         temp["name"]=temp_data["name"]
    #         temp["path"]=temp_data["path"]
    #         temp["updata_time"]=format_date(temp_data["timestamp"])
    #         temp["node_id"]=i["id(h)"]
    #         temp["shareLevel"] = "完全共享"
    #         File_data.append(temp)
    # # print(File_data)
    # json_data_2 = session.write_transaction(search_file_in_Strict, userID)
    # Strict_Data=[]
    # if len(json_data_2)>0:
    #     # print(json_data_1)
    #     for i in json_data_2:
    #         temp={}
    #         temp_data=i["h"]
    #         temp["name"]=temp_data["name"]
    #         temp["path"]=temp_data["path"]
    #         temp["updata_time"]=format_date(temp_data["timestamp"])
    #         temp["node_id"]=i["id(h)"]
    #         temp["shareLevel"]="自带文档"
    #         Strict_Data.append(temp)
    # # print(Strict_Data)
    # all_data=File_data+Strict_Data
    # sorted_data = sorted(all_data, key=lambda x: x['updata_time'],reverse=True)
    # return json.dumps(sorted_data, ensure_ascii=False)