from neo4j import GraphDatabase
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
def search_file_in_Neo4j(tx,userID,nodeID,FileStatus,db):
    if FileStatus=="完全共享":
        # query_word = ("MATCH (h:File) WHERE h.user_id=$name and id(h)=$nodeID REMOVE h:File SET h:Strict RETURN h")
        query = """
                UPDATE `db_hp`.`file` 
                SET `private` = "1" 
                WHERE id = %s AND userid = %s;
                    """

    elif FileStatus=="自带文档":
        # query_word = ("MATCH (h:Strict) WHERE h.user_id=$name and id(h)=$nodeID REMOVE h:Strict SET h:File RETURN h")
        query ="""
               UPDATE `db_hp`.`file` 
                SET `private` = "0" 
                WHERE id = %s AND userid = %s;
                            """
    else:
        return "error"
    # result = tx.run(query_word, name=int(userID),nodeID=int(nodeID))
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(nodeID),int(userID), ))
            db.connection.commit()
            return int(cursor.rowcount)
    except pymysql.MySQLError as e:
        print(f"修改用户状态失败：{e}")
        raise
    # return result.data()
def main(request):
    nodeID = request.GET['node_ID']
    userID= request.GET['userID']
    FileStatus =request.GET['status']
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session=driver.session()
    db=MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    data=session.write_transaction(search_file_in_Neo4j, userID,nodeID,FileStatus,db)
    if data=="error":
        return "文件状态异常！！"
    if data==0:
        return "不存在该文件！"
    return "顺利修改！！"
# import os
# from minio import Minio, InvalidResponseError, S3Error
# from neo4j import GraphDatabase
# import json
# import zipfile
# import hashlib
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import datetime
# def search_file_in_Neo4j(tx,userID,nodeID,FileStatus):
#     if FileStatus=="完全共享":
#         query_word = ("MATCH (h:File) WHERE h.user_id=$name and id(h)=$nodeID REMOVE h:File SET h:Strict RETURN h")
#     elif FileStatus=="自带文档":
#         query_word = ("MATCH (h:Strict) WHERE h.user_id=$name and id(h)=$nodeID REMOVE h:Strict SET h:File RETURN h")
#     else:
#         return "error"
#     result = tx.run(query_word, name=int(userID),nodeID=int(nodeID))
#     return result.data()
# def main(request):
#     nodeID = request.GET['node_ID']
#     userID= request.GET['userID']
#     FileStatus =request.GET['status']
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session=driver.session()
#     data=session.write_transaction(search_file_in_Neo4j, userID,nodeID,FileStatus)
#     if data=="error":
#         return "文件状态异常！！"
#     if len(data)<=0:
#         return "不存在该文件！"
#     return "顺利修改！！"