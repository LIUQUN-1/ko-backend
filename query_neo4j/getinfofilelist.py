import sys
import time
import hashlib
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
import zipfile
import os
import tqdm
from minio import Minio, InvalidResponseError, S3Error
from neo4j import GraphDatabase
import zhconv
import shutil
import json
import chardet
import numpy as np
from collections import Counter
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
                cursor.execute(check_query, (primary_key_value, last_key_value,))
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

    def query_tables(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"查询失败：{e}")
            raise

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭！")
class Bucket:

    def __init__(self, minio_address, minio_admin, minio_password):
        # 通过ip 账号 密码 连接minio server
        # Http连接 将secure设置为False
        self.minioClient = Minio(endpoint=minio_address,
                                 access_key=minio_admin,
                                 secret_key=minio_password,
                                 secure=False)

        # 获取桶里某个目录下的所有目录和文件
    def download_minio_object(self,bucket_name, object_name, local_file_path):
        try:
            self.minioClient.fget_object(bucket_name, object_name, local_file_path)
        except Exception as e:
            return e
def get_sha1_hash(file_name):
    shal_hash = hashlib.sha1(file_name.encode()).hexdigest()
    return shal_hash
def file_iterator(file_path, chunk_size=512):
    """
    文件生成器,防止文件过大，导致内存溢出
    :param file_path: 文件绝对路径
    :param chunk_size: 块大小
    :return: 生成器
    """
    with open(file_path, mode='rb') as f:
        while True:
            c = f.read(chunk_size)
            if c:
                yield c
            else:
                break
def deletezip(directory):
    for filename in os.listdir(directory):
        # 判断是否以 .zip 结尾
        if filename.endswith('.zip'):
            # 构建完整的文件路径
            file_path = os.path.join(directory, filename)
            # 删除文件
            os.remove(file_path)


def zipDir(dirpath, outFullName):
    """
    压缩指定文件夹
    :param dirpath: 目标文件夹路径
    :param outFullName: 压缩文件保存路径+xxxx.zip
    :return: 无
    """
    zip = zipfile.ZipFile(outFullName, "a", zipfile.ZIP_DEFLATED)
    for path, dirnames, filenames in os.walk(dirpath):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = dirpath
        # print(dirpath.split("\\")[-1])
        for filename in filenames:
            zip.write(os.path.join(path, filename), os.path.join(fpath, filename))
    zip.close()
def get_similarity(str1,str2):
    # 统计每个字符串中字符的频率
    return 0.5
def get_json_data(x,search_name,file_name=""):
    temp={}
    temp["node_id"] = int(x._id)
    if file_name=="":
        temp["name"] = x._properties["name"]
    else:
        temp["name"] = file_name
    if search_name in x._properties["name"]:
        temp["similarity"]=1*100
    else:
        temp["similarity"]=get_similarity(search_name,temp["name"])
    return temp
def search_nodeID_to_file(nodeID,content,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    query = """
    SELECT ef.sim, f.*
    FROM file AS f
    JOIN entity_to_file AS ef ON f.id = ef.file_id
    WHERE ef.entity = %s and f.id=%s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (content,nodeID))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise

def check_node_exists(name,node_id,userID,session):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id and n.name=$name and (n.user_id=$userID or n.private=0)
        RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
        """,
        node_id=int(node_id),
        name=name,
        userID=int(userID)
    )
    return result.single()[0]

def check_node_strict(name, node_id, userID,session):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id and n.name=$name and (n.user_id=$userID or n.private=0)
        RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
        """,
        node_id=int(node_id),
        name=name,
        userID=int(userID)
    )
    return result.single()[0]
    # return result.single()[0]  # 返回查询结果的第一个值
def main(request):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )

    node_id = request.GET["nodeID"]
    content = request.GET["content"]
    userID =request.GET["userID"]
    nodeIDs = str(node_id).split(",")

    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session = driver.session()

    db.connect()
    file_json_data=[]
    for i1 in nodeIDs:
        result = search_nodeID_to_file(int(i1), content,db)

        if len(result)<=0:
            pass
        else:
            for i in result:
                temp = {}
                temp["node_id"] = i[1]
                temp["name"] = i[2]
                temp["similarity"] = (float)(int(i[0]*10000)/100)
                print(i[1])
                print(i[2])
                result = check_node_exists(i[2],i[1],userID,session)
                if ("1" in str(result)):
                    file_json_data.append(temp)
    # sorted_data=[]
    sorted_data = sorted(file_json_data, key=lambda x: x['similarity'],reverse=True)
    for i in sorted_data:
        i["similarity"]=str(i["similarity"])+"%"
    return json.dumps(sorted_data, ensure_ascii=False)
# import sys
# import time
# import hashlib
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import zipfile
# import os
# import tqdm
# from minio import Minio, InvalidResponseError, S3Error
# from neo4j import GraphDatabase
# import zhconv
# import shutil
# import json
# import chardet
# import numpy as np
# from collections import Counter
# import pymysql
# class MySQLDatabase:
#     def __init__(self, host, user, password, database, charset="utf8mb4"):
#         """
#         初始化数据库连接
#         """
#         self.config = {
#             "host": host,
#             "user": user,
#             "password": password,
#             "database": database,
#             "charset": charset
#         }
#         self.connection = None
#
#     def connect(self):
#         """
#         建立数据库连接
#         """
#         try:
#             self.connection = pymysql.connect(**self.config)
#             print("数据库连接成功！")
#         except pymysql.MySQLError as e:
#             print(f"数据库连接失败：{e}")
#             raise
#
#     def insert_data(self, table_name, data):
#         try:
#             # 先检查主键是否存在
#             primary_key = list(data.keys())[0]  # 假设主键在第一个位置
#             primary_key_value = data[primary_key]
#
#             # 生成检查主键是否存在的 SQL 查询
#             check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s"
#             with self.connection.cursor() as cursor:
#                 cursor.execute(check_query, (primary_key_value,))
#                 result = cursor.fetchone()
#
#                 if result[0] > 0:
#                     print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
#                     return  # 主键已存在，跳过插入操作
#
#             # 生成插入 SQL 语句
#             columns = ", ".join(data.keys())
#             placeholders = ", ".join(["%s"] * len(data))
#             insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
#
#             # 执行插入操作
#             with self.connection.cursor() as cursor:
#                 cursor.execute(insert_query, tuple(data.values()))
#                 self.connection.commit()
#                 print("数据插入成功！")
#         except pymysql.MySQLError as e:
#             print(f"插入数据失败：{e}")
#             self.connection.rollback()  # 回滚事务
#
#     def insert_relation(self, table_name, data):
#         try:
#             # 先检查主键是否存在
#             primary_key = list(data.keys())[0]  # 假设主键在第一个位置
#             last_key = list(data.keys())[-1]
#             primary_key_value = data[primary_key]
#             last_key_value = data[last_key]
#
#             # 生成检查主键是否存在的 SQL 查询
#             check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s AND {last_key} = %s "
#             with self.connection.cursor() as cursor:
#                 cursor.execute(check_query, (primary_key_value, last_key_value,))
#                 result = cursor.fetchone()
#
#                 if result[0] > 0:
#                     print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
#                     return  # 主键已存在，跳过插入操作
#
#             # 生成插入 SQL 语句
#             columns = ", ".join(data.keys())
#             placeholders = ", ".join(["%s"] * len(data))
#             insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
#
#             # 执行插入操作
#             with self.connection.cursor() as cursor:
#                 cursor.execute(insert_query, tuple(data.values()))
#                 self.connection.commit()
#                 print("数据插入成功！")
#         except pymysql.MySQLError as e:
#             print(f"插入数据失败：{e}")
#             self.connection.rollback()  # 回滚事务
#
#     def query_tables(self, query):
#         try:
#             with self.connection.cursor() as cursor:
#                 cursor.execute(query)
#                 result = cursor.fetchall()
#                 return result
#         except pymysql.MySQLError as e:
#             print(f"查询失败：{e}")
#             raise
#
#     def close(self):
#         """
#         关闭数据库连接
#         """
#         if self.connection:
#             self.connection.close()
#             print("数据库连接已关闭！")
# class Bucket:
#
#     def __init__(self, minio_address, minio_admin, minio_password):
#         # 通过ip 账号 密码 连接minio server
#         # Http连接 将secure设置为False
#         self.minioClient = Minio(endpoint=minio_address,
#                                  access_key=minio_admin,
#                                  secret_key=minio_password,
#                                  secure=False)
#
#         # 获取桶里某个目录下的所有目录和文件
#     def download_minio_object(self,bucket_name, object_name, local_file_path):
#         try:
#             self.minioClient.fget_object(bucket_name, object_name, local_file_path)
#         except Exception as e:
#             return e
# def get_sha1_hash(file_name):
#     shal_hash = hashlib.sha1(file_name.encode()).hexdigest()
#     return shal_hash
# def file_iterator(file_path, chunk_size=512):
#     """
#     文件生成器,防止文件过大，导致内存溢出
#     :param file_path: 文件绝对路径
#     :param chunk_size: 块大小
#     :return: 生成器
#     """
#     with open(file_path, mode='rb') as f:
#         while True:
#             c = f.read(chunk_size)
#             if c:
#                 yield c
#             else:
#                 break
# def deletezip(directory):
#     for filename in os.listdir(directory):
#         # 判断是否以 .zip 结尾
#         if filename.endswith('.zip'):
#             # 构建完整的文件路径
#             file_path = os.path.join(directory, filename)
#             # 删除文件
#             os.remove(file_path)
#
#
# def zipDir(dirpath, outFullName):
#     """
#     压缩指定文件夹
#     :param dirpath: 目标文件夹路径
#     :param outFullName: 压缩文件保存路径+xxxx.zip
#     :return: 无
#     """
#     zip = zipfile.ZipFile(outFullName, "a", zipfile.ZIP_DEFLATED)
#     for path, dirnames, filenames in os.walk(dirpath):
#         # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
#         fpath = dirpath
#         # print(dirpath.split("\\")[-1])
#         for filename in filenames:
#             zip.write(os.path.join(path, filename), os.path.join(fpath, filename))
#     zip.close()
# def get_similarity(str1,str2):
#     # 统计每个字符串中字符的频率
#     return 0.5
# def get_json_data(x,search_name,file_name=""):
#     temp={}
#     temp["node_id"] = int(x._id)
#     if file_name=="":
#         temp["name"] = x._properties["name"]
#     else:
#         temp["name"] = file_name
#     if search_name in x._properties["name"]:
#         temp["similarity"]=1*100
#     else:
#         temp["similarity"]=get_similarity(search_name,temp["name"])
#     return temp
# def search_content_to_file(content,db):
#     """
#     根据给定的 content 查询 entity_to_file 表中的 file_id，
#     并返回 file 表中所有匹配的行的信息。
#     """
#     query = """
#     SELECT ef.sim, f.*
#     FROM file AS f
#     JOIN entity_to_file AS ef ON f.id = ef.file_id
#     WHERE ef.entity = %s;
#     """
#     try:
#         with db.connection.cursor() as cursor:
#             cursor.execute(query, (content,))
#             result = cursor.fetchall()
#             return result
#     except pymysql.MySQLError as e:
#         print(f"查询失败：{e}")
#         raise
#
# def check_node_exists(name,node_id,userID,session):
#     result = session.run(
#         """
#         MATCH (n)
#         WHERE id(n) = $node_id and n.name=$name and n.user_id=$userID
#         RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
#         """,
#         node_id=int(node_id),
#         name=name,
#         userID=int(userID)
#     )
#     return result.single()[0]
#
# def check_node_strict(name, node_id, userID,session):
#         result = session.run(
#             """
#             MATCH (n)
#             WHERE id(n) = $node_id and n.name=$name and n.user_id=$userID
#             RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
#             """,
#             node_id=int(node_id),
#             name=name,
#             userID=int(userID)
#         )
#         return result.single()[0]
#         # return result.single()[0]  # 返回查询结果的第一个值
# def main(request):
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#
#     content = request.GET["name"]
#     userID =request.GET["userID"]
#     db.connect()
#     query_word = '''
#              MATCH (h)<-[r]-(t:hypernode) WHERE (h.name contains {arg_1} and (h:wikipage or h:baidupage or h:File or (h:Strict and h.user_id={arg_2})))
#     RETURN h,r,t LIMIT 30
#             '''
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#     results_place = session.run(query_word, parameters={"arg_1": content,"arg_2": int(userID)})
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#
#
#     count = 30
#     file_json_data=[]
#     for res in results_place:
#         count -= 1
#         # print("this is getinfoFile！！")
#         # print(res['t']._id)
#         if 'File' in list(set(res['h'].labels)):
#             node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#             temp = get_json_data(res['h'],content)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#         if 'File' in list(set(res['t'].labels)):
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#             temp = get_json_data(res['t'],content)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#         elif 'Strict' in list(set(res['h'].labels)):
#             node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#             temp = get_json_data(res['h'],content)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#         elif 'Strict' in list(set(res['t'].labels)):
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#             temp = get_json_data(res['t'],content)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#         elif 'baidupage' in list(set(res['h'].labels)):
#             node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#             file_name = node_name + '百度百科.html'
#             temp = get_json_data(res['h'],content,file_name)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#         elif 'baidupage' in list(set(res['t'].labels)):
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#             file_name = node_name + '百度百科.html'
#             temp = get_json_data(res['t'],content,file_name)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#         elif 'wikipage' in list(set(res['h'].labels)):
#             node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#             file_name = node_name + '维基百科.html'
#             temp = get_json_data(res['h'],content,file_name)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#         elif 'wikipage' in list(set(res['t'].labels)):
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#             file_name=node_name+'维基百科.html'
#             temp = get_json_data(res['t'],content,file_name)
#             file_json_data.append(temp)
#             # bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#         else:
#             count += 1
#         if count == 0:
#             break
#
#     # if count ==30:
#     #     message = {
#     #         "message": "文件为空",
#     #     }
#     #     return json.dumps(message)
#     db.connect()
#     result = search_content_to_file(content, db)
#
#     if len(result)<=0:
#         pass
#     else:
#         for i in result:
#             temp = {}
#             temp["node_id"] = i[1]
#             temp["name"] = i[2]
#             temp["similarity"] = (float)(int(i[0]*10000)/100)
#             print(i[1])
#             print(i[2])
#             result = check_node_exists(i[2],i[1],userID,session)
#             if ("1" in str(result)):
#                 file_json_data.append(temp)
#     # sorted_data=[]
#     sorted_data = sorted(file_json_data, key=lambda x: x['similarity'],reverse=True)
#     for i in sorted_data:
#         i["similarity"]=str(i["similarity"])+"%"
#     return json.dumps(sorted_data, ensure_ascii=False)