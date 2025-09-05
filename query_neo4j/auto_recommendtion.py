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
import pymysql
import ctypes
from query_neo4j.search_urls import search_urls
import os
import re
import time
import json
import requests
from typing import Optional, Dict
from django.http import HttpRequest  # 添加这行导入
from django.http import HttpResponse as DjangoHttpResponse  # 如果需要

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

    def create_one_bucket(self, bucket_name):
        # 创建桶(调用make_bucket api来创建一个桶)
        """
        桶命名规则：小写字母，句点，连字符和数字 允许使用 长度至少3个字符
        使用大写字母、下划线等会报错
        """
        try:
            # bucket_exists：检查桶是否存在
            if self.minioClient.bucket_exists(bucket_name=bucket_name):
                print("该存储桶已经存在")
            else:
                self.minioClient.make_bucket(bucket_name=bucket_name)
                print(f"{bucket_name}桶创建成功")
        except InvalidResponseError as err:
            print(err)

    def remove_one_bucket(self, bucket_name):
        # 删除桶(调用remove_bucket api来创建一个存储桶)
        try:
            if self.minioClient.bucket_exists(bucket_name=bucket_name):
                self.minioClient.remove_bucket(bucket_name)
                print("删除存储桶成功")
            else:
                print("该存储桶不存在")
        except InvalidResponseError as err:
            print(err)

    def upload_file_to_bucket(self, bucket_name, file_name, file_path):
        """
        将文件上传到bucket
        :param bucket_name: minio桶名称
        :param file_name: 存放到minio桶中的文件名字(相当于对文件进行了重命名，可以与原文件名不同)
                            file_name处可以创建新的目录(文件夹) 例如 /example/file_name
                            相当于在该桶中新建了一个example文件夹 并把文件放在其中
        :param file_path: 本地文件的路径
        """
        # 桶是否存在 不存在则新建
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if not check_bucket:
            self.minioClient.make_bucket(bucket_name)

        try:
            self.minioClient.fput_object(bucket_name=bucket_name,
                                         object_name=file_name,
                                         file_path=file_path)
        except FileNotFoundError as err:
            print('upload_failed: ' + str(err))
        except S3Error as err:
            print("upload_failed:", err)

    def download_file_from_bucket(self, bucket_name, minio_file_path, download_file_path):
        """
        从bucket下载文件
        :param bucket_name: minio桶名称
        :param minio_file_path: 存放在minio桶中文件名字
                            file_name处可以包含目录(文件夹) 例如 /example/file_name
        :param download_file_path: 文件获取后存放的路径
        """
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            try:
                self.minioClient.fget_object(bucket_name=bucket_name,
                                             object_name=minio_file_path,
                                             file_path=download_file_path)
                return 1
            except FileNotFoundError as err:
                print('download_failed: ' + str(err))
                return 0
            except S3Error as err:
                print("download_failed:", err)
                return 0

    def remove_object(self, bucket_name, object_name):
        """
        从bucket删除文件
        :param bucket_name: minio桶名称
        :param object_name: 存放在minio桶中的文件名字
                            object_name处可以包含目录(文件夹) 例如 /example/file_name
        """
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            try:
                self.minioClient.remove_object(bucket_name=bucket_name,
                                               object_name=object_name)
            except FileNotFoundError as err:
                print('upload_failed: ' + str(err))
            except S3Error as err:
                print("upload_failed:", err)

    # 获取所有的桶
    def get_all_bucket(self):
        buckets = self.minioClient.list_buckets()
        ret = []
        for _ in buckets:
            ret.append(_.name)
        return ret

    # 获取一个桶中的所有一级目录和文件
    def get_list_objects_from_bucket(self, bucket_name):
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            # 获取到该桶中的所有目录和文件
            objects = self.minioClient.list_objects(bucket_name=bucket_name)
            ret = []
            for _ in objects:
                ret.append(_.object_name)
            return ret

    # 获取桶里某个目录下的所有目录和文件
    def get_list_objects_from_bucket_dir(self, bucket_name, dir_name):
        # 桶是否存在
        check_bucket = self.minioClient.bucket_exists(bucket_name)
        if check_bucket:
            # 获取到bucket_所name桶中的dir_name下的有目录和文件
            # prefix 获取的文件路径需包含该前缀
            objects = self.minioClient.list_objects(bucket_name=bucket_name,
                                                    prefix=dir_name,
                                                    recursive=True)
            ret = []
            for obj in objects:
                object_name = obj.object_name
                # 获取对象的内容
                content = self.minioClient.get_object(bucket_name=bucket_name,
                                                      object_name=object_name)
                ret.append(content.data.decode())
            return ret


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
    flag=False
    for path, dirnames, filenames in os.walk(dirpath):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = dirpath
        # print(dirpath.split("\\")[-1])
        # zip.write(path, os.path.relpath(path, os.path.dirname(dirpath)))
        for filename in filenames:
            flag=True
            print(str(path)+" "+str(dirnames)+" "+str(filenames))
            zip.write(os.path.join(path, filename), os.path.join(fpath, filename))
    if flag==False:
        print(dirpath)
        zip.write(dirpath, dirpath)
        # zip.writestr(os.path.basename(dirpath) + '/.keep', '')
    zip.close()

def search_content_to_file(content, db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    query = """
    SELECT ef.sim, f.*
    FROM file AS f
    JOIN entity_to_file AS ef ON f.id = ef.file_id
    WHERE ef.entity = %s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (content,))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise


def check_node_exists(name, node_id, userID, session):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id and n.name=$name and n.user_id=$userID
        RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
        """,
        node_id=int(node_id),
        name=name,
        userID=int(userID)
    )
    return result.single()[0]


def check_node_strict(name, node_id, userID, session):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id and n.name=$name and n.user_id=$userID
        RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
        """,
        node_id=int(node_id),
        name=name,
        userID=int(userID)
    )
    return result.single()[0]
    # return result.single()[0]  # 返回查询结果的第一个值


def chuli_file_dire(under_dire, under_dire_to_file, session):
    for i in under_dire:
        query_word = '''
            MATCH (h:KOCategory)<-[r]-(t)
    WHERE id(h) = {arg_1} AND t:KOCategory
    RETURN h AS h, r AS r, t AS t, NULL AS fullPath, 0 AS pathLength
    LIMIT 100

    UNION ALL

    MATCH (h:KOCategory)
    WHERE id(h) = {arg_1}
    CALL apoc.path.expandConfig(h, {
        relationshipFilter: "<edge",
        labelFilter: ">KOCategory",
        maxLevel: 5,
        limit: 300,
        direction: "incoming"
    })
    YIELD path
    WITH h, path, relationships(path) AS rels  // 保留 h 和 path
    UNWIND rels AS rel
    WITH h, rel, startNode(rel) AS t, endNode(rel) AS ko, path, length(path) AS pathLength  // 计算路径长度
    WHERE ko:KOCategory AND t <> h  // 确保 t 不是 h
    RETURN h AS h, rel AS r, ko AS t, path AS fullPath, pathLength
    ORDER BY pathLength DESC  // 按路径长度降序排列
    LIMIT 1  // 仅返回最长的路径
        '''
        results_place = session.run(query_word, parameters={"arg_1": int(i)})
        dire_relation = []
        for res in results_place:
            if res["fullPath"] is not None:
                for edge in res["fullPath"]:
                    dire_relation.append((edge.start_node.id, edge.end_node.id))
                    # print(node)
        for pair in dire_relation:
            if int(pair[0]) not in under_dire_to_file:
                temp = []
            else:
                temp = under_dire_to_file[int(pair[0])]
            temp.append(int(pair[1]))
            under_dire_to_file[int(pair[0])] = temp
    return under_dire_to_file


def check_node_name(session, i):
    node1 = id_get_node(session, i)
    if "wikipage" in node1.labels:
        node_name = node1["name"] + "_维基.html"
    elif "baidupage" in node1.labels:
        node_name = node1["name"] + "_百度.html"
    else:
        node_name = node1["name"]
    return node_name


def id_get_node(session, id):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id
        RETURN n
        """,
        node_id=int(id)
    )
    # print(id)
    return result.single()["n"]

def compress_file(file_path, zip_path):
    # 检查指定的文件是否存在
    if not os.path.isfile(file_path):
        print(f"文件 {file_path} 不存在。")
        return

    # 创建一个压缩包并将文件添加到其中
    with zipfile.ZipFile(zip_path, 'a') as zipf:
        zipf.write(file_path, os.path.basename(file_path))
        print(f"文件 {file_path} 已成功压缩到 {zip_path}。")

def search_fileId_in_mysql(db,fileid,userID):
    query = """
SELECT
    de.dir_private,
    f.name
FROM
    dir_file AS df
JOIN
    dir_entity AS de ON df.dir_id = de.id and de.userID=%s
JOIN
    file AS f ON df.file_id = f.id
WHERE
    df.file_id = %s
        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID),int(fileid),))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def search_dire_in_mysql(db,content,userid):
    query = """
    SELECT
    de.dir_private
FROM
    xiaoqi_new AS xn
JOIN
    dir_entity AS de ON xn.xiaoqi_id = de.entity_id and de.userid=%s
WHERE
    xn.xiaoqi_name=%s
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userid),str(content)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def search_nodeIds(userID,content):
    db = MySQLDatabase(
            host="114.213.234.179",
            user="koroot",  # 替换为您的用户名
            password="DMiC-4092",  # 替换为您的密码
            database="db_hp"  # 替换为您的数据库名
        )
    db.connect()
    query = """
    SELECT
            xn.xiaoqi_id,
        de.id AS directory_id,
        de.dir_private,
        x.file_id AS xiaoqi_file_id,
            f.name AS file_name,
            f.private As file_private
    FROM
        xiaoqi_new xn
    JOIN
        xiaoqi_to_file x ON xn.xiaoqi_id = x.xiaoqi_id
    JOIN
        dir_file df ON x.file_id = df.file_id
    JOIN
        dir_entity de ON df.dir_id = de.id and de.entity_id=xn.xiaoqi_id and de.userid=%s
    JOIN
        file f ON x.file_id = f.id and (f.private=0 or f.userid=%s)
    WHERE
        xn.xiaoqi_name = %s;
        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID), int(userID), str(content)))
            result = cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    nodes=[]
    for i in result:
        nodes.append(int(i[3]))
    return nodes

#
# def test(content, userID):
#     # 创建以 content 命名的主文件夹路径
#     print("##############################################")
#     base_dir = os.path.join('D:\data\Auto_recommendtion', content)
#     if not os.path.exists(base_dir):
#         os.makedirs(base_dir)
#
#     zip_new_file = os.path.join(base_dir, content + ".zip")
#     print(zip_new_file)
#
#     # 清理旧数据（如果逻辑需要）
# #     deletezip(base_dir)
#
#     # 数据库与Bucket连接
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",
#         password="DMiC-4092",
#         database="db_hp"
#     )
#     db.connect()
#
#     bucket = Bucket(
#         minio_address="114.213.232.140:19000",
#         minio_admin="minioadmin",
#         minio_password="minioadmin"
#     )
#
#     node_id = search_nodeIds(userID, content)
#
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#
#     all_dire = []
#
#     # 创建所有子目录（基于MySQL目录信息）
#     result_dire = search_dire_in_mysql(db, content, userID)
#     for i in result_dire:
#         dir_path = os.path.join(base_dir, i[0])
#         if not os.path.exists(dir_path):
#             os.makedirs(dir_path)
#         if dir_path not in all_dire:
#             all_dire.append(dir_path)
#
#     address = []
#     for i in node_id:
#         result = search_fileId_in_mysql(db, i, userID)
#         for res in result:
#             dire_name = res[0]
#             node_path = "bb/" + res[1]
#             target_dir = os.path.join(base_dir, dire_name)
#             if not os.path.exists(target_dir):
#                 os.makedirs(target_dir)
#             if target_dir not in all_dire:
#                 all_dire.append(target_dir)
#
#             filename = node_path.split('/')[-1]
#             full_path = os.path.join(target_dir, filename)
#             k = bucket.download_file_from_bucket('kofiles', node_path, full_path)
#             if k == 1:
#                 address.append(full_path)
#
#     return address

def test(content, userID):
    print(f"[DEBUG] 开始处理: {content}, 用户ID: {userID}")

    base_dir = os.path.join('D:\data\Auto_recommendtion', content)
    print(f"[DEBUG] 基础目录: {base_dir}")

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"[DEBUG] 创建目录: {base_dir}")
    else:
        print(f"[DEBUG] 目录已存在: {base_dir}")

    # 数据库连接
    print("[DEBUG] 连接数据库...")
    db = MySQLDatabase(host="114.213.234.179", user="koroot", password="DMiC-4092", database="db_hp")
    db.connect()

    # 获取节点ID
    print("[DEBUG] 查询节点ID...")
    node_id = search_nodeIds(userID, content)
    print(f"[DEBUG] 找到节点ID: {node_id}")

    # 创建目录
    result_dire = search_dire_in_mysql(db, content, userID)
    print(f"[DEBUG] 数据库目录结果: {result_dire}")

    for i in result_dire:
        dir_path = os.path.join(base_dir, i[0])
        print(f"[DEBUG] 处理目录: {dir_path}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"[DEBUG] 创建子目录: {dir_path}")

    # 下载文件
    address = []
    for i in node_id:
        print(f"[DEBUG] 处理节点 {i}...")
        result = search_fileId_in_mysql(db, i, userID)
        print(f"[DEBUG] 文件查询结果: {result}")

        for res in result:
            dire_name = res[0]
            node_path = "bb/" + res[1]
            target_dir = os.path.join(base_dir, dire_name)
            filename = node_path.split('/')[-1]
            full_path = os.path.join(target_dir, filename)

            print(f"[DEBUG] 下载: {node_path} -> {full_path}")

            k = bucket.download_file_from_bucket('kofiles', node_path, full_path)
            if k == 1:
                print(f"[DEBUG] 下载成功: {full_path}")
                address.append(full_path)
            else:
                print(f"[ERROR] 下载失败: {node_path}")
                # 检查MinIO中文件是否存在
                try:
                    objects = bucket.minioClient.list_objects('kofiles', prefix=node_path)
                    for obj in objects:
                        print(f"[DEBUG] MinIO中找到文件: {obj.object_name}")
                except Exception as e:
                    print(f"[ERROR] 检查MinIO文件失败: {e}")

    print(f"[DEBUG] 最终文件列表: {address}")
    return address



##########################################################################################################################################################################################
# print(test("吴信东1",6000622))#第一个参数代表消歧实体名称，对应xiaoqi_new表中的name，第二个是用户ID（这个到时候前端也会提供）
#第一个模块：前端传入 实体名 和 对应ID,调用test返回下载在本地文件的地址（test 函数通过 数据库查询 → 本地目录创建 → MinIO 文件下载 的流程）
#######################################
#######################################

#######################################
import os
import re
import pdfplumber
from docx import Document
from bs4 import BeautifulSoup
import requests
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import chardet


def read_file_content(filepath: str) -> str:
    try:
        if filepath.endswith(".pdf"):
            with pdfplumber.open(filepath) as pdf:
                return "\n".join(page.extract_text() or '' for page in pdf.pages)
        elif filepath.endswith(".docx"):
            return "\n".join([para.text for para in Document(filepath).paragraphs])
        elif filepath.endswith(".doc"):
            print(f"[跳过] 不支持 .doc 文件: {filepath}")
            return ''
        elif filepath.endswith((".html", ".htm", ".shtml")):
            with open(filepath, "rb") as f:
                raw_data = f.read()
                encoding = chardet.detect(raw_data)["encoding"] or "utf-8"
                text = raw_data.decode(encoding, errors="ignore")
                soup = BeautifulSoup(text, "html.parser")
                return soup.get_text(separator='\n', strip=True)
        else:
            print(f"[跳过] 不支持的文件类型: {filepath}")
            return ''
    except Exception as e:
        print(f"[读取失败] {filepath} 错误: {str(e)}")
        return ''



# ==== 构造提示词 ====
def generate_prompt(person_name: str, raw_text: str, max_keywords: int = 15) -> str:
    return f"""你是一个信息检索专家，请从以下原始文本中，**逐字提取出**最多 {max_keywords} 个与人物 [{person_name}] 高度相关、便于网络搜索的关键词短语。

# 请严格遵守以下约束：

1. 所有关键词必须**逐字出现在原文中**，不能编造、概括或扩展；
2. 优先提取以下两类内容：
   - **机构/单位**：如“清华大学”“自动化系”
   - **较大的研究方向**：如“人工智能”“生物医学”“计算机科学”
3. 如果原文中某些机构为长结构（如“安徽大学计算机科学与技术学院”），请**合理拆分为多个关键词**，例如：
   - “安徽大学”
   - “计算机科学与技术学院”

# 输出格式：
- 只需要输出关键词
- 中文分号分隔（使用中文分号“；”）
- 每条关键词应具有独立的搜索价值
- 所有关键词必须来自原文，且不超过 {max_keywords} 个

原始文本如下：
{raw_text}
"""

# def generate_prompt(entity_name: str, raw_text: str, max_keywords: int = 15) -> str:
#     return f"""你是一个信息检索专家，请从以下原始文本中，**逐字提取出**最多 {max_keywords} 个与【{entity_name}】实体高度相关、便于网络搜索的关键词短语。
#
# # 实体类型说明：
# - 人物实体：如"李白"、"爱因斯坦"等具体人物
# - 概念实体：如"数据挖掘"、"机器学习"等抽象概念
# - 机构实体：如"合肥工业大学"、"中国科学院"等组织机构
# - 地点实体：如"北京市"、"黄山"等地理名称
# - 事件实体：如"第二次世界大战"、"北京奥运会"等历史事件
# - 产品实体：如"iPhone"、"微信"等具体产品
#
# # 请严格遵守以下约束：
#
# 1. 所有关键词必须**逐字出现在原文中**，不能编造、概括或扩展；
# 2. 根据目标实体的类型，优先提取以下相关内容：
#    - 对于**人物实体**：提取所属机构、职务、研究领域、成就等
#    - 对于**概念实体**：提取相关技术、应用领域、理论基础等
#    - 对于**机构实体**：提取所在地点、下属部门、重点学科、知名成果等
#    - 对于**地点实体**：提取地理位置、特色产业、历史文化等
#    - 对于**事件实体**：提取发生时间、参与方、影响范围等
#    - 对于**产品实体**：提取开发商、功能特点、应用场景等
# 3. 提取具有独立搜索价值的关键词片段，例如：
#    - "安徽大学计算机科学与技术学院" → "安徽大学"；"计算机科学与技术学院"
#    - "深度学习神经网络模型" → "深度学习"；"神经网络"
#
# # 输出格式：
# - 只需要输出关键词
# - 中文分号分隔（使用中文分号"；"）
# - 每条关键词应具有独立的搜索价值
# - 所有关键词必须来自原文，且不超过 {max_keywords} 个
#
# 原始文本如下：
# {raw_text}
# """


def call_big_model(text: str, person_name: str, max_keywords: int, api_key: str) -> Optional[str]:
    """调用大模型 API
    参数:
        text: 要分析的文本
        person_name: 人物名称
        max_keywords: 最大关键词数量
        api_key: API密钥
    """
    API_URL = "https://api.siliconflow.cn/v1/chat/completions"
    MODEL_NAME = "deepseek-ai/DeepSeek-V3"
    API_KEY = "sk-uwokmhxknecolbmvcrnfstfrcqzjeuekvnxfoghzrakeqybw"  # API Key
    HEADERS = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    print("##############################################")
    prompt = generate_prompt(person_name, text, max_keywords)
    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5,
        "max_tokens": 1024
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"[API请求失败]: {e}")
        return None

#### 读取是地址列表
def merge_all_texts(file_paths: list, max_workers: int = 10) -> str:
    """
    直接合并文件列表内容（跳过文件夹扫描）
    :param file_paths: 文件绝对路径列表
    :param max_workers: 并发线程数
    :return: 合并后的文本
    """

    def read_file(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return path, f.read()  # 返回（路径，内容）用于错误追踪
        except Exception as e:
            print(f"⚠️ 读取失败 {os.path.basename(path)}: {str(e)[:50]}...")
            return path, None

    valid_contents = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_file, path): path for path in file_paths}
        for future in as_completed(futures):
            path, content = future.result()
            if content and content.strip():
                valid_contents.append(content)

    return "\n\n".join(valid_contents).strip()

def clean_entity_name(entity: str) -> str:
    """
    自动去除实体名末尾的数字
    示例:
        "吴信东1" -> "吴信东"
        "实体123" -> "实体"
        "无数字" -> "无数字"
    """
    return re.sub(r'\d+$', '', entity)



def auto_recommendtion(request: HttpRequest) -> dict:
    """
    主推荐函数，接收 Django 请求对象
    返回包含结果或错误信息的字典
    """
    name = request.GET.get("name", "").strip()
    user_id = request.GET.get("user_id", "").strip()
    api_key = request.GET.get("api_key", "sk-uwokmhxknecolbmvcrnfstfrcqzjeuekvnxfoghzrakeqybw")
    max_keywords = int(request.GET.get("max_keywords", 5))
    num_pages_to_crawl = int(request.GET.get("num_pages_to_crawl", 30))
    print("##############################################")
    if not name or not user_id:
        return {"status": "error", "message": "Missing required parameters: name or user_id"}

    try:
        folder = test(name, int(user_id))
        entity_name = clean_entity_name(name)

        # 读取文件内容
        print("正在并行读取文件内容...")
        start_time = time.time()
        full_text = merge_all_texts(folder)
        end_time = time.time()
        print(f"✅ 处理完成！耗时: {end_time - start_time:.2f}秒")

        keywords = []
        if full_text:
            limited_text = full_text[:10000]
            start_time = time.time()
            result = call_big_model(
                text=limited_text,
                person_name=entity_name,
                max_keywords=max_keywords,
                api_key=api_key
            )
            end_time = time.time()
            print(f"✅ 关键词提取完成！耗时: {end_time - start_time:.2f}秒")

            if result:
                keywords = [kw.strip() for kw in re.split(r"[；;]", result) if kw.strip()]
            else:
                print("⚠️ 无法提取关键词，默认使用实体名作为查询。")  # ✅ 不中断
        else:
            print("⚠️ 文件内容为空，默认使用实体名作为查询。")  # ✅ 不中断

        # ✅ 即使关键词为空，也可以继续进行，只用实体名搜索
        combined_query = f"{entity_name} {keywords[0]} {keywords[1]}" if len(keywords) >= 2 else entity_name

        # 创建模拟请求对象
        mock_request = type('MockRequest', (), {'GET': {
            "name": combined_query,
            "num_pages_to_crawl": num_pages_to_crawl,
            "userID": user_id,
            "xiaoqi_name": entity_name,
            "enable_deduplication": "true"
        }})

        search_result = search_urls(mock_request)
        return {
            "status": "success",
            "data": search_result,
            "keywords": keywords,
            "combined_query": combined_query
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}





