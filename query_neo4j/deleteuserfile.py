import os
from minio import Minio, InvalidResponseError, S3Error
from neo4j import GraphDatabase
import json
import zipfile
import hashlib
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
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
                return "success"
            except FileNotFoundError as err:
                print('upload_failed: ' + str(err))
                return 'upload_failed: ' + str(err)
            except S3Error as err:
                print("upload_failed:", err)
                return "upload_failed:"+str(err)

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
def search_file_in_Neo4j(tx,userID,nodeID):
    query_word = ("MATCH (h) WHERE (h.user_id=$name or h.user_id=$str_name or h.private=0) and id(h)=$nodeID RETURN h")
    result = tx.run(query_word, name=int(userID),nodeID=int(nodeID),str_name=str(userID))
    return result.data()
def delete_file_in_Neo4j(tx,userID,nodeID):
    query_word = ("MATCH (h) WHERE (h.user_id=$name or h.user_id=$str_name or h.private=0) and id(h)=$nodeID DETACH Delete h")
    try:
        tx.run(query_word, name=int(userID), nodeID=int(nodeID),str_name=str(userID))  # 执行Cypher查询
        return "Node deleted successfully"  # 如果查询成功，返回成功信息
    except Exception as e:
        return f"Error occurred: {e}"  # 如果查询失败，返回错误信息
def search_hypernode_in_Neo4j(tx,nodeID):
    query_word=("MATCH (n) WHERE id(n) = $nodeID \
MATCH (n)-[r]-(m:hypernode) \
WITH m  \
MATCH (m)-[r1]-(connectedNode) \
WHERE  'Strict' IN labels(connectedNode) \
RETURN COUNT(connectedNode) AS connectedCount,id(m) \
            "
                )
    result=tx.run(query_word, nodeID=int(nodeID))  # 执行Cypher查询
    connected_count = None
    hyper_id=None
    for record in result:
        connected_count = record["connectedCount"]  # 获取connectedCount的值
        hyper_id=record["id(m)"]
    print(f"Connected Count: {connected_count}")
    return connected_count,hyper_id  # 返回连接的计数
def delete_hypernode_in_neo4j(tx,nodeID):
    query_word = ("MATCH (h:hypernode) WHERE id(h)=$nodeID Detach Delete h")
    try:
        tx.run(query_word, nodeID=int(nodeID))  # 执行Cypher查询
        return "Node deleted successfully"  # 如果查询成功，返回成功信息
    except Exception as e:
        return f"Error occurred: {e}"  # 如果查询失败，返回错误信息

def delete_data_in_mysql(nodeId,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    delete_entity_query = """
        DELETE FROM entity_to_file 
        WHERE file_id = %s;
    """

    delete_file_query = """
        DELETE FROM file 
        WHERE id = %s;
    """

    # delete_xiaoqi_query="""
    #     UPDATE xiaoqi_data
    #     SET file_id = REPLACE(file_id, %s, '')
    #     WHERE file_id LIKE %s;
    # """ 这一版废弃了
    delete_xiaoqi_to_file_query=""" 
        DELETE FROM xiaoqi_to_file 
        WHERE file_id = %s;
    """

    delete_dir_to_file_query = """
            DELETE FROM dir_to_file 
            WHERE fileid = %s;
        """
    delete_dir_file_query = """
                DELETE FROM dir_file 
                WHERE file_id = %s;
            """
    try:
        with db.connection.cursor() as cursor:
            # 先删除 entity_to_file 表中的记录
            cursor.execute(delete_entity_query, (nodeId,))

            # 然后删除 file 表中的记录
            cursor.execute(delete_file_query, (nodeId,))

            cursor.execute(delete_xiaoqi_to_file_query, (int(nodeId,)))

            cursor.execute(delete_dir_to_file_query, (int(nodeId, )))

            cursor.execute(delete_dir_file_query, (int(nodeId, )))

            # 提交事务
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"删除失败：{e}")
        db.connection.rollback()  # 回滚事务
        raise
def main(request):
    nodeID = request.GET['node_ID']
    userID= request.GET['userID']
    nodeID = int(nodeID)
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session=driver.session()
    data=session.write_transaction(search_file_in_Neo4j, userID,nodeID)

    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    result = delete_data_in_mysql(nodeID, db)
    if len(data)<=0:
        return "不存在该文件！"
    else:
        file_data=data[0]["h"]
        minio_address = "114.213.232.140:19000"
        minio_admin = "minioadmin"
        minio_password = "minioadmin"

        bucket = Bucket(minio_address=minio_address,
                        minio_admin=minio_admin,
                        minio_password=minio_password)
        result=bucket.remove_object("kofiles",file_data["path"])
        if result=="success":
            hyper_count,hyper_id=session.write_transaction(search_hypernode_in_Neo4j,nodeID)#检查文件超点下是不是就这一个文件，是的话就把超点也删掉
            data = session.write_transaction(delete_file_in_Neo4j, userID, nodeID)
            if (hyper_count!=None and int(hyper_count)==1):
                if hyper_id!=None:
                    data1=session.write_transaction(delete_hypernode_in_neo4j, hyper_id)
            result = delete_data_in_mysql(nodeID, db)
            if (data=="Node deleted successfully"):
                return "文件删除成功"
            else:
                return data
        else:
            return result