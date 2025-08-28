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
# def zipDir(dirpath, outFullName):
#     """
#     压缩指定文件夹，包括空文件夹
#     :param dirpath: 目标文件夹路径
#     :param outFullName: 压缩文件保存路径+xxxx.zip
#     :return: 无
#     """
#     with zipfile.ZipFile(outFullName, 'a', zipfile.ZIP_DEFLATED) as zipf:
#         for root, dirs, files in os.walk(dirpath):
#             # 添加当前文件夹
#             # zipf.write(root, os.path.relpath(root, os.path.dirname(dirpath)))
#             # 添加文件到压缩文件
#             for file in files:
#                 zipf.write(os.path.join(root, file),
#                             os.path.relpath(os.path.join(root, file), os.path.dirname(dirpath)))
#
#         # 确保添加空文件夹
#         if not files and not dirs:
#             zipf.write(dirpath, os.path.relpath(dirpath, os.path.dirname(dirpath)))

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


def chuli_mysql(db, content, userID, session, bucket, zip_file1, zip_file2, zip_file3, zip_file4, count,
                under_dire_to_file, under_dire):
    db.connect()
    result = search_content_to_file(content, db)
    all_file_id = []
    if len(result) <= 0:
        return count, under_dire_to_file, under_dire
    else:
        for i in result:
            temp = {}
            temp["node_id"] = i[1]
            temp["name"] = i[2]
            temp["similarity"] = (float)(int(i[0] * 10000) / 100)
            # print(i[1])
            # print(i[2])
            result = check_node_exists(i[2], i[1], userID, session)
            if ("1" in str(result)):
                all_file_id.append(i[1])

        for j in all_file_id:
            query_word = '''
                         MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
                RETURN h,r,t,r1,t1 LIMIT 300
                        '''
            results_place = session.run(query_word, parameters={"arg_1": int(j)})
            for res in results_place:
                count -= 1
                if 'File' in list(set(res['h'].labels)):
                    node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
                elif 'File' in list(set(res['t'].labels)):
                    node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
                elif 'Strict' in list(set(res['h'].labels)):
                    node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
                    # print(node_path)
                elif 'Strict' in list(set(res['t'].labels)):
                    node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
                elif 'baidupage' in list(set(res['h'].labels)):
                    node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file2 + node_name + '.html')
                elif 'baidupage' in list(set(res['t'].labels)):
                    node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file2 + node_name + '.html')
                elif 'wikipage' in list(set(res['h'].labels)):
                    node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
                    node_path = node_path + "/" + node_name + ".html"
                    # print(node_path)
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_name + '.html')
                elif 'wikipage' in list(set(res['t'].labels)):
                    node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
                    node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
                    node_path = node_path + "/" + node_name + ".html"
                    # print(node_path)
                    bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_name + '.html')
                else:
                    count += 1
                # if count == 0:
                #     break
            query_word = '''
                            MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
                RETURN h,r,t,r1,t1 LIMIT 300
                           '''
            results_place = session.run(query_word, parameters={"arg_1": int(j)})
            for res in results_place:
                # print(res)
                if (res['t1'] is not None):
                    if int(res['t1'].id) not in under_dire_to_file:
                        temp = []
                        under_dire.append(int(res["t1"].id))
                    else:
                        temp = under_dire_to_file[int(res["t1"].id)]
                    temp.append(int(res["h"].id))
                    under_dire_to_file[int(res["t1"].id)] = temp
        return count, under_dire_to_file, under_dire


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
def search_dire_in_mysql(db,content,xiaoqi_id,userid):
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
            cursor.execute(query, (int(userid),str(content)+str(xiaoqi_id)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def main(request):
    xiaoqi_id =request.GET["xiaoqi_id"]
    content = request.GET["content"]
    node_id = request.GET["node_ID"]
    userID = request.GET["userID"]

    test_type = request.GET.get("type")

    if (test_type!="0"):
        print("error!!")
        return old_method(xiaoqi_id,content,node_id,userID,test_type)

    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    minio_address = "114.213.232.140:19000"
    minio_admin = "minioadmin"
    minio_password = "minioadmin"

    bucket = Bucket(minio_address=minio_address,
                    minio_admin=minio_admin,
                    minio_password=minio_password)
    zip_new_file = 'F:\data\\' + content+str(xiaoqi_id)+ ".zip"
    print(zip_new_file)
    deletezip('F:\data\\')

    nodeIDs = str(node_id).split(",")


    # 检查第一个地址是否存在
    under_dire_to_file = {}
    under_dire = []
    count = 30
    file_id_list = []
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session = driver.session()
    print(nodeIDs)
    all_dire=[]

    result_dire=search_dire_in_mysql(db,content,xiaoqi_id,userID)
    for i in result_dire:
        zip_file1 = 'F:\data\\' + i[0] + "\\"
        if not os.path.exists(zip_file1):
            # 如果不存在,创建该目录
            os.makedirs(zip_file1)
        if (zip_file1 not in all_dire):
            all_dire.append(zip_file1)
        # invisible_file_path = os.path.join(zip_file1, ".123")
        # # 创建不可见文件
        # with open(invisible_file_path, 'w') as f:
        #     f.write('')  # 写入空内容
        # os.system(f'attrib +h "{invisible_file_path}"')
    if nodeIDs==['']:
        zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
        zip.close()
        for i in all_dire:
            zipDir(i, zip_new_file)
            shutil.rmtree(i)
        response = StreamingHttpResponse(file_iterator(zip_new_file))
        response['content_type'] = "application/zip"
        response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
        response['message'] = "上传成功"
        return response
    for i in nodeIDs:
        result=search_fileId_in_mysql(db,i,userID)
        for res in result:
            dire_name = res[0]
            node_path = "bb/"+res[1]
            zip_file1='F:\data\\' + dire_name+ "\\"
            if not os.path.exists(zip_file1):
                # 如果不存在,创建该目录
                os.makedirs(zip_file1)
            if (zip_file1 not in all_dire):
                all_dire.append(zip_file1)
            bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_path.split('/')[-1])
    print(all_dire)
    # print("222222" + str(file_id_list))
    try:
        zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
        zip.close()
        for i in all_dire:
            zipDir(i, zip_new_file)
            shutil.rmtree(i)
        # for i in all_dire:
        #     zipDir(i, zip_new_file)
        # shutil.rmtree(zip_file5)
        response = StreamingHttpResponse(file_iterator(zip_new_file))
        response['content_type'] = "application/zip"
        response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
        response['message'] = "上传成功"
        return response
    except PermissionError:
        return '文件权限错误，无法访问', 403
    except Exception as e:
        return f'发生错误：{str(e)}', 500














def old_method(xiaoqi_id,content,node_id,userID,test_type):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    minio_address = "114.213.232.140:19000"
    minio_admin = "minioadmin"
    minio_password = "minioadmin"

    bucket = Bucket(minio_address=minio_address,
                    minio_admin=minio_admin,
                    minio_password=minio_password)
    zip_new_file = 'F:\data\\' + content+str(xiaoqi_id)+ ".zip"
    print(zip_new_file)
    deletezip('F:\data\\')

    nodeIDs = str(node_id).split(",")
    if nodeIDs==['']:
        return "文件为空！！！"

    # 检查第一个地址是否存在


    under_dire_to_file = {}
    under_dire = []
    count = 30
    file_id_list = []
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session = driver.session()
    print(nodeIDs)
    all_dire=[]
    for i in nodeIDs:
        if test_type=="Test":
            query_word = '''
                     MATCH (t2:hypernode)-[r1]-(t1:Test)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
             RETURN t,r1,t1,r2,t2 LIMIT 100
                    '''
        elif test_type=="Test1":
            query_word = '''
                MATCH (t2:hypernode)-[r1]-(t1:Test1)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
                RETURN t,r1,t1,r2,t2 LIMIT 100
                                '''
        elif test_type=="Test2":
            query_word = '''
                MATCH (t2:hypernode)-[r1]-(t1:Test2)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
                RETURN t,r1,t1,r2,t2 LIMIT 100
                '''
        else:
            query_word = '''
                     MATCH (t:Strict) WHERE (id(t)={arg_1})
             RETURN t LIMIT 100
                                '''
        results_place = session.run(query_word, parameters={"arg_1": int(i)})

        for res in results_place:
            print(res)
            node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
            node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
            dire_name = zhconv.convert(res['t1']._properties["name"], 'zh-cn')
            zip_file1='F:\data\\' + dire_name+ "\\"
            if not os.path.exists(zip_file1):
                # 如果不存在,创建该目录
                os.makedirs(zip_file1)
            if (zip_file1 not in all_dire):
                all_dire.append(zip_file1)
            bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_path.split('/')[-1])

    # print("222222" + str(file_id_list))
    try:
        zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
        zip.close()
        for i in all_dire:
            zipDir(i, zip_new_file)
            shutil.rmtree(i)
        # shutil.rmtree(zip_file5)
        response = StreamingHttpResponse(file_iterator(zip_new_file))
        response['content_type'] = "application/zip"
        response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
        response['message'] = "上传成功"
        return response
    except PermissionError:
        return '文件权限错误，无法访问', 403
    except Exception as e:
        return f'发生错误：{str(e)}', 500
# def main(request):
#     xiaoqi_id =request.GET["xiaoqi_id"]
#     content = request.GET["content"]
#     node_id = request.GET["node_ID"]
#     userID = request.GET["userID"]
#
#     test_type=request.GET["type"]
#
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     zip_new_file = 'F:\data\\' + content+str(xiaoqi_id)+ ".zip"
#     print(zip_new_file)
#     deletezip('F:\data\\')
#
#     nodeIDs = str(node_id).split(",")
#     if nodeIDs==['']:
#         return "文件为空！！！"
#
#     # 检查第一个地址是否存在
#
#
#     under_dire_to_file = {}
#     under_dire = []
#     count = 30
#     file_id_list = []
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#     print(nodeIDs)
#     all_dire=[]
#     for i in nodeIDs:
#         if test_type=="Test":
#             query_word = '''
#                      MATCH (t2:hypernode)-[r1]-(t1:Test)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#              RETURN t,r1,t1,r2,t2 LIMIT 100
#                     '''
#         elif test_type=="Test1":
#             query_word = '''
#                 MATCH (t2:hypernode)-[r1]-(t1:Test1)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#                 RETURN t,r1,t1,r2,t2 LIMIT 100
#                                 '''
#         elif test_type=="Test2":
#             query_word = '''
#                 MATCH (t2:hypernode)-[r1]-(t1:Test2)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#                 RETURN t,r1,t1,r2,t2 LIMIT 100
#                 '''
#         else:
#             query_word = '''
#                      MATCH (t:Strict) WHERE (id(t)={arg_1})
#              RETURN t LIMIT 100
#                                 '''
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#
#         for res in results_place:
#             print(res)
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#             dire_name = zhconv.convert(res['t1']._properties["name"], 'zh-cn')
#             zip_file1='F:\data\\' + dire_name+ "\\"
#             if not os.path.exists(zip_file1):
#                 # 如果不存在,创建该目录
#                 os.makedirs(zip_file1)
#             if (zip_file1 not in all_dire):
#                 all_dire.append(zip_file1)
#             bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_path.split('/')[-1])
#
#     print("222222" + str(file_id_list))
#     try:
#         zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#         zip.close()
#         for i in all_dire:
#             zipDir(i, zip_new_file)
#             shutil.rmtree(i)
#         # shutil.rmtree(zip_file5)
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         response['message'] = "上传成功"
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500
# def main(request):
#     content = request.GET["content"]
#     node_id = request.GET["node_ID"]
#     userID = request.GET["userID"]
#
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     zip_new_file = 'F:\data\\' + content + ".zip"
#     zip_file1 = 'F:\data\\wikipage\\'
#     zip_file2 = 'F:\data\\baidupage\\'
#     zip_file3 = 'F:\data\\File\\'
#     zip_file4 = 'F:\data\\Strict\\'
#     zip_file5 = 'F:\\' +"1"+"\\"
#     deletezip('F:\data\\')
#
#     nodeIDs = str(node_id).split(",")
#     if nodeIDs==['']:
#         return "文件为空！！！"
#
#     # 检查第一个地址是否存在
#     if not os.path.exists(zip_file1):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file1)
#
#     # 检查第二个地址是否存在
#     if not os.path.exists(zip_file2):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file2)
#
#     if not os.path.exists(zip_file3):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file3)
#
#     if not os.path.exists(zip_file4):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file4)
#
#     if not os.path.exists(zip_file5):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file5)
#
#     under_dire_to_file = {}
#     under_dire = []
#     count = 30
#     file_id_list = []
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#     for i in nodeIDs:
#         query_word = '''
#                  MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
#         RETURN h,r,t,r1,t1 LIMIT 300
#                 '''
#
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#
#
#         for res in results_place:
#             count -= 1
#             # print(res)
#             if 'File' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'File' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#             elif 'Strict' in list(set(res['h'].labels)):
#                 # print(1)
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'Strict' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#             elif 'baidupage' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'baidupage' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#             elif 'wikipage' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#                 node_path = node_path + "/" + node_name + ".html"
#                 # print(node_path)
#                 bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'wikipage' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#                 node_path = node_path + "/" + node_name + ".html"
#                 # print(node_path)
#                 bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#
#             else:
#                 count += 1
#             # if count == 0:
#             #     break
#
#     print("222222" + str(file_id_list))
#     for i in nodeIDs:
#         query_word = '''
#                      MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
#             RETURN h,r,t,r1,t1 LIMIT 300
#                     '''
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#         for res in results_place:
#             if (res['t1'] is not None):
#                 if int(res['t1'].id) not in under_dire_to_file:
#                     temp = []
#                     under_dire.append(int(res["t1"].id))
#                 else:
#                     temp = under_dire_to_file[int(res["t1"].id)]
#                 temp.append(int(res["h"].id))
#                 if res["h"]["name"] not in file_id_list:
#                     file_id_list.append(res["h"]["name"])
#                     print(str(res["h"]["name"]) + " " + str(res["t1"]["name"]))
#                 under_dire_to_file[int(res["t1"].id)] = temp
#     print("11111111" + str(file_id_list))
#     print(under_dire_to_file)
#     # count, under_dire_to_file, under_dire = chuli_mysql(db, content, userID, session, bucket, zip_file1, zip_file2,
#     #                                                     zip_file3, zip_file4, count, under_dire_to_file, under_dire)
#
#     under_dire_to_file = chuli_file_dire(under_dire, under_dire_to_file, session)
#     print(under_dire_to_file)
#     clean_dire = {}
#     for key, value in under_dire_to_file.items():
#         clean_dire[key] = list(set(value))
#     under_dire_to_file = clean_dire
#     print(under_dire_to_file)
#     now_id = 5440190
#     while (1):
#         with open(zip_file5 + "文件涉及目录.txt", "w", encoding="utf-8") as file:
#             file.write(id_get_node(session, now_id)["name"] + "\n")
#             for i1 in under_dire_to_file[5440190]:  # i1是第一层目录
#                 file.write("—— " + str(id_get_node(session, i1)["name"]) + "\n")
#                 if int(i1) not in under_dire_to_file:
#                     continue
#                 for i2 in under_dire_to_file[i1]:  # i2是第二层目录
#                     file.write("—— —— " + str(id_get_node(session, i2)["name"]) + "\n")
#                     if int(i2) not in under_dire_to_file:
#                         continue
#                     for i3 in under_dire_to_file[int(i2)]:  # i3是第三层目录
#                         file.write("—— —— —— " + str(id_get_node(session, i3)["name"]) + "\n")
#                         if int(i3) not in under_dire_to_file:
#                             continue
#                         for i4 in under_dire_to_file[int(i3)]:
#                             node_name = check_node_name(session, i4)
#                             file.write("—— —— —— —— " + str(node_name) + "\n")
#                             if int(i4) in under_dire_to_file:
#                                 for i5 in under_dire_to_file[int(i4)]:
#                                     node_name = check_node_name(session, i5)
#                                     file.write("—— —— —— —— " + str(node_name) + "\n")
#                                     if int(i5) in under_dire_to_file:
#                                         for i6 in under_dire_to_file[int(i5)]:
#                                             node_name = check_node_name(session, i6)
#                                             file.write("—— —— —— —— " + str(node_name) + "\n")
#                                             if int(i6) in under_dire_to_file:
#                                                 for i7 in under_dire_to_file[int(i6)]:
#                                                     node_name = check_node_name(session, i7)
#                                                     file.write("—— —— —— —— " + str(node_name) + "\n")
#         break
#
#     if count == 30:
#         message = {
#             "message": "文件为空",
#         }
#         return json.dumps(message)
#     try:
#         zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#         zip.close()
#         zipDir(zip_file1, zip_new_file)
#         zipDir(zip_file2, zip_new_file)
#         zipDir(zip_file3, zip_new_file)
#         zipDir(zip_file4, zip_new_file)
#         compress_file(zip_file5+"文件涉及目录.txt", zip_new_file)
#         shutil.rmtree(zip_file1)
#         shutil.rmtree(zip_file2)
#         shutil.rmtree(zip_file3)
#         shutil.rmtree(zip_file4)
#         # shutil.rmtree(zip_file5)
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         response['message'] = "上传成功"
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500

# def main(request):
#     xiaoqi_id =request.GET["xiaoqi_id"]
#     content = request.GET["content"]
#     node_id = request.GET["node_ID"]
#     userID = request.GET["userID"]
#
#     test_type=request.GET["type"]
#
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     zip_new_file = 'F:\data\\' + content+str(xiaoqi_id)+ ".zip"
#     print(zip_new_file)
#     deletezip('F:\data\\')
#
#     nodeIDs = str(node_id).split(",")
#     if nodeIDs==['']:
#         return "文件为空！！！"
#
#     # 检查第一个地址是否存在
#
#
#     under_dire_to_file = {}
#     under_dire = []
#     count = 30
#     file_id_list = []
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#     print(nodeIDs)
#     all_dire=[]
#     for i in nodeIDs:
#         if test_type=="Test":
#             query_word = '''
#                      MATCH (t2:hypernode)-[r1]-(t1:Test)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#              RETURN t,r1,t1,r2,t2 LIMIT 100
#                     '''
#         elif test_type=="Test1":
#             query_word = '''
#                 MATCH (t2:hypernode)-[r1]-(t1:Test1)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#                 RETURN t,r1,t1,r2,t2 LIMIT 100
#                                 '''
#         elif test_type=="Test2":
#             query_word = '''
#                 MATCH (t2:hypernode)-[r1]-(t1:Test2)-[r2]-(t:Strict) WHERE (id(t)={arg_1})
#                 RETURN t,r1,t1,r2,t2 LIMIT 100
#                 '''
#         else:
#             query_word = '''
#                      MATCH (t:Strict) WHERE (id(t)={arg_1})
#              RETURN t LIMIT 100
#                                 '''
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#
#         for res in results_place:
#             print(res)
#             node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#             node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#             dire_name = zhconv.convert(res['t1']._properties["name"], 'zh-cn')
#             zip_file1='F:\data\\' + dire_name+ "\\"
#             if not os.path.exists(zip_file1):
#                 # 如果不存在,创建该目录
#                 os.makedirs(zip_file1)
#             if (zip_file1 not in all_dire):
#                 all_dire.append(zip_file1)
#             bucket.download_file_from_bucket('kofiles', node_path, zip_file1 + node_path.split('/')[-1])
#
#     print("222222" + str(file_id_list))
#     try:
#         zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#         zip.close()
#         for i in all_dire:
#             zipDir(i, zip_new_file)
#             shutil.rmtree(i)
#         # shutil.rmtree(zip_file5)
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         response['message'] = "上传成功"
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500
# def main(request):
#     content = request.GET["content"]
#     node_id = request.GET["node_ID"]
#     userID = request.GET["userID"]
#
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     zip_new_file = 'F:\data\\' + content + ".zip"
#     zip_file1 = 'F:\data\\wikipage\\'
#     zip_file2 = 'F:\data\\baidupage\\'
#     zip_file3 = 'F:\data\\File\\'
#     zip_file4 = 'F:\data\\Strict\\'
#     zip_file5 = 'F:\\' +"1"+"\\"
#     deletezip('F:\data\\')
#
#     nodeIDs = str(node_id).split(",")
#     if nodeIDs==['']:
#         return "文件为空！！！"
#
#     # 检查第一个地址是否存在
#     if not os.path.exists(zip_file1):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file1)
#
#     # 检查第二个地址是否存在
#     if not os.path.exists(zip_file2):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file2)
#
#     if not os.path.exists(zip_file3):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file3)
#
#     if not os.path.exists(zip_file4):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file4)
#
#     if not os.path.exists(zip_file5):
#         # 如果不存在,创建该目录
#         os.makedirs(zip_file5)
#
#     under_dire_to_file = {}
#     under_dire = []
#     count = 30
#     file_id_list = []
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session = driver.session()
#     for i in nodeIDs:
#         query_word = '''
#                  MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
#         RETURN h,r,t,r1,t1 LIMIT 300
#                 '''
#
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#
#
#         for res in results_place:
#             count -= 1
#             # print(res)
#             if 'File' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'File' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file3 + node_path.split('/')[-1])
#             elif 'Strict' in list(set(res['h'].labels)):
#                 # print(1)
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'Strict' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file4 + node_path.split('/')[-1])
#             elif 'baidupage' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'baidupage' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#                 bucket.download_minio_object('kofiles', node_path, zip_file2 + node_name + '.html')
#             elif 'wikipage' in list(set(res['h'].labels)):
#                 node_name = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['h']._properties["file_path"], 'zh-cn')
#                 node_path = node_path + "/" + node_name + ".html"
#                 # print(node_path)
#                 bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#                 if (res['h'].id) not in file_id_list:
#                     file_id_list.append(res['h'].id)
#             elif 'wikipage' in list(set(res['t'].labels)):
#                 node_name = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_path = zhconv.convert(res['t']._properties["file_path"], 'zh-cn')
#                 node_path = node_path + "/" + node_name + ".html"
#                 # print(node_path)
#                 bucket.download_minio_object('kofiles', node_path, zip_file1 + node_name + '.html')
#
#             else:
#                 count += 1
#             # if count == 0:
#             #     break
#
#     print("222222" + str(file_id_list))
#     for i in nodeIDs:
#         query_word = '''
#                      MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1})
#             RETURN h,r,t,r1,t1 LIMIT 300
#                     '''
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#         for res in results_place:
#             if (res['t1'] is not None):
#                 if int(res['t1'].id) not in under_dire_to_file:
#                     temp = []
#                     under_dire.append(int(res["t1"].id))
#                 else:
#                     temp = under_dire_to_file[int(res["t1"].id)]
#                 temp.append(int(res["h"].id))
#                 if res["h"]["name"] not in file_id_list:
#                     file_id_list.append(res["h"]["name"])
#                     print(str(res["h"]["name"]) + " " + str(res["t1"]["name"]))
#                 under_dire_to_file[int(res["t1"].id)] = temp
#     print("11111111" + str(file_id_list))
#     print(under_dire_to_file)
#     # count, under_dire_to_file, under_dire = chuli_mysql(db, content, userID, session, bucket, zip_file1, zip_file2,
#     #                                                     zip_file3, zip_file4, count, under_dire_to_file, under_dire)
#
#     under_dire_to_file = chuli_file_dire(under_dire, under_dire_to_file, session)
#     print(under_dire_to_file)
#     clean_dire = {}
#     for key, value in under_dire_to_file.items():
#         clean_dire[key] = list(set(value))
#     under_dire_to_file = clean_dire
#     print(under_dire_to_file)
#     now_id = 5440190
#     while (1):
#         with open(zip_file5 + "文件涉及目录.txt", "w", encoding="utf-8") as file:
#             file.write(id_get_node(session, now_id)["name"] + "\n")
#             for i1 in under_dire_to_file[5440190]:  # i1是第一层目录
#                 file.write("—— " + str(id_get_node(session, i1)["name"]) + "\n")
#                 if int(i1) not in under_dire_to_file:
#                     continue
#                 for i2 in under_dire_to_file[i1]:  # i2是第二层目录
#                     file.write("—— —— " + str(id_get_node(session, i2)["name"]) + "\n")
#                     if int(i2) not in under_dire_to_file:
#                         continue
#                     for i3 in under_dire_to_file[int(i2)]:  # i3是第三层目录
#                         file.write("—— —— —— " + str(id_get_node(session, i3)["name"]) + "\n")
#                         if int(i3) not in under_dire_to_file:
#                             continue
#                         for i4 in under_dire_to_file[int(i3)]:
#                             node_name = check_node_name(session, i4)
#                             file.write("—— —— —— —— " + str(node_name) + "\n")
#                             if int(i4) in under_dire_to_file:
#                                 for i5 in under_dire_to_file[int(i4)]:
#                                     node_name = check_node_name(session, i5)
#                                     file.write("—— —— —— —— " + str(node_name) + "\n")
#                                     if int(i5) in under_dire_to_file:
#                                         for i6 in under_dire_to_file[int(i5)]:
#                                             node_name = check_node_name(session, i6)
#                                             file.write("—— —— —— —— " + str(node_name) + "\n")
#                                             if int(i6) in under_dire_to_file:
#                                                 for i7 in under_dire_to_file[int(i6)]:
#                                                     node_name = check_node_name(session, i7)
#                                                     file.write("—— —— —— —— " + str(node_name) + "\n")
#         break
#
#     if count == 30:
#         message = {
#             "message": "文件为空",
#         }
#         return json.dumps(message)
#     try:
#         zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#         zip.close()
#         zipDir(zip_file1, zip_new_file)
#         zipDir(zip_file2, zip_new_file)
#         zipDir(zip_file3, zip_new_file)
#         zipDir(zip_file4, zip_new_file)
#         compress_file(zip_file5+"文件涉及目录.txt", zip_new_file)
#         shutil.rmtree(zip_file1)
#         shutil.rmtree(zip_file2)
#         shutil.rmtree(zip_file3)
#         shutil.rmtree(zip_file4)
#         # shutil.rmtree(zip_file5)
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         response['message'] = "上传成功"
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500