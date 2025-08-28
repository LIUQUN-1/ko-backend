import sys
import time
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
import zipfile
import os
from py2neo import Graph, Node, Relationship
import pandas as pd
import io
import sys
import networkx as nx
import json
import re
import csv
from neo4j import GraphDatabase
import requests
# 设置显示中文字体
import hashlib
import shutil
from minio import Minio, InvalidResponseError, S3Error
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
            except FileNotFoundError as err:
                print('download_failed: ' + str(err))
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
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')



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


def chuli_string(data,type):
    string=str(data)
    if type==0:
        pattern = r'\{(.*?)\}'
        match = re.search(pattern, string)
        result=match.group(1)
        result="{"+result+"}"
        result = result.replace("\'", "\"")
        result=result.replace("name:","\"name\":")
        result = result.replace("timestamp:", "\"timestamp\":")
        result = result.replace("type:", "\"type\":")
        result = result.replace("url:", "\"url\":")
        result = result.replace("des:", "\"des\":")
        result = result.replace("file_path:", "\"file_path\":")
        # print(result)
        json_data=json.loads(result)
        return json_data
    else:
        return string
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
def zipFiles(filepaths, outFullName):
    """
    压缩指定文件列表到压缩文件
    :param filepaths: 待压缩文件路径列表
    :param outFullName: 压缩文件保存路径 + xxxx.zip
    :return: 无
    """
    with zipfile.ZipFile(outFullName, "a", zipfile.ZIP_DEFLATED) as zipf:
        for filepath in filepaths:
            if os.path.isfile(filepath):  # 确保是文件
                zipf.write(filepath, os.path.basename(filepath))
            else:
                print(f"Warning: {filepath} is not a file and will be skipped.")
def search_category(name):


    url = "http://114.213.210.30:8095/entity/alias2entities"

    params = {
        # "entity_name": "合肥工业大学",
        "alias_name": name
    }
    response = requests.post(url, data=params)
    if response.status_code == 200:
        return response.json()
    else:
        return "请求失败"



def qu_chong_node(data):
    unique_data = list({item["name"]: item for item in data}.values())
    return unique_data

def main(request):
    # 连接到 Neo4j 数据库
    # 连接到 Neo4j 数据库
    start_time=time.time()
    content = request.GET.get("content")
    # the_index = request.GET.get("index")
    # print(the_index)
    # content="吴信东"
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    results=[]
    query = """          
            MATCH (n)-[r]-(m) WHERE (n:KOCategory and n.name={arg_1}) and (m:KOCategory) RETURN n,type(r),m LIMIT 100

    """
    # query = """
    #                 MATCH (n)-[r]-(m) WHERE (t.name={arg_2}) or (id(n) = {arg_1} and m:wikibaike) or (id(n) = {arg_1} and m:baidu_directory)
    #                  or (id(n) = {arg_1} and m:baidupage) or (id(n) = {arg_1} and m:wikipage)  RETURN n,m,type(r) LIMIT 30
    #
    #         """
    # 执行查询并获取结果
    result = session.run(query, parameters={"arg_1": content})
    # if str(result)=="(No data)":
    #     print("success")
    #     return "抱歉，系统未保存该实体文件!"
    # 遍历结果,获取节点和关系
    records=[]
    for record in result:
        n = record['n']
        m = record['m']
        r_type = record['type(r)']  # 获取关系的类型

        # 构造字典
        record_dict = {
            "n": {
                "id": n.id,
                "labels": list(n.labels),
                "properties": dict(n)
            },
            "m": {
                "id": m.id,
                "labels": list(m.labels),
                "properties": dict(m)
            },
            "relationship_type": r_type
        }
        records.append(record_dict)
    # 转换为 JSON
    # 打印 JSON
    # print(json_output)

    head=[]
    relation=[]
    tail=[]#边和节点分两个文件
    all_node_json=[]
    for i in records:
        n_json=i["n"]["properties"]
        m_json=i["m"]["properties"]
        re = i["relationship_type"]
        head.append(n_json["name"])
        relation.append(re)
        tail.append(m_json["name"])
        all_node_json.append(n_json)
        all_node_json.append(m_json)
    length=len(head)
    data_json=[]
    for i in range(length):
        l={}
        l["head"]=tail[i]
        l["relation"]=relation[i]
        l["tail"]=head[i]
        data_json.append(l)

    print("索引用时"+str(time.time()-start_time))
    new_file = 'F:\data\\' + str(content) + ".ko-关系.json"
    with open(new_file, "w", encoding="utf-8") as file:
        for data in data_json:
            json.dump(data, file, ensure_ascii=False, indent=4)
            file.write(",\n")

    with open('F:\data\\' + str(content) + ".ko-关系.csv", 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write each JSON object as a separate row in the CSV file
        for item in data_json:
            writer.writerow([json.dumps(item,ensure_ascii=False)])

    zip_dir='F:\data\\'
    zip_file1 =str(content) + ".ko-关系.json"
    zip_file3 = str(content) + ".ko-关系.csv"
    # zip_file5 = "F:\\data\\相关文件\\"
    zip_new_file = zip_dir+content + "-索引.zip"
    # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
    # zip.close()
    try:
        zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
        zip.close()
        zipFiles([zip_dir+zip_file1], zip_new_file)
        zipFiles([zip_dir+zip_file3], zip_new_file)
        # shutil.rmtree(zip_dir+zip_file1)
        # shutil.rmtree(zip_dir+zip_file3)
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
#
# import sys
# import time
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import zipfile
# import os
# from py2neo import Graph, Node, Relationship
# import pandas as pd
# import io
# import sys
# import networkx as nx
# import json
# import re
# import csv
# from neo4j import GraphDatabase
# import requests
# # 设置显示中文字体
# import hashlib
# from minio import Minio, InvalidResponseError, S3Error
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
#     def create_one_bucket(self, bucket_name):
#         # 创建桶(调用make_bucket api来创建一个桶)
#         """
#         桶命名规则：小写字母，句点，连字符和数字 允许使用 长度至少3个字符
#         使用大写字母、下划线等会报错
#         """
#         try:
#             # bucket_exists：检查桶是否存在
#             if self.minioClient.bucket_exists(bucket_name=bucket_name):
#                 print("该存储桶已经存在")
#             else:
#                 self.minioClient.make_bucket(bucket_name=bucket_name)
#                 print(f"{bucket_name}桶创建成功")
#         except InvalidResponseError as err:
#             print(err)
#
#     def remove_one_bucket(self, bucket_name):
#         # 删除桶(调用remove_bucket api来创建一个存储桶)
#         try:
#             if self.minioClient.bucket_exists(bucket_name=bucket_name):
#                 self.minioClient.remove_bucket(bucket_name)
#                 print("删除存储桶成功")
#             else:
#                 print("该存储桶不存在")
#         except InvalidResponseError as err:
#             print(err)
#
#     def upload_file_to_bucket(self, bucket_name, file_name, file_path):
#         """
#         将文件上传到bucket
#         :param bucket_name: minio桶名称
#         :param file_name: 存放到minio桶中的文件名字(相当于对文件进行了重命名，可以与原文件名不同)
#                             file_name处可以创建新的目录(文件夹) 例如 /example/file_name
#                             相当于在该桶中新建了一个example文件夹 并把文件放在其中
#         :param file_path: 本地文件的路径
#         """
#         # 桶是否存在 不存在则新建
#         check_bucket = self.minioClient.bucket_exists(bucket_name)
#         if not check_bucket:
#             self.minioClient.make_bucket(bucket_name)
#
#         try:
#             self.minioClient.fput_object(bucket_name=bucket_name,
#                                          object_name=file_name,
#                                          file_path=file_path)
#         except FileNotFoundError as err:
#             print('upload_failed: ' + str(err))
#         except S3Error as err:
#             print("upload_failed:", err)
#
#     def download_file_from_bucket(self, bucket_name, minio_file_path, download_file_path):
#         """
#         从bucket下载文件
#         :param bucket_name: minio桶名称
#         :param minio_file_path: 存放在minio桶中文件名字
#                             file_name处可以包含目录(文件夹) 例如 /example/file_name
#         :param download_file_path: 文件获取后存放的路径
#         """
#         # 桶是否存在
#         check_bucket = self.minioClient.bucket_exists(bucket_name)
#         if check_bucket:
#             try:
#                 self.minioClient.fget_object(bucket_name=bucket_name,
#                                              object_name=minio_file_path,
#                                              file_path=download_file_path)
#             except FileNotFoundError as err:
#                 print('download_failed: ' + str(err))
#             except S3Error as err:
#                 print("download_failed:", err)
#
#     def remove_object(self, bucket_name, object_name):
#         """
#         从bucket删除文件
#         :param bucket_name: minio桶名称
#         :param object_name: 存放在minio桶中的文件名字
#                             object_name处可以包含目录(文件夹) 例如 /example/file_name
#         """
#         # 桶是否存在
#         check_bucket = self.minioClient.bucket_exists(bucket_name)
#         if check_bucket:
#             try:
#                 self.minioClient.remove_object(bucket_name=bucket_name,
#                                                object_name=object_name)
#             except FileNotFoundError as err:
#                 print('upload_failed: ' + str(err))
#             except S3Error as err:
#                 print("upload_failed:", err)
#
#     # 获取所有的桶
#     def get_all_bucket(self):
#         buckets = self.minioClient.list_buckets()
#         ret = []
#         for _ in buckets:
#             ret.append(_.name)
#         return ret
#
#     # 获取一个桶中的所有一级目录和文件
#     def get_list_objects_from_bucket(self, bucket_name):
#         # 桶是否存在
#         check_bucket = self.minioClient.bucket_exists(bucket_name)
#         if check_bucket:
#             # 获取到该桶中的所有目录和文件
#             objects = self.minioClient.list_objects(bucket_name=bucket_name)
#             ret = []
#             for _ in objects:
#                 ret.append(_.object_name)
#             return ret
#
#     # 获取桶里某个目录下的所有目录和文件
#     def get_list_objects_from_bucket_dir(self, bucket_name, dir_name):
#         # 桶是否存在
#         check_bucket = self.minioClient.bucket_exists(bucket_name)
#         if check_bucket:
#             # 获取到bucket_所name桶中的dir_name下的有目录和文件
#             # prefix 获取的文件路径需包含该前缀
#             objects = self.minioClient.list_objects(bucket_name=bucket_name,
#                                                     prefix=dir_name,
#                                                     recursive=True)
#             ret = []
#             for obj in objects:
#                 object_name = obj.object_name
#                 # 获取对象的内容
#                 content = self.minioClient.get_object(bucket_name=bucket_name,
#                                                       object_name=object_name)
#                 ret.append(content.data.decode())
#             return ret
#
#
# def get_sha1_hash(file_name):
#     shal_hash = hashlib.sha1(file_name.encode()).hexdigest()
#     return shal_hash
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
#
#
#
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
#
#
# def chuli_string(data,type):
#     string=str(data)
#     if type==0:
#         pattern = r'\{(.*?)\}'
#         match = re.search(pattern, string)
#         result=match.group(1)
#         result="{"+result+"}"
#         result = result.replace("\'", "\"")
#         result=result.replace("name:","\"name\":")
#         result = result.replace("timestamp:", "\"timestamp\":")
#         result = result.replace("type:", "\"type\":")
#         result = result.replace("url:", "\"url\":")
#         result = result.replace("des:", "\"des\":")
#         result = result.replace("file_path:", "\"file_path\":")
#         # print(result)
#         json_data=json.loads(result)
#         return json_data
#     else:
#         return string
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
#
# def search_category(name):
#
#
#     url = "http://114.213.210.30:8095/entity/alias2entities"
#
#     params = {
#         # "entity_name": "合肥工业大学",
#         "alias_name": name
#     }
#     response = requests.post(url, data=params)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         return "请求失败"
#
#
# # def compress_specific_files(dirpath, outFullName, filenames):
# #     # 确保提供的文件名是列表形式
# #     if not isinstance(filenames, list):
# #         raise ValueError("filenames should be a list of file names.")
# #
# #     # 创建或打开 ZIP 文件用于写入
# #     with zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED) as zipf:
# #         # 遍历提供的文件名列表
# #         for filename in filenames:
# #             # 检查文件是否存在
# #             file_path = os.path.join(dirpath, filename)
# #             # 打印文件路径用于调试
# #             print(f"Attempting to add: {file_path}")
# #             if os.path.isfile(os.path.join(dirpath, filename)):
# #                 # 将文件添加到 ZIP 中，使用绝对路径
# #                 zipf.write(os.path.join(dirpath, filename), arcname=filename)
# #                 print(f"Added to zip: {filename}")
# #             else:
# #                 print(f"Warning: {filename} does not exist in {dirpath}")
# def compress_files_and_folders(zip_new_file, dirpath, files, folders):
#     with zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED) as zipf:
#         # 添加文件
#         for file in files:
#             file_path = os.path.join(dirpath, file)
#             if os.path.isfile(file_path):
#                 zipf.write(file_path, arcname=file)  # 使用文件名作为存档名
#                 print(f"Added file to zip: {file}")
#             else:
#                 print(f"Warning: {file_path} does not exist.")
#         print("file end")
#         # 添加文件夹及其内容
#         for folder in folders:
#             folder_path = os.path.join(dirpath, folder)
#             if os.path.isdir(folder_path):
#                 for root, _, files_in_folder in os.walk(folder_path):
#                     for file in files_in_folder:
#                         file_path = os.path.join(root, file)
#                         arcname = os.path.relpath(file_path, start=dirpath)
#                         zipf.write(file_path, arcname)
#                         print(f"Added folder content to zip: {arcname}")
#             else:
#                 print(f"Warning: {folder_path} does not exist.")
# def qu_chong_node(data):
#     unique_data = list({item["url"]: item for item in data}.values())
#     return unique_data
#
# def export_content_file(content,driver,session):
#     query = """
#                 MATCH (n)-[r]-(m)-[r1]-(l) WHERE  ((n.name={arg_1} and n:wikibaike) or (n.name={arg_1} and n:baidu_directory))
#                 and (m:hypernode) and (l:baidupage or l:wikipage)
#                 RETURN m,type(r),l LIMIT 50
#
#         """
#     records=[]
#     result = session.run(query, parameters={"arg_1": content})
#     for record in result:
#         l = record['l']
#         m = record['m']
#         r_type = record['type(r)']  # 获取关系的类型
#         # 构造字典
#         record_dict = {
#             "l": {
#                 "id": l.id,
#                 "labels": list(l.labels),
#                 "properties": dict(l)
#             },
#             "m": {
#                 "id": m.id,
#                 "labels": list(m.labels),
#                 "properties": dict(m)
#             },
#             "relationship_type": r_type
#         }
#         records.append(record_dict)
#     # head = []
#     # relation = []
#     # tail = []  # 边和节点分两个文件
#     all_node_json = []
#     for i in records:
#         l_json = i["l"]["properties"]
#         all_node_json.append(l_json)
#
#     print(all_node_json)
#     #拿文件
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     # 创建桶测试
#     bucket.create_one_bucket('kofiles')
#     zip_file_file = str(content) + '相关文件\\'
#     if not os.path.exists('F:\data\\' + zip_file_file):
#         # 如果不存在,创建该目录
#         os.makedirs('F:\data\\' + zip_file_file)
#         print(f"已创建目录: {zip_file_file}")
#
#     num = 0
#     all_file_node_json=all_node_json
#     print(len(all_file_node_json))
#     wiki_path = 'F:\data\\' + str(content) + '相关文件\\wikipage\\'
#     baidu_path = 'F:\data\\' + str(content) + '相关文件\\baidupage\\'
#
#     if not os.path.exists(wiki_path):
#         # 如果不存在,创建该目录
#         os.makedirs(wiki_path)
#         print(f"已创建目录: {wiki_path}")
#     if not os.path.exists(baidu_path):
#         # 如果不存在,创建该目录
#         os.makedirs(baidu_path)
#         print(f"已创建目录: {baidu_path}")
#
#     for i in all_file_node_json:
#         name = i["name"]
#         if "file_path" in i:
#             file_path = i["file_path"]
#         else:
#             continue
#         if "usrid" in i:
#             file_path = file_path + "/" + name + ".html"
#         # print(file_path)
#         ret = bucket.get_list_objects_from_bucket_dir('kofiles',
#                                                       file_path)
#         if ret == None or len(ret) == 0:
#             print("失败")
#             wiki_queshi = 1
#         else:
#             # print('F:\data\\'+zip_file_file +"\\"+ name + '.html')
#             # save_path=""
#             if "usrid" in i:
#                 save_path = wiki_path + name + '.html'
#             else:
#                 save_path = baidu_path + name + '.html'
#             with open(save_path, 'w', encoding='utf-8') as file:
#                 file.write(ret[0])
#             num += 1
#         if (num >= 50):
#             break
#     zip_dir = 'F:\data\\'
#     # zip_file5 = "F:\\data\\相关文件\\"
#     zip_baidu_file = str(content)+"相关文件\\baidupage\\"
#     zip_wiki_file = "相关文件\\wikipage\\"
#     zip_new_file = zip_dir + content + "-索引.zip"
#     # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#     # zip.close()
#     try:
#         compress_files_and_folders(zip_new_file, zip_dir, [],
#                                    [zip_baidu_file, zip_wiki_file])
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500
#
# def main(request):
#     # 连接到 Neo4j 数据库
#     # 连接到 Neo4j 数据库
#     start_time=time.time()
#     content = request.GET.get("content")
#     # the_index = request.GET.get("index")
#     # print(the_index)
#     # content="吴信东"
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
#     session = driver.session()
#     name_list = search_category(content)
#     print(name_list)
#     if name_list == "请求失败":
#         return "请求失败"
#     else:
#         name_list = name_list["data"]
#     if len(name_list)<=0:
#         message=export_content_file(content,driver,session)
#         return message
#     id_list = []
#     for i in name_list:
#         id_list.append(int(i["graphId"]))
#     # print(id_list)
#     # 执行 Cypher 查询
#     results=[]
#     for i in id_list:
#         query = """
#                 MATCH (n)-[r]-(m) WHERE (n:KOCategory and n.name={arg_1}) and (m:KOCategory) RETURN n,r,m LIMIT 100
#
#         """
#         # query = """
#         #                 MATCH (n)-[r]-(m) WHERE (t.name={arg_2}) or (id(n) = {arg_1} and m:wikibaike) or (id(n) = {arg_1} and m:baidu_directory)
#         #                  or (id(n) = {arg_1} and m:baidupage) or (id(n) = {arg_1} and m:wikipage)  RETURN n,m,type(r) LIMIT 30
#         #
#         #         """
#         # 执行查询并获取结果
#         result = session.run(query, parameters={"arg_1": i})
#         results.append(result)
#     # if str(result)=="(No data)":
#     #     print("success")
#     #     return "抱歉，系统未保存该实体文件!"
#     # 遍历结果,获取节点和关系
#     records=[]
#     for result in results:
#         for record in result:
#             n = record['n']
#             m = record['m']
#             r_type = record['type(r)']  # 获取关系的类型
#
#             # 构造字典
#             record_dict = {
#                 "n": {
#                     "id": n.id,
#                     "labels": list(n.labels),
#                     "properties": dict(n)
#                 },
#                 "m": {
#                     "id": m.id,
#                     "labels": list(m.labels),
#                     "properties": dict(m)
#                 },
#                 "relationship_type": r_type
#             }
#
#             records.append(record_dict)
#
#     # 转换为 JSON
#     json_output = json.dumps(records, ensure_ascii=False, indent=4)
#
#     # 打印 JSON
#     # print(json_output)
#
#     head=[]
#     relation=[]
#     tail=[]#边和节点分两个文件
#     all_node_json=[]
#     for i in records:
#         n_json=i["n"]["properties"]
#         m_json=i["m"]["properties"]
#         re = i["relationship_type"]
#         head.append(n_json["url"])
#         relation.append(re)
#         tail.append(m_json["url"])
#         all_node_json.append(n_json)
#         all_node_json.append(m_json)
#     # print(len(all_node_json))
#     all_node_json=qu_chong_node(all_node_json)
#     # print(len(all_node_json))
#     length=len(head)
#     data_json=[]
#     for i in range(length):
#         l={}
#         l["head"]=tail[i]
#         l["relation"]=relation[i]
#         l["tail"]=head[i]
#         data_json.append(l)
#
#     print("索引用时"+str(time.time()-start_time))
#     start_time=time.time()
#     #以上是索引的，下面是文件的
#     all_file_results=[]
#     for i in id_list:
#         query = """
#                    MATCH (start:c_hypernode)-[*1..2]-(node)-[*1..2]-(node1)
#                     WHERE (node:hypernode) and (node1:File or  node1:baidupage) and (id(start)={arg_1})
#                     WITH node1
#                     LIMIT 10
#                     RETURN node1
#                 """
#         result = session.run(query, parameters={"arg_1": i})
#         all_file_results.append(result)
#     file_records=[]
#     for result in all_file_results:
#         for record in result:
#             node1=record["node1"]
#             record_dict = {
#                 "n": {
#                     "id": node1.id,
#                     "labels": list(node1.labels),
#                     "properties": dict(node1)
#                 }
#             }
#
#             file_records.append(record_dict)
#         # 转换为 JSON
#     # json_file = json.dumps(file_records, ensure_ascii=False, indent=4)
#     print("查询用时" + str(time.time() - start_time))
#     start_time=time.time()
#     # 打印 JSON
#     # print(json_file)
#     all_file_node_json=[]
#     for i in file_records:
#         node_json=i["n"]["properties"]
#         all_file_node_json.append(node_json)
#     # print(all_file_node_json)
#     minio_address = "114.213.232.140:19000"
#     minio_admin = "minioadmin"
#     minio_password = "minioadmin"
#
#     bucket = Bucket(minio_address=minio_address,
#                     minio_admin=minio_admin,
#                     minio_password=minio_password)
#     # 创建桶测试
#     bucket.create_one_bucket('kofiles')
#     zip_file_file = str(content)+'相关文件\\'
#     if not os.path.exists('F:\data\\'+zip_file_file):
#         # 如果不存在,创建该目录
#         os.makedirs('F:\data\\'+zip_file_file)
#         print(f"已创建目录: {zip_file_file}")
#     num=0
#     print(len(all_file_node_json))
#     wiki_path='F:\data\\'+str(content)+'相关文件\\wikipage\\'
#     baidu_path = 'F:\data\\'+str(content)+'相关文件\\baidupage\\'
#
#     if not os.path.exists(wiki_path):
#         # 如果不存在,创建该目录
#         os.makedirs(wiki_path)
#         print(f"已创建目录: {wiki_path}")
#     if not os.path.exists(baidu_path):
#         # 如果不存在,创建该目录
#         os.makedirs(baidu_path)
#         print(f"已创建目录: {baidu_path}")
#
#     for i in all_file_node_json:
#         name=i["name"]
#         if "file_path" in i:
#             file_path=i["file_path"]
#         else:
#             continue
#         if "usrid" in i:
#             file_path=file_path+"/"+name+".html"
#         # print(file_path)
#         ret = bucket.get_list_objects_from_bucket_dir('kofiles',
#                                                      file_path)
#         if ret == None or len(ret) == 0:
#             # print("失败")
#             wiki_queshi = 1
#         else:
#             # print('F:\data\\'+zip_file_file +"\\"+ name + '.html')
#             # save_path=""
#             if "usrid" in i:
#                 save_path=wiki_path+name + '.html'
#             else:
#                 save_path=baidu_path+name+'.html'
#             with open(save_path, 'w', encoding='utf-8') as file:
#                 file.write(ret[0])
#             num+=1
#         if (num>=50):
#             break
#     print("处理+创建耗时"+str(time.time()-start_time))
#     new_file = 'F:\data\\' + str(content) + ".ko-关系.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in data_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#
#     # df = pd.DataFrame(data_json)
#     # df.to_excel('F:\data\\' + str(content) + ".ko-关系.xlsx", index=False)
#
#
#     with open('F:\data\\' + str(content) + ".ko-关系.csv", 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#
#         # Write each JSON object as a separate row in the CSV file
#         for item in data_json:
#             writer.writerow([json.dumps(item,ensure_ascii=False)])
#
#     new_file = 'F:\data\\' + str(content) + ".ko-实体.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in all_node_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#
#     # df = pd.DataFrame(all_node_json)
#     # df.to_excel('F:\data\\' + str(content) + ".ko-实体.xlsx", index=False)
#
#     with open('F:\data\\' + str(content) + ".ko-实体.csv", 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#
#         # Write each JSON object as a separate row in the CSV file
#         for item in all_node_json:
#             writer.writerow([json.dumps(item,ensure_ascii=False)])
#
#     zip_dir='F:\data\\'
#     zip_file1 =str(content) + ".ko-关系.json"
#     zip_file2 = str(content) + ".ko-实体.json"
#     zip_file3 = str(content) + ".ko-关系.csv"
#     zip_file4 = str(content) + ".ko-实体.csv"
#     # zip_file5 = "F:\\data\\相关文件\\"
#     zip_baidu_file = str(content)+"相关文件\\baidupage\\"
#     zip_wiki_file =  str(content)+"相关文件\\wikipage\\"
#     zip_new_file = zip_dir+content + "-索引.zip"
#     # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#     # zip.close()
#     try:
#         # zipDir(zip_file1, zip_new_file)
#         # zipDir(zip_file2, zip_new_file)
#         # zipDir(zip_file3, zip_new_file)
#         # zipDir(zip_file4, zip_new_file)
#         # zipDir(zip_file5,zip_new_file)
#         compress_files_and_folders(zip_new_file,zip_dir,[zip_file1,zip_file2,zip_file3,zip_file4],[zip_baidu_file,zip_wiki_file])
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500
#8.23的版本
# import sys
# import time
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import zipfile
# import os
# from py2neo import Graph, Node, Relationship
# import pandas as pd
# import io
# import sys
# import networkx as nx
# import json
# import re
# import csv
# # 设置显示中文字体
#
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
#
#
#
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
#
#
# def chuli_string(data,type):
#     string=str(data)
#     if type==0:
#         pattern = r'\{(.*?)\}'
#         match = re.search(pattern, string)
#         result=match.group(1)
#         result="{"+result+"}"
#         result = result.replace("\'", "\"")
#         result=result.replace("name:","\"name\":")
#         result = result.replace("timestamp:", "\"timestamp\":")
#         result = result.replace("type:", "\"type\":")
#         result = result.replace("url:", "\"url\":")
#         result = result.replace("des:", "\"des\":")
#         result = result.replace("file_path:", "\"file_path\":")
#         # print(result)
#         json_data=json.loads(result)
#         return json_data
#     else:
#         return string
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
#
#
# def compress_specific_files(dirpath, outFullName, filenames):
#     # 确保提供的文件名是列表形式
#     if not isinstance(filenames, list):
#         raise ValueError("filenames should be a list of file names.")
#
#     # 创建或打开 ZIP 文件用于写入
#     with zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED) as zipf:
#         # 遍历提供的文件名列表
#         for filename in filenames:
#             # 检查文件是否存在
#             file_path = os.path.join(dirpath, filename)
#             # 打印文件路径用于调试
#             print(f"Attempting to add: {file_path}")
#             if os.path.isfile(os.path.join(dirpath, filename)):
#                 # 将文件添加到 ZIP 中，使用绝对路径
#                 zipf.write(os.path.join(dirpath, filename), arcname=filename)
#                 print(f"Added to zip: {filename}")
#             else:
#                 print(f"Warning: {filename} does not exist in {dirpath}")
# def qu_chong_node(data):
#     unique_data = list({item["url"]: item for item in data}.values())
#     return unique_data
#
#
# def main(request):
#     # 连接到 Neo4j 数据库
#     # 连接到 Neo4j 数据库
#     content = request.GET.get("content")
#     the_index = request.GET.get("index")
#     print(the_index)
#     # content="吴信东"
#     graph = Graph("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#
#     # 执行 Cypher 查询
#     if int(the_index) == 1:
#         query = """
#                         MATCH (n:hypernode)<-[r]-(m)
#                         WHERE (n.name=\"""" + str(content) + """\" )
#                         RETURN n,m,type(r) LIMIT 300
#         """
#     elif int(the_index)==2:
#         query = """
#         MATCH (n:hypernode)<-[r]-(m)<-[p]-(o)
#         WHERE (n.name=\"""" + str(content) + """\" and o:wikibaike) or (n.name=\"""" + str(content) + """\" and o:baidu_directory)
#         RETURN n,m,o,type(r),type(p) LIMIT 300
#                 """
#     elif int(the_index) == 3:
#         query = """
#                 MATCH (n:hypernode)<-[r]-(m)<-[p]-(o)<-[l]-(q)
#                 WHERE (n.name=\"""" + str(content) + """\" and o:wikibaike and q:wikibaike) or (n.name=\"""" + str(
#             content) + """\" and o:wikibaike and q:baidu_directory) or (n.name=\"""" + str(
#             content) + """\" and o:baidu_directory and q:wikibaike) or (n.name=\"""" + str(content) + """\" and o:baidu_directory and q:baidu_directory)
#                 RETURN n,m,o,q,type(r),type(p),type(l) LIMIT 300
#                 """
#     # 执行查询并获取结果
#     print(query)
#     result = graph.run(query)
#     print(result)
#     if str(result)=="(No data)":
#         print("sucess")
#         return "抱歉，系统未保存该实体文件!"
#     # 遍历结果,获取节点和关系
#     result_df = pd.DataFrame([dict(record) for record in result])
#     head=[]
#     relation=[]
#     tail=[]#边和节点分两个文件
#     all_node_json=[]
#     for index, row in result_df.iterrows():
#         if int(the_index) >= 1:
#             head.append(chuli_string(row['n'], 0)["url"])
#             relation.append(chuli_string(row['type(r)'], 1))
#             tail.append(chuli_string(row['m'], 0)["url"])
#             all_node_json.append(chuli_string(row['n'], 0))
#             all_node_json.append(chuli_string(row['m'], 0))
#         if int(the_index) >= 2:
#             head.append(chuli_string(row['m'], 0)["url"])
#             relation.append(chuli_string(row['type(p)'], 1))
#             tail.append(chuli_string(row['o'], 0)["url"])
#             all_node_json.append(chuli_string(row['o'], 0))
#         if int(the_index) >= 3:
#             head.append(chuli_string(row['o'], 0)["url"])
#             relation.append(chuli_string(row['type(l)'], 1))
#             tail.append(chuli_string(row['q'], 0)["url"])
#             all_node_json.append(chuli_string(row['q'], 0))
#     # print(len(all_node_json))
#     all_node_json=qu_chong_node(all_node_json)
#     # print(len(all_node_json))
#     length=len(head)
#     data_json=[]
#     for i in range(length):
#         l={}
#         l["head"]=tail[i]
#         l["relation"]=relation[i]
#         l["tail"]=head[i]
#         data_json.append(l)
#
#     new_file = 'F:\data\\' + str(content) + ".ko-关系.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in data_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#
#     # df = pd.DataFrame(data_json)
#     # df.to_excel('F:\data\\' + str(content) + ".ko-关系.xlsx", index=False)
#
#
#     with open('F:\data\\' + str(content) + ".ko-关系.csv", 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#
#         # Write each JSON object as a separate row in the CSV file
#         for item in data_json:
#             writer.writerow([json.dumps(item,ensure_ascii=False)])
#
#     new_file = 'F:\data\\' + str(content) + ".ko-实体.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in all_node_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#
#     # df = pd.DataFrame(all_node_json)
#     # df.to_excel('F:\data\\' + str(content) + ".ko-实体.xlsx", index=False)
#
#     with open('F:\data\\' + str(content) + ".ko-实体.csv", 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#
#         # Write each JSON object as a separate row in the CSV file
#         for item in all_node_json:
#             writer.writerow([json.dumps(item,ensure_ascii=False)])
#
#     zip_dir='F:\data\\'
#     zip_file1 = str(content) + ".ko-关系.json"
#     zip_file2 = str(content) + ".ko-实体.json"
#     zip_file3 = str(content) + ".ko-关系.csv"
#     zip_file4 = str(content) + ".ko-实体.csv"
#     zip_new_file = zip_dir + content + "-索引.zip"
#     # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#     # zip.close()
#     try:
#         compress_specific_files(zip_dir,zip_new_file,[zip_file1,zip_file2,zip_file3,zip_file4])
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500

# print(head)
# print(relation)
# print(tail)


# for record in result:
#     node1 = record["n"]
#     relationship = record["r"]
#     node2 = record["m"]
#
#     # 处理节点和关系的数据
#     print(f"Node1: {node1.encode('utf-8')}")
#     print(f"Relationship: {relationship}")
#     print(f"Node2: {node2.encode('utf-8')}")
#     print("---")
#非常老的版本
# import sys
# import time
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import zipfile
# import os
# from py2neo import Graph, Node, Relationship
# import pandas as pd
# import io
# import sys
# import networkx as nx
# import json
# import re
#
# # 设置显示中文字体
#
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
#
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
#
#
# def chuli_string(data,type):
#     string=str(data)
#     if type==0:
#         pattern = r'\{(.*?)\}'
#         match = re.search(pattern, string)
#         result=match.group(1)
#         result="{"+result+"}"
#         result = result.replace("\'", "\"")
#         result=result.replace("name:","\"name\":")
#         result = result.replace("timestamp:", "\"timestamp\":")
#         result = result.replace("type:", "\"type\":")
#         result = result.replace("url:", "\"url\":")
#         result = result.replace("des:", "\"des\":")
#         result = result.replace("file_path:", "\"file_path\":")
#         # print(result)
#         json_data=json.loads(result)
#         return json_data
#     else:
#         return string
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
#
#
# def compress_specific_files(dirpath, outFullName, filenames):
#     # 确保提供的文件名是列表形式
#     if not isinstance(filenames, list):
#         raise ValueError("filenames should be a list of file names.")
#
#     # 创建或打开 ZIP 文件用于写入
#     with zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED) as zipf:
#         # 遍历提供的文件名列表
#         for filename in filenames:
#             # 检查文件是否存在
#             file_path = os.path.join(dirpath, filename)
#             # 打印文件路径用于调试
#             print(f"Attempting to add: {file_path}")
#             if os.path.isfile(os.path.join(dirpath, filename)):
#                 # 将文件添加到 ZIP 中，使用绝对路径
#                 zipf.write(os.path.join(dirpath, filename), arcname=filename)
#                 print(f"Added to zip: {filename}")
#             else:
#                 print(f"Warning: {filename} does not exist in {dirpath}")
# def qu_chong_node(data):
#     unique_data = list({item["url"]: item for item in data}.values())
#     return unique_data
# def main(request):
#     # 连接到 Neo4j 数据库
#     content = request.GET.get("content")
#     the_index = request.GET.get("index")
#     print(the_index)
#     # content="吴信东"
#     graph = Graph("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#
#     # 执行 Cypher 查询
#     if int(the_index) == 2:
#         query = """
#         MATCH (n:hypernode)<-[r]->(m)<-[p]->(o)
#         WHERE (n.name=\"""" + str(content) + """\" and o:wikibaike) or (n.name=\"""" + str(content) + """\" and o:baidu_directory)
#         RETURN n,m,o,type(r),type(p) LIMIT 300
#         """
#     elif int(the_index) == 1:
#         query = """
#                 MATCH (n:hypernode)<-[r]->(m)
#                 WHERE (n.name=\"""" + str(content) + """\" )
#                 RETURN n,m,type(r) LIMIT 300
#                 """
#     elif int(the_index) == 3:
#         query = """
#                 MATCH (n:hypernode)<-[r]->(m)<-[p]->(o)<-[l]->(q)
#                 WHERE (n.name=\"""" + str(content) + """\" and o:wikibaike and q:wikibaike) or (n.name=\"""" + str(
#             content) + """\" and o:wikibaike and q:baidu_directory) or (n.name=\"""" + str(
#             content) + """\" and o:baidu_directory and q:wikibaike) or (n.name=\"""" + str(content) + """\" and o:baidu_directory and q:baidu_directory)
#                 RETURN n,m,o,q,type(r),type(p),type(l) LIMIT 300
#                 """
#     # 执行查询并获取结果
#     print(query)
#     result = graph.run(query)
#     # 遍历结果,获取节点和关系
#     result_df = pd.DataFrame([dict(record) for record in result])
#     head=[]
#     relation=[]
#     tail=[]#边和节点分两个文件
#     all_node_json=[]
#     for index, row in result_df.iterrows():
#         if int(the_index) >= 1:
#             head.append(chuli_string(row['n'], 0)["url"])
#             relation.append(chuli_string(row['type(r)'], 1))
#             tail.append(chuli_string(row['m'], 0)["url"])
#             all_node_json.append(chuli_string(row['n'], 0))
#             all_node_json.append(chuli_string(row['m'], 0))
#         if int(the_index) >= 2:
#             head.append(chuli_string(row['m'], 0)["url"])
#             relation.append(chuli_string(row['type(p)'], 1))
#             tail.append(chuli_string(row['o'], 0)["url"])
#             all_node_json.append(chuli_string(row['o'], 0))
#         if int(the_index) >= 3:
#             head.append(chuli_string(row['o'], 0)["url"])
#             relation.append(chuli_string(row['type(l)'], 1))
#             tail.append(chuli_string(row['q'], 0)["url"])
#             all_node_json.append(chuli_string(row['q'], 0))
#     # print(len(all_node_json))
#     all_node_json=qu_chong_node(all_node_json)
#     # print(len(all_node_json))
#     length=len(head)
#     data_json=[]
#     for i in range(length):
#         l={}
#         l["head"]=tail[i]
#         l["relation"]=relation[i]
#         l["tail"]=head[i]
#         data_json.append(l)
#     new_file = 'F:\data\\' + str(content) + ".ko-关系.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in data_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#     new_file = 'F:\data\\' + str(content) + ".ko-实体.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in all_node_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#     zip_dir='F:\data\\'
#     zip_file1 = str(content) + ".ko-关系.json"
#     zip_file2 = str(content) + ".ko-实体.json"
#     zip_new_file = zip_dir + content + "-索引.zip"
#     # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#     # zip.close()
#     try:
#         compress_specific_files(zip_dir,zip_new_file,[zip_file1,zip_file2])
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500

# print(head)
# print(relation)
# print(tail)


# for record in result:
#     node1 = record["n"]
#     relationship = record["r"]
#     node2 = record["m"]
#
#     # 处理节点和关系的数据
#     print(f"Node1: {node1.encode('utf-8')}")
#     print(f"Relationship: {relationship}")
#     print(f"Node2: {node2.encode('utf-8')}")
#     print("---")

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
#
#
# def chuli_string(data,type):
#     string=str(data)
#     if type==0:
#         pattern = r'\{(.*?)\}'
#         match = re.search(pattern, string)
#         result=match.group(1)
#         result="{"+result+"}"
#         result = result.replace("\'", "\"")
#         result=result.replace("name","\"name\"")
#         result = result.replace("timestamp", "\"timestamp\"")
#         result = result.replace("type", "\"type\"")
#         result = result.replace("url", "\"url\"")
#         result = result.replace("des", "\"des\"")
#         result = result.replace("file_path", "\"file_path\"")
#         # print(result)
#         json_data=json.loads(result)
#         return json_data
#     else:
#         return string
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
#
#
# def compress_specific_files(dirpath, outFullName, filenames):
#     # 确保提供的文件名是列表形式
#     if not isinstance(filenames, list):
#         raise ValueError("filenames should be a list of file names.")
#
#     # 创建或打开 ZIP 文件用于写入
#     with zipfile.ZipFile(outFullName, "w", zipfile.ZIP_DEFLATED) as zipf:
#         # 遍历提供的文件名列表
#         for filename in filenames:
#             # 检查文件是否存在
#             file_path = os.path.join(dirpath, filename)
#             # 打印文件路径用于调试
#             print(f"Attempting to add: {file_path}")
#             if os.path.isfile(os.path.join(dirpath, filename)):
#                 # 将文件添加到 ZIP 中，使用绝对路径
#                 zipf.write(os.path.join(dirpath, filename), arcname=filename)
#                 print(f"Added to zip: {filename}")
#             else:
#                 print(f"Warning: {filename} does not exist in {dirpath}")
# def qu_chong_node(data):
#     unique_data = list({item["url"]: item for item in data}.values())
#     return unique_data
# def main(request):
#     # 连接到 Neo4j 数据库
#     content = request.GET["content"]
#     # content="吴信东"
#     graph = Graph("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#
#     # 执行 Cypher 查询
#     query = """
#     MATCH (n:hypernode)<-[r]->(m)<-[p]->(o)
#     WHERE (n.name=\""""+str(content)+"""\" and o:wikibaike) or (n.name=\""""+str(content)+"""\" and o:baidu_directory)
#     RETURN n,m,o,type(r),type(p) LIMIT 300
#     """
#
#     # 执行查询并获取结果
#     result = graph.run(query)
#     # 遍历结果,获取节点和关系
#     result_df = pd.DataFrame([dict(record) for record in result])
#     head=[]
#     relation=[]
#     tail=[]#边和节点分两个文件
#     all_node_json=[]
#     for index, row in result_df.iterrows():
#         all_node_json.append(chuli_string(row['n'], 0))
#         all_node_json.append(chuli_string(row['m'], 0))
#         all_node_json.append(chuli_string(row['o'], 0))
#
#         head.append(chuli_string(row['n'],0)["url"])
#         relation.append(chuli_string(row['type(r)'],1))
#         tail.append(chuli_string(row['m'],0)["url"])
#
#         head.append(chuli_string(row['m'],0)["url"])
#         relation.append(chuli_string(row['type(p)'],1))
#         tail.append(chuli_string(row['o'],0)["url"])
#     # print(len(all_node_json))
#     all_node_json=qu_chong_node(all_node_json)
#     # print(len(all_node_json))
#     length=len(head)
#     data_json=[]
#     for i in range(length):
#         l={}
#         l["head"]=tail[i]
#         l["relation"]=relation[i]
#         l["tail"]=head[i]
#         data_json.append(l)
#     new_file = 'F:\data\\' + str(content) + ".ko-关系.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in data_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#     new_file = 'F:\data\\' + str(content) + ".ko-实体.json"
#     with open(new_file, "w", encoding="utf-8") as file:
#         for data in all_node_json:
#             json.dump(data, file, ensure_ascii=False, indent=4)
#             file.write(",\n")
#     zip_dir='F:\data\\'
#     zip_file1 = str(content) + ".ko-关系.json"
#     zip_file2 = str(content) + ".ko-实体.json"
#     zip_new_file = zip_dir + content + "-索引.zip"
#     # zip = zipfile.ZipFile(zip_new_file, "w", zipfile.ZIP_DEFLATED)
#     # zip.close()
#     try:
#         compress_specific_files(zip_dir,zip_new_file,[zip_file1,zip_file2])
#         response = StreamingHttpResponse(file_iterator(zip_new_file))
#         response['content_type'] = "application/zip"
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         return response
#     except PermissionError:
#         return '文件权限错误，无法访问', 403
#     except Exception as e:
#         return f'发生错误：{str(e)}', 500
#
#     # print(head)
#     # print(relation)
#     # print(tail)
#
#
#     # for record in result:
#     #     node1 = record["n"]
#     #     relationship = record["r"]
#     #     node2 = record["m"]
#     #
#     #     # 处理节点和关系的数据
#     #     print(f"Node1: {node1.encode('utf-8')}")
#     #     print(f"Relationship: {relationship}")
#     #     print(f"Node2: {node2.encode('utf-8')}")
#     #     print("---")