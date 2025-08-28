import sys
import time
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
import zipfile
import os
from py2neo import Graph, Node, Relationship
import hashlib
from neo4j import GraphDatabase
import pandas as pd
import io
import sys
import networkx as nx
import requests
from bs4 import BeautifulSoup
import json
import re
import tqdm
from minio import Minio, InvalidResponseError, S3Error


# MinIO使用bucket（桶）来组织对象。
# bucket类似于文件夹或目录，其中每个bucket可以容纳任意数量的对象。
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

current_date = time.strftime('%Y-%m-%d')
def get_sha1_hash(file_name):

    shal_hash = hashlib.sha1(file_name.encode()).hexdigest()
    return shal_hash
def search_in_neo4j(url):
    print(url)
    graph = Graph("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    query='''
    MATCH(n)
    WHERE
    n.url =\"'''+str(url)+'''\"
    RETURN
    n 
    '''
    result = graph.run(query)
    #
    # if result=="[]":
    #     print("No data")
    #     return "No data"
    #隐含问题，如果返回多个怎么办？
    result_dict = [record.data() for record in result]
    node_json = json.dumps(result_dict, indent=2)
    if node_json=="[]":
        print("No data")
        return "No data"
    print(len(node_json))
    print(node_json)
    return node_json
def create_edge(tx, source_url, target_url,node_type):
    """
    创建一条边关系
    :param tx: 事务对象
    :param source_url: 源节点的 URL
    :param target_url: 目标节点的 URL
    :param relationship_type: 边的类型
    """
    # 使用单个 Cypher 查询语句创建边关系
    tx.run("""
        MERGE (a:hypernode {url: $source_url})
        MERGE (b:"""+node_type+"""{url: $target_url})
        CREATE (a)-[r:Superedge]->(b)
        RETURN type(r) AS relationship_type
    """, source_url=source_url, target_url=target_url)
def neo4j_daoru(url,source):
    # response = requests.get("http://localhost:8088/getRel?userId=6000622&desc=test")
    # # 等待 Java 控制器方法的回执
    # while True:
    #     time.sleep(1)
    #     try:
    #         # 检查是否收到回执
    #         response = requests.get("http://localhost:8088/getResult")
    #         if response.json()["status"] == "success":
    #             print("Insertion successful!")
    #             break
    #         else:
    #             print("Waiting for insertion to complete...")
    #     except requests.exceptions.RequestException:
    #         # 如果 Java 控制器方法还没有返回,则继续等待
    #         continue
    # #以上为日志
    if source == "0":
        string = str(url).split("wiki/")
        file_name = string[len(string) - 1] + ".html"
    else:
        string = str(url).split("baike.baidu.com/item/")
        print(string)
        string = string[len(string) - 1]
        print(string)
        string = string.split("/")
        print(string)
        file_name = string[0] + ".html"
    tmp = file_name.split('.')
    has_name = get_sha1_hash(tmp[0])
    upload_name = has_name + '/' + tmp[0]+".html"
    if source == "1":
        new_path = get_sha1_hash('baidupage')[:2] + '/' + upload_name
    elif source == "0":
        new_path = get_sha1_hash('wikipage')[:2]+'/'+has_name
    graph = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # 定义 Cypher 查询
    if source=="0":
        source_neo4j="wikipage"
    else:
        source_neo4j="baidupage"
    #超点
    query = """
        CREATE (n:hypernode{
            name: $name,
            type: $type,
            url: $url,
            timestamp: $timestamp,
            user_id:$user_id,
            private:$private
        })
        """
    json_data = {
        "name": tmp[0],
        "type": "hypernode",
        "url": "ko.zhonghuapu.com/hypernode/"+tmp[0],
        "timestamp": current_date,
        "user_id": "6000622",
        "private": "0"
    }
    with graph.session() as session:
        result = session.run(query, json_data)
    #页面
    query = """
    MERGE (n:"""+source_neo4j+"""{
        name: $name,
        file_path: $file_path,
        type: $type,
        url: $url,
        timestamp: $timestamp,
        user_id:$user_id,
        private:$private
    })
    """
    print(query)
    json_data={
        "name": tmp[0],
        "file_path": new_path,
        "type":"html",
        "url":url,
        "timestamp":current_date,
        "user_id":"6000622",
        "private":"0"
    }
    print(json_data)
    with graph.session() as session:
        result=session.run(query, json_data)

    #建边
    with graph.session() as session:
        session.write_transaction(create_edge, "ko.zhonghuapu.com/hypernode/"+tmp[0], url,source_neo4j)
    # result=""

    # #以下为日志测试
    # requests.get("http://localhost:8088/finish")

    return result
def upload_html(bucket,folder_path,file_name,model):
    file_path = os.path.join(folder_path, file_name)
    tmp = file_name.split('.')
    has_name = get_sha1_hash(tmp[0])
    print("upload name is",file_name)
    if model == 'wiki':
        upload_name = has_name + '/' + file_name
        bucket.upload_file_to_bucket('kofiles', get_sha1_hash('wikipage')[:2] + '/' + upload_name, file_path)
    else:
        upload_name = has_name + '/' + file_name
        bucket.upload_file_to_bucket('kofiles', get_sha1_hash('baidupage')[:2] + '/' + upload_name, file_path)
def get_upload_html(url,source,bucket):
    if source=="1":
        type="baidu"
    elif source=="0":
        type="wiki"
    try:
        # 设置请求头,模拟浏览器访问
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        # 发起 HTTP 请求并获取响应
        response = requests.get(url, headers=headers)
        folder_path="F:\\data\\"
        if type=="wiki":
            string=str(url).split("wiki/")
            file_name=string[len(string)-1]+".html"
        else:
            string = str(url).split("baike.baidu.com/item/")
            # print(string)
            string = string[len(string) - 1]
            # print(string)
            string = string.split("/")
            # print(string)
            file_name = string[0] + ".html"
        output_file=folder_path+file_name
        # 检查响应状态码是否成功
        if response.status_code == 200:
            # 保存 HTML 内容到文件
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"HTML content saved to: {output_file}")
            print(folder_path," ",file_name)
            upload_html(bucket,folder_path,file_name,type)

        else:
            return "reponse error!!"
    except requests.exceptions.RequestException as e:
        print(f"request error")
        return "request error!!"
def write_neo4j(j,new_path,source):
    graph = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # 定义 Cypher 查询
    if source=="0":
        source_neo4j="wikipage"
    else:
        source_neo4j="baidupage"
    query="""
    MATCH (n:"""+source_neo4j+""") WHERE n.url=\""""+j["n"]["url"] +"""\"Delete n
    """
    result = graph.run(query)

    query = """
        CREATE (n:"""+source_neo4j+"""{
            name: $name,
            file_path: $file_path,
            type: $type,
            url: $url,
            timestamp: $timestamp,
            user_id:$user_id,
            private:$private
        })
        """
    json_data = {
        "name": j["n"]["name"],
        "file_path": new_path,
        "type": "html",
        "url": j["n"]["url"],
        "timestamp": current_date,
        "user_id": "6000622",
        "private": "0"
    }
    print(json_data)
    with graph.session() as session:
        result = session.run(query, json_data)
    # result=""
    return result
def main(request):
    print(1)
    username = request.GET['username']  # 1-百度百科，0-维基百科
    userID = request.GET['userID']
    print(userID)
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    minio_address = "114.213.232.140:19000"
    minio_admin = "minioadmin"
    minio_password = "minioadmin"

    bucket = Bucket(minio_address=minio_address,
                    minio_admin=minio_admin,
                    minio_password=minio_password)
    # 创建桶测试
    bucket.create_one_bucket('kofiles')
    session=driver.session()
    query_word = """          
                    MATCH (n:Strict) 
WHERE n.user_id = {arg_1} 
WITH n
OPTIONAL MATCH (n)  
DETACH DELETE n 
RETURN CASE
    WHEN n IS NOT NULL THEN 'Node deleted successfully'
    ELSE 'Node not found'
END AS message
            """
    result = session.run(query_word, parameters={"arg_1": int(userID)})
    message = result.single()["message"]
    print(message)
    if "successfully" in message:
        return "成功删除您的私有文件！！"
    else:
        return