import os
from minio import Minio, InvalidResponseError, S3Error
from neo4j import GraphDatabase
import json
import zipfile
import hashlib
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
import datetime
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
    query_word = ("MATCH (h) WHERE id(h)=$nodeID RETURN h,labels(h)")
    result = tx.run(query_word, name=int(userID),nodeID=int(nodeID))
    return result.data()


def add_file_to_zip(zip_path, file_path):
    # 确保 ZIP 文件路径存在
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)

    # 打开 ZIP 文件，如果文件不存在则创建
    with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_DEFLATED) as zipf:
        # 添加文件到 ZIP
        zipf.write(file_path)
def main(request):
    nodeID = request.GET['node_ID']
    userID= request.GET['userID']
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session=driver.session()
    data=session.write_transaction(search_file_in_Neo4j, userID,nodeID)
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month
    day = current_date.day
    minute = current_date.minute
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
        zip_file = 'F:\\data\\'
        # 检查第一个地址是否存在
        if not os.path.exists(zip_file):
            # 如果不存在,创建该目录
            os.makedirs(zip_file)
            print(f"已创建目录: {zip_file}")
        print(data)
        if ("baidupage" in data[0]["labels(h)"]) or ("wikipage" in data[0]["labels(h)"]):
            real_file_path=file_data["file_path"]
        else:
            real_file_path=file_data["path"]
        file_path = zip_file + str(real_file_path).split("/")[-1]
        print(file_path)
        # ret = bucket.get_list_objects_from_bucket_dir('kofiles',
        #                                               file_data["path"])
        # if len(ret)==0:
        #     return "抱歉，系统未保存该实体文件!"
        # else:
        #     with open(zip_file + str(file_data["path"]).split("/")[-1], 'w', encoding='utf-8') as file:
        #         file.write(ret[0])
        #     file_path=zip_file + str(file_data["path"]).split("/")[-1]
        #     print(file_path)
        ret = bucket.download_file_from_bucket('kofiles',real_file_path,file_path)
        if ret==0:
            return "抱歉，系统未保存该实体文件!"
        # 设置文件的 MIME 类型
        mime_type = 'application/octet-stream'  # 或者根据文件类型设置更合适的 MIME 类型

        # 创建 StreamingHttpResponse 对象
        response = StreamingHttpResponse(open(file_path, 'rb'), content_type=mime_type)

        # 设置响应头
        response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(os.path.basename(file_path)))

        return response
        # try:
        #     add_file_to_zip(zip_new_file,file_path)
        #     response = StreamingHttpResponse(file_iterator(zip_new_file))
        #     response['content_type'] = "application/zip"
        #     response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
        #     response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
        #     return response
        # except PermissionError:
        #     return '文件权限错误，无法访问', 403
        # except Exception as e:
        #     return f'发生错误：{str(e)}', 500

# import os
# from minio import Minio, InvalidResponseError, S3Error
# from neo4j import GraphDatabase
# import json
# import zipfile
# import hashlib
# from django.http import HttpResponse, Http404, StreamingHttpResponse
# from django.utils.encoding import escape_uri_path
# import datetime
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
#                 return 1
#             except FileNotFoundError as err:
#                 print('download_failed: ' + str(err))
#                 return 0
#             except S3Error as err:
#                 print("download_failed:", err)
#                 return 0
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
# def search_file_in_Neo4j(tx,userID,nodeID):
#     query_word = ("MATCH (h) WHERE h.user_id=$name and id(h)=$nodeID RETURN h")
#     result = tx.run(query_word, name=int(userID),nodeID=int(nodeID))
#     return result.data()
#
#
# def add_file_to_zip(zip_path, file_path):
#     # 确保 ZIP 文件路径存在
#     os.makedirs(os.path.dirname(zip_path), exist_ok=True)
#
#     # 打开 ZIP 文件，如果文件不存在则创建
#     with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_DEFLATED) as zipf:
#         # 添加文件到 ZIP
#         zipf.write(file_path)
# def main(request):
#     nodeID = request.GET['node_ID']
#     userID= request.GET['userID']
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     session=driver.session()
#     data=session.write_transaction(search_file_in_Neo4j, userID,nodeID)
#     current_date = datetime.datetime.now()
#     year = current_date.year
#     month = current_date.month
#     day = current_date.day
#     minute = current_date.minute
#     if len(data)<=0:
#         return "不存在该文件！"
#     else:
#         file_data=data[0]["h"]
#         minio_address = "114.213.232.140:19000"
#         minio_admin = "minioadmin"
#         minio_password = "minioadmin"
#
#         bucket = Bucket(minio_address=minio_address,
#                         minio_admin=minio_admin,
#                         minio_password=minio_password)
#         zip_file = 'F:\\data\\'
#         # 检查第一个地址是否存在
#         if not os.path.exists(zip_file):
#             # 如果不存在,创建该目录
#             os.makedirs(zip_file)
#             print(f"已创建目录: {zip_file}")
#         file_path = zip_file + str(file_data["path"]).split("/")[-1]
#         print(file_path)
#         # ret = bucket.get_list_objects_from_bucket_dir('kofiles',
#         #                                               file_data["path"])
#         # if len(ret)==0:
#         #     return "抱歉，系统未保存该实体文件!"
#         # else:
#         #     with open(zip_file + str(file_data["path"]).split("/")[-1], 'w', encoding='utf-8') as file:
#         #         file.write(ret[0])
#         #     file_path=zip_file + str(file_data["path"]).split("/")[-1]
#         #     print(file_path)
#         ret = bucket.download_file_from_bucket('kofiles',file_data["path"],file_path)
#         if ret==0:
#             return "抱歉，系统未保存该实体文件!"
#         # 设置文件的 MIME 类型
#         mime_type = 'application/octet-stream'  # 或者根据文件类型设置更合适的 MIME 类型
#
#         # 创建 StreamingHttpResponse 对象
#         response = StreamingHttpResponse(open(file_path, 'rb'), content_type=mime_type)
#
#         # 设置响应头
#         response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(os.path.basename(file_path)))
#
#         return response
#         # try:
#         #     add_file_to_zip(zip_new_file,file_path)
#         #     response = StreamingHttpResponse(file_iterator(zip_new_file))
#         #     response['content_type'] = "application/zip"
#         #     response['Access-Control-Expose-Headers'] = "Content-Disposition, Content-Type"
#         #     response['Content-Disposition'] = 'attachment; filename={}'.format(escape_uri_path(zip_new_file))
#         #     return response
#         # except PermissionError:
#         #     return '文件权限错误，无法访问', 403
#         # except Exception as e:
#         #     return f'发生错误：{str(e)}', 500
