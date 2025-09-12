import json
import logging
import os

import pymysql
from django.http import JsonResponse
# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')
from query_neo4j.rabbitmq_client_producer import RabbitMQClientProducer
from query_neo4j.disambiguation import Bucket
class MyDirectorySQLDatabase:
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
            # 确保开启自动提交模式
            self.connection.autocommit(True)
            logger.info(f"✅ 数据库连接成功，已开启autocommit")
        except pymysql.MySQLError as e:
            logger.error(f"数据库连接失败：{e}")
            raise
    def get_files_by_entity(self, entity_id):
        """
        根据实体ID获取相关文件列表
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT file_id FROM dir_file WHERE entity_id = %s;"
                cursor.execute(sql, (entity_id,))
                results = cursor.fetchall()
                return results
        except pymysql.MySQLError as e:
            logger.error(f"查询文件失败：{e}")
            raise
    def change_entity_directory(self, user_id, entity_name, category_2, new_categories=None):
        """
        更改实体所属的二级目录,更新其下的三级目录
        所有操作在同一个事务中，任一失败则全部回滚。
        """
        try:
            with self.connection.cursor() as cursor:
                # 开始事务
                self.connection.begin()

                # 1. 先查询获取xiaoqi_id
                sql_select = "SELECT xiaoqi_id FROM xiaoqi_new WHERE xiaoqi_name = %s;"
                cursor.execute(sql_select, (entity_name,))
                result = cursor.fetchone()

                if not result:
                    # 统一抛出异常，让上层调用者清晰地知道失败原因
                    raise ValueError(f"未找到实体: {entity_name}")

                xiaoqi_id = result[0]

                # 2. 更新dir_to_entity表的second字段
                sql_update_dir = "UPDATE dir_to_entity SET `second` = %s WHERE entity_id = %s;"
                cursor.execute(sql_update_dir, (category_2, xiaoqi_id))

                # 3. 更新dir_entity表（如果提供了新分类列表）
                if new_categories is not None:
                    # 先删除该user_id和entity_id对应的所有现有记录
                    sql_delete_category = "DELETE FROM dir_entity WHERE user_id = %s AND entity_id = %s;"
                    cursor.execute(sql_delete_category, (user_id, xiaoqi_id))

                    # 如果有新的分类数据，则批量插入
                    if new_categories:
                        sql_insert_category = """
                        INSERT INTO dir_entity (user_id, entity_id, dir_private, dir_sys)
                        VALUES (%s, %s, %s, %s);
                        """
                        # 性能优化: 使用 executemany 进行批量插入
                        # 根据您的确认，dir_private 和 dir_sys 的值相同
                        data_to_insert = [(user_id, xiaoqi_id, category, category) for category in new_categories]
                        cursor.executemany(sql_insert_category, data_to_insert)

                # 提交事务
                self.connection.commit()

                # !!! 日志信息也同步更新，不再包含 category_1 !!!
                logger.info(f"✅ 实体目录更新成功: {entity_name} -> {category_2}")
                if new_categories is not None:
                    logger.info(f"✅ 同时更新了dir_entity表，处理了{len(new_categories)}条新分类记录")
                return True

        except (pymysql.MySQLError, ValueError) as e:
            # 发生任何数据库错误或我们主动抛出的值错误，都回滚整个事务
            self.connection.rollback()
            logger.error(f"更新实体目录失败，已回滚所有操作：{e}")
            # 将异常重新抛出，以便上层代码能够感知到失败并进行处理
            raise
    def get_all_files(self, entity_name, user_id):
        """
        获取dir_file表中待更改目录实体下文件的ID
        """
        try:
            with self.connection.cursor() as cursor:
                # 1. 先查询获取xiaoqi_id
                sql_select = "SELECT xiaoqi_id,key_words FROM xiaoqi_new WHERE xiaoqi_name = %s;"
                cursor.execute(sql_select, (entity_name,))
                result = cursor.fetchone()

                if not result:
                # 统一抛出异常，让上层调用者清晰地知道失败原因
                    return f"未找到实体: {entity_name}",None

                xiaoqi_id = result[0]
                key_words = result[1]
                sql_select = "SELECT file_id FROM dir_file where dir_id in (SELECT id FROM dir_entity WHERE entity_id = %s AND userid = %s);"
                cursor.execute(sql_select, (xiaoqi_id,user_id,))
                result = cursor.fetchall()

                if not result:
                    return f"未找到实体: {entity_name} 下的文件",None

                ids = [item[0] for item in result]
                # 构造 SQL 查询，使用 IN 语句查询多个 id
                placeholders = ','.join(['%s'] * len(ids))  # 创建占位符，如 "%s, %s, %s"
                sql_select = f"SELECT id,path FROM file WHERE id IN ({placeholders})"

                # 执行查询
                cursor.execute(sql_select, tuple(ids))  # 将 ids 转为元组传入
                id_path_pairs = cursor.fetchall()  # 获取所有匹配的 path
                result = dict(id_path_pairs)  # 直接转为字典

                return f"找到实体: {entity_name} 下的文件",result,xiaoqi_id,key_words


        except pymysql.MySQLError as e:
            logger.error(f"查询所有文件失败：{e}")
            raise
from query_neo4j.disambiguation import extract_text_with_markitdown_safe
def download_files_if_not_exist(bucket_client, bucket_name, file_ids_paths, local_folder_path):
    """
    批量检查并下载文件，返回便于后续处理的结构化结果。

    Args:
        bucket_client (Bucket): 已初始化的Bucket类实例。
        bucket_name (str): Minio中的桶名称。
        file_ids_paths (dict): 键为文件ID，值为文件在Minio中的路径的字典。
        local_folder_path (str): 保存文件的本地文件夹路径。

    Returns:
        dict: 包含处理结果的字典，结构如下:
              {
                  'processed': {file_id_1: 'local/path/to/file1', ...}, # 成功或已存在的文件
                  'failed': {file_id_x: 'error message', ...},          # 失败的文件
                  'summary': {'total': N, 'success': N, 'skipped': N, 'failed': N}
              }
    """
    if not isinstance(file_ids_paths, dict):
        logging.error("参数 file_ids_paths 必须是一个字典。")
        return {
            'processed': {},
            'failed': {'error': '输入参数类型错误，必须是字典'},
            'summary': {'total': 0, 'success': 0, 'skipped': 0, 'failed': 0}
        }

    # 初始化结果容器
    processed_files = {}
    failed_files = {}
    summary = {
        'total': len(file_ids_paths),
        'success': 0,
        'skipped': 0,
        'failed': 0
    }

    logging.info(f"开始批量处理 {summary['total']} 个文件...")

    for file_id, minio_path in file_ids_paths.items():
        local_file_name = os.path.basename(minio_path)
        local_file_path = os.path.join(local_folder_path, local_file_name)

        # 检查文件是否已存在
        if os.path.exists(local_file_path):
            logging.info(f"文件ID {file_id}: 已存在于 '{local_file_path}'，跳过下载。")
            processed_files[file_id] = local_file_path # 将已存在的文件加入可处理列表
            summary['skipped'] += 1
            continue

        # 下载文件
        try:
            os.makedirs(local_folder_path, exist_ok=True)
            result_code = bucket_client.download_file_from_bucket(
                bucket_name=bucket_name,
                minio_file_path=minio_path,
                download_file_path=local_file_path
            )

            if result_code == 1:
                logging.info(f"文件ID {file_id}: 下载成功，已保存至 '{local_file_path}'")
                processed_files[file_id] = local_file_path # 将成功下载的文件加入可处理列表
                summary['success'] += 1
            else:
                message = f"下载操作失败，Minio客户端返回错误。"
                logging.warning(f"文件ID {file_id}: {message} (路径: {minio_path})")
                failed_files[file_id] = message
                summary['failed'] += 1

        except Exception as e:
            message = f"下载过程中发生异常: {e}"
            logging.error(f"文件ID {file_id}: {message} (路径: {minio_path})")
            failed_files[file_id] = message
            summary['failed'] += 1

    logging.info("批量处理完成。")
    return {
        'processed': processed_files,
        'failed': failed_files,
        'summary': summary
    }
def download_and_extract_files(bucket_client, bucket_name, file_ids_paths, local_folder_path):
    """
    工作流函数：下载文件，提取内容，并返回一个包含文件信息的元组列表。

    Args:
        bucket_client (Bucket): Bucket类实例。
        bucket_name (str): Minio中的桶名称。
        file_ids_paths (dict): 要处理的文件字典 {file_id: minio_path}。
        local_folder_path (str): 本地工作目录。

    Returns:
        list: 一个元组列表，每个元组的格式为 (文件名, 文件内容, Minio路径, 文件ID)。
              例如：[('manual.docx', '...', 'docs/manual.docx', 401), ...]
    """
    # 步骤1: 下载文件
    download_results = download_files_if_not_exist(
        bucket_client=bucket_client,
        bucket_name=bucket_name,
        file_ids_paths=file_ids_paths,
        local_folder_path=local_folder_path
    )

    # 初始化要返回的列表
    results_list = []

    files_to_process = download_results.get('processed', {})
    if not files_to_process:
        logging.warning("没有可供提取内容的文件。")
        return []

    logging.info(f"开始从 {len(files_to_process)} 个已就绪文件中提取内容...")
    for file_id, local_path in files_to_process.items():
        try:
            # 准备元组的各个元素
            filename = os.path.basename(local_path)
            doc_type = filename.split('.')[-1].lower() if '.' in filename else ''

            # 提取文件内容
            content = extract_text_with_markitdown_safe(local_path, filename, doc_type)

            # 从原始输入中获取 Minio 路径
            minio_path = file_ids_paths.get(file_id)

            # 将组装好的元组添加到结果列表中
            if minio_path:
                results_list.append((filename, content, minio_path, file_id))
                logging.info(f"文件ID {file_id}: 处理成功并已添加到结果列表。")
            else:
                logging.warning(f"文件ID {file_id}: 未能在原始输入中找到对应的Minio路径，已跳过。")

        except Exception as e:
            logging.error(f"文件ID {file_id}: 在处理过程中发生异常: {e}")

    return results_list
#-------------------------------------------------------------------------------------------
from query_neo4j.rabbitmq_client_producer import RabbitMQClientProducer
def change_directory(request):
    """
    处理更改目录的请求，需要更改实体所属目录，以及实体下所有文件所属目录
    实体所属目录请求已包含
            user_id: 用户ID
            entity_name: 实体名称
            category_1:一级目录
            category_2:二级目录
    文件需要再次分类
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)  # 手动解析JSON
            user_id = data.get('userID')
            entity_name = data.get('entity_name')
            category_2 = data.get('category_2')
            categories_3 = data.get('categories_3', None)  # 可选参数

            db = MyDirectorySQLDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            # 建立数据库连接
            db.connect()
            """获取需要重新分类的文件id与路径"""
            messqge,file_ids_paths,xiaoqi_id,key_words = db.get_all_files(entity_name, user_id)
            if file_ids_paths is None:
                return JsonResponse({"status": "error", "message": messqge}, status=400)
            """获取需要重新分类的文件内容"""
            # 1. 初始化 Bucket 客户端
            minio_address = "114.213.232.140:19000"
            minio_admin = "minioadmin"
            minio_password = "minioadmin"
            bucket_client = Bucket(minio_address=minio_address,
                                   minio_admin=minio_admin,
                                   minio_password=minio_password)
            # 2. 调用批量下载函数,并提取文件内容
            results = download_and_extract_files(
                bucket_client=bucket_client,
                bucket_name='kofiles',
                file_ids_paths=file_ids_paths,
                local_folder_path='D:/upload/'
            )
            # 3. 构造分类接口的数据结构
            #final_output = [{'customize_content': ['执业经历', '获奖经历'], 'entity': '李傲祥1', 'entity_with_keyword': '李傲祥11', 'filetext_dict': {'嫦娥六号_百度百科_1757493641752.html': '文件内容'}, 'info': '律师'}]
            final_output = []
            file_dict_rev = {}
            temp = {}
            temp['customize_content'] = categories_3 if categories_3 is not None else []
            temp['entity'] = entity_name
            temp['entity_with_keyword'] = key_words if key_words is not None else entity_name
            temp['info'] = category_2
            final_output.append(temp)
            final_output[0]['filetext_dict'] = {}
            for file_name, file_content, minio_path, file_id in results:
                final_output[0]['filetext_dict'][file_name] = file_content
                file_dict_rev[minio_path] = file_id

            rbq = RabbitMQClientProducer()
            flag = rbq.send_first_classification_tasks_and_wait(final_output,file_dict_rev,xiaoqi_id,user_id)
            if flag:
                return JsonResponse({"status": "success", "message": "切换成功"}, status=200)
            else:
                return JsonResponse({"status": "error", "message": "切换失败，消息队列处理失败"}, status=500)

        except json.JSONDecodeError:
            return JsonResponse({"status": "error", "message": "无效的JSON格式"}, status=400)