import mysql.connector
from mysql.connector import Error
import pymysql
import requests
import json
from requests.exceptions import RequestException
from query_neo4j.WSD import xiaoqi_instance, jiekou_3, DatabaseHandler


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

    def get_column_as_list(self, table_name, column_name, condition=None, parameters=None):
        """
        从指定表格读取特定列的数据并以列表形式返回

        参数:
        - table_name: 表名
        - column_name: 列名
        - condition: 可选，WHERE条件子句
        - parameters: 可选，条件参数元组

        返回:
        - 包含指定列数据的列表
        """
        try:
            # 构建查询
            query = f"SELECT {column_name} FROM {table_name}"
            if condition:
                query += f" WHERE {condition}"

            # 执行查询
            with self.connection.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)

                # 获取所有结果
                results = cursor.fetchall()

                # 将结果转换为列表
                result_list = [row[0] for row in results if row[0] is not None]

                print(f"成功从表 {table_name} 读取 {column_name} 列，共 {len(result_list)} 条记录")
                return result_list

        except pymysql.MySQLError as e:
            print(f"读取数据失败: {e}")
            return []
    def search_file(self, result, file_dict, file_dict_rev):
        # 优化数据库连接：单次连接处理所有查询
        try:
            self.connect()
            with self.connection.cursor() as cursor:
                for entity in list(result.keys()):  # 遍历副本防止迭代修改
                    file_ids = result[entity][1]
                    # 创建新列表存储有效file_id
                    valid_file_ids = []
                    for file_id in file_ids:
                        # 使用参数化查询防止SQL注入 [[5]]
                        query = "SELECT id, path FROM file WHERE id = %s"
                        cursor.execute(query, (file_id,))
                        row = cursor.fetchone()  # 假设id唯一，使用fetchone()

                        if not row:  # 未查询到结果时跳过
                            continue

                        current_path = row[1]
                        # 检查路径是否以HTTP开头 [[7]]
                        if current_path.lower().startswith(('http://', 'https://')):
                            continue  # 跳过HTTP路径

                        # 保留有效file_id和路径
                        valid_file_ids.append(file_id)
                        file_dict[int(file_id)] = current_path  # 更新路径
                        file_dict_rev[current_path] = int(file_id)
                    # 更新result中的有效file_ids列表
                    if valid_file_ids:
                        result[entity][1] = valid_file_ids
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            self.connection.close()

        return file_dict


    def upload_direct(self, xiaoqi, second_classify):
        # 验证输入数据有效性
        if not second_classify.get('data', {}).get('entity_path'):
            print(f"Invalid input data: {second_classify}")
            return

        original_path = second_classify['data']['entity_path']
        try:
            # 路径转换逻辑
            parts = original_path.strip('\\').split('\\')
            processed_parts = ['KO目录'] + [p for p in parts]
            new_entity_path = ['->'.join(processed_parts)]

            # 数据库操作 - 添加事务支持
            with self.connection.cursor() as cursor:
                try:
                    # 开始事务
                    self.connection.begin()

                    # 检查目录状态（注意表名保持一致性）
                    query_sql = "SELECT directory FROM xiaoqi_new WHERE xiaoqi_name = %s"
                    cursor.execute(query_sql, (xiaoqi,))
                    result = cursor.fetchone()

                    if result is None or result[0] is None:
                        # 使用JSON序列化替代字符串强转（更规范的存储方式）
                        directory_json = json.dumps(new_entity_path, ensure_ascii=False)
                        update_sql = "UPDATE xiaoqi_new SET directory = %s WHERE xiaoqi_name = %s"

                        cursor.execute(update_sql, (directory_json, xiaoqi))

                        # 检查是否实际更新了数据
                        if cursor.rowcount > 0:
                            print(f"✅ 成功更新目录: {xiaoqi}")
                        else:
                            print(f"⚠️ 未找到对应的xiaoqi记录: {xiaoqi}")
                    else:
                        print(f"⚠️ 跳过更新: {xiaoqi} 已存在目录")

                    # 提交事务
                    self.connection.commit()

                except Exception as e:
                    # 发生异常时回滚事务
                    self.connection.rollback()
                    print(f"❌ 数据库操作失败，已回滚: {str(e)}")
                    raise

        except Exception as e:
            print(f"❌ 处理异常: {str(e)}")
            raise  # 重新抛出异常让调用方处理

    def insert_classification_and_entity_data(self, classification_data, entity_id, file_dict_rev=None, userid=None):
        """
        合并的函数：插入分类结果和目录到实体关联，统一管理事务
        参数:
        - classification_data: API返回的分类结果数据
        - entity_id: 实体ID
        - file_dict_rev: 文件字典反向映射（可选，用于分类结果插入）
        - userid: 用户ID（可选，用于分类结果插入）
        """
        try:
            # 开始事务
            self.connection.begin()

            # 验证输入数据
            if classification_data.get('code') != 200 or 'data' not in classification_data:
                print("分类数据格式不正确或API返回错误")
                self.connection.rollback()
                return None

            data = classification_data['data']
            files = data.get('files', {})
            label_1 = data.get('label_1', '')
            label_2 = data.get('label_2', '')

            # 第一部分：插入目录到实体关联（原 insert_dir_toentity 的功能）
            dir_entity_data = {
                'second': label_2,
                'entity_id': entity_id,
            }
            self.insert_data_without_primary('dir_to_entity', dir_entity_data)
            print(f"成功插入目录到实体关联: entity_id={entity_id}, label_2={label_2}")

            # 第二部分：插入分类结果（原 insert_classification_result 的功能）
            results = []
            if file_dict_rev is not None and userid is not None and files:
                # 为每个文件插入记录
                for filename, file_category in files.items():
                    # 获取目录ID（无事务版本）
                    directory_id = self.get_directory_simple(label_2, file_category)

                    # 插入到 dir_to_file 表
                    file_data = {
                        'id': directory_id,
                        'fileid': file_dict_rev[f"bb/{filename}"],
                    }
                    self.insert_data_without_primary('dir_to_file', file_data)

                    # 获取私有目录列表（无事务版本）
                    self.get_dir_private_list_simple(entity_id, userid)

                    # 获取新目录ID
                    pri_dir_id = self.get_new_directory(entity_id, userid, file_category)

                    # 插入到 dir_file 表
                    dir_file_data = {
                        'dir_id': pri_dir_id,
                        'file_id': file_dict_rev[f"bb/{filename}"],
                    }
                    self.insert_data_without_primary("dir_file", dir_file_data)

                print(f"成功插入分类结果: entity_id={entity_id}, 文件数量={len(files)}")

            # 提交事务
            self.connection.commit()
            print(f"所有数据插入成功: entity_id={entity_id}")
            return True

        except Exception as e:
            # 回滚事务
            if hasattr(self, 'connection') and self.connection:
                self.connection.rollback()
            print(f"插入数据失败，事务已回滚: {e}")
            raise e

    def get_new_directory(self, entity_id, userid, label_2):
        """
        根据entity_id和userid查询dir_private字段，并以列表形式返回
        """
        with self.connection.cursor() as cursor:
            sql = """
                  SELECT id 
                  FROM dir_entity 
                  WHERE entity_id = %s AND userid = %s AND dir_private = %s
              """
            cursor.execute(sql, (entity_id, userid, label_2))
            result = cursor.fetchall()
            return result[0]


    def get_dir_private_list(self, entity_id, userid):
        """
        查询dir_entity，如果没结果则查询dir_to_entity -> directory，并写入新内容后返回third列表
        """
        # 开始事务
        connection = self.connection
        try:
            # 开始事务
            connection.begin()

            with connection.cursor() as cursor:
                # 先查 dir_entity
                sql1 = """
                    SELECT dir_private 
                    FROM dir_entity 
                    WHERE entity_id = %s AND userid = %s
                """
                cursor.execute(sql1, (entity_id, userid))
                result = cursor.fetchall()
                if result:
                    # 如果查到结果，提交事务并返回
                    connection.commit()
                    return [row[0] for row in result]

                # 查不到 -> 查 dir_to_entity 获取 second
                sql2 = "SELECT second FROM dir_to_entity WHERE entity_id = %s"
                cursor.execute(sql2, (entity_id,))
                second_result = cursor.fetchone()
                if not second_result:
                    connection.commit()
                    return []  # 如果连 second 都查不到，直接返回空列表

                second = second_result[0]

                # 查 directory 表获取 third
                sql3 = "SELECT third FROM directory WHERE second = %s"
                cursor.execute(sql3, (second,))
                third_results = cursor.fetchall()
                third_list = [row[0] for row in third_results]

                # 批量插入到 dir_entity 表
                insert_sql = """
                    INSERT INTO dir_entity (entity_id, dir_private, dir_sys, userid) 
                    VALUES (%s, %s, %s, %s)
                """
                for third in third_list:
                    cursor.execute(insert_sql, (entity_id, third, third, userid))

            # 所有操作成功，提交事务
            connection.commit()
            return third_list

        except Exception as e:
            # 发生错误，回滚事务
            connection.rollback()
            print(f"获取目录私有列表失败，事务已回滚：{e}")
            raise


    def get_directory(self, label_1, label_2):
        """
        插入或获取目录ID
        """
        try:
            # 检查目录是否已存在
            query = "SELECT id FROM directory WHERE second = %s AND third = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(query, (label_1, label_2))
                result = cursor.fetchone()

                if result:
                    return result[0]  # 返回已存在的目录ID

        except pymysql.MySQLError as e:
            print(f"目录插入或查询失败: {e}")
            self.connection.rollback()
            return None

    def get_directory_simple(self, label_1, label_2):
        """
        插入或获取目录ID（无事务控制版本）
        """
        try:
            # 检查目录是否已存在
            query = "SELECT id FROM directory WHERE second = %s AND third = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(query, (label_1, label_2))
                result = cursor.fetchone()

                if result:
                    return result[0]  # 返回已存在的目录ID
                else:
                    return -1
        except pymysql.MySQLError as e:
            print(f"目录插入或查询失败: {e}")
            raise e  # 抛出异常让外层事务处理

    def get_dir_private_list_simple(self, entity_id, userid):
        """
        查询dir_entity，如果没结果则查询dir_to_entity -> directory，并写入新内容后返回third列表（无事务控制版本）
        """
        try:
            with self.connection.cursor() as cursor:
                # 先查 dir_entity
                sql1 = """
                    SELECT dir_private 
                    FROM dir_entity 
                    WHERE entity_id = %s AND userid = %s
                """
                cursor.execute(sql1, (entity_id, userid))
                result = cursor.fetchall()
                if result:
                    # 如果查到结果，直接返回
                    return [row[0] for row in result]

                # 查不到 -> 查 dir_to_entity 获取 second
                sql2 = "SELECT second FROM dir_to_entity WHERE entity_id = %s"
                cursor.execute(sql2, (entity_id,))
                second_result = cursor.fetchone()
                if not second_result:
                    return []  # 如果连 second 都查不到，直接返回空列表

                second = second_result[0]

                # 查 directory 表获取 third
                sql3 = "SELECT third FROM directory WHERE second = %s"
                cursor.execute(sql3, (second,))
                third_results = cursor.fetchall()
                third_list = [row[0] for row in third_results]

                # 批量插入到 dir_entity 表
                insert_sql = """
                    INSERT INTO dir_entity (entity_id, dir_private, dir_sys, userid) 
                    VALUES (%s, %s, %s, %s)
                """
                for third in third_list:
                    cursor.execute(insert_sql, (entity_id, third, third, userid))

            return third_list

        except Exception as e:
            print(f"获取目录私有列表失败：{e}")
            raise e  # 抛出异常让外层事务处理
    def insert_data(self, table_name, data, primary_key='xiaoqi_name'):
        try:
            existing_id = None
            # 获取主键值（支持自定义主键字段名）
            primary_key_value = data.get(primary_key)

            # 1. 检查记录是否已存在
            check_query = f"SELECT {primary_key}, xiaoqi_id FROM {table_name} WHERE {primary_key} = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value,))
                existing_id = cursor.fetchone()

                if existing_id:
                    print(f"主键 {primary_key_value} 已存在，返回现有ID")
                    return existing_id[1]  # 直接返回已存在记录的ID

            # 2. 执行插入操作
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print("数据插入成功！")
                return cursor.lastrowid  # 返回新插入的ID[1,4](@ref)

        except pymysql.MySQLError as e:
            print(f"插入失败：{e}")
            self.connection.rollback()

            # 3. 插入失败时再次查询已存在ID
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(check_query, (primary_key_value,))
                    existing_id = cursor.fetchone()
                    return existing_id[0] if existing_id else None
            except Exception as e:
                print(f"二次查询失败：{e}")
                return None

    def insert_data_without_primary(self, table_name, data):
        """不包含主键的数据插入（无事务控制版本），依赖外层事务管理"""
        try:
            with self.connection.cursor() as cursor:
                # 构建 WHERE 条件
                conditions = " AND ".join([f"{k} = %s" for k in data.keys()])
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {conditions} FOR UPDATE"

                # 执行检查并加锁
                cursor.execute(check_query, tuple(data.values()))
                result = cursor.fetchone()

                if result[0] > 0:
                    print("数据已存在，跳过插入")
                    return False

                # 执行插入操作
                columns = ", ".join(data.keys())
                placeholders = ", ".join(["%s"] * len(data))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, tuple(data.values()))

                return True

        except pymysql.MySQLError as e:
            print(f"插入操作失败：{e}")
            raise e  # 抛出异常让外层事务处理

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
def jiekou_1(content,userID):
    db_config = {
        'host': '114.213.234.179',
        'user': 'koroot',
        'password': 'DMiC-4092',
        'database': 'db_hp',
    }

    db_handler = DatabaseHandler(db_config)
    db_handler.connect()
    file_dict,docdict,docdict_plus,file_dict_rev = db_handler.fetch_entities_by_keyword(content,userID)
    # for key,value in docdict.items():
    #     print(str(key)+" "+str(value))
    return file_dict,docdict,docdict_plus,file_dict_rev
def jiekou_2(searchname,userID):
    '''
        python环境3.9 需要另外安装以下安装包：

        pip install gensim==3.3.0
        pip install scikit-learn==1.3.0
        pip install scipy==1.12.0
        pip install networkx

        3.10的话gensim调整为4.3.3，这个也是4.3.3的
        '''

    _,docdict,docdict_plus,file_dict_rev=jiekou_1(searchname,userID)
    print(docdict)
    if docdict=={}:
        print("没有任何文件！！！")
        return
    rest = XiaoQi(docdict, searchname)
    # print(rest)
    return rest


def updata_to_mysql_new(result, is_xiaoqi):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",
        password="DMiC-4092",
        database="db_hp"
    )
    entity_file = {}

    try:
        db.connect()
        # 显式开启事务
        db.connection.begin()

        for key, value in result.items():
            if is_xiaoqi:
                data_to_insert = {
                    "key_words": str(value[0]),
                    "xiaoqi_name": str(key)
                }
                # 插入 xiaoqi_new 表并获取 ID
                xiaoqi_id = db.insert_xiaoqi_new("xiaoqi_new", data_to_insert)
                entity_file[key] = xiaoqi_id

            # 批量处理 xiaoqi_to_file 关联
            file_ids = [int(i) for i in value[1]]
            if file_ids:
                with db.connection.cursor() as cursor:
                    # 使用 SELECT ... FOR UPDATE 锁定 xiaoqi_new 记录
                    select_query = """
                        SELECT xiaoqi_id 
                        FROM xiaoqi_new 
                        WHERE xiaoqi_name = %s 
                        FOR UPDATE
                    """
                    cursor.execute(select_query, (key,))
                    xiaoqi_record = cursor.fetchone()

                    if not xiaoqi_record:
                        raise ValueError(f"关联实体 {key} 不存在")

                    xiaoqi_id = xiaoqi_record[0]

                    # 批量插入 xiaoqi_to_file
                    insert_query = """
                        INSERT INTO xiaoqi_to_file (xiaoqi_id, file_id)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE xiaoqi_id = xiaoqi_id
                    """
                    # 使用 executemany 批量插入
                    cursor.executemany(
                        insert_query,
                        [(xiaoqi_id, fid) for fid in file_ids]
                    )

        # 提交事务
        db.connection.commit()
        return entity_file

    except Exception as e:
        # 回滚事务
        db.connection.rollback()
        print(f"数据库操作失败: {e}")
        return {}

    finally:
        db.close()

def get_most_relevant_keywords(file_keywords_list,top_n=5):
    """
    综合筛选与目标人物最相关的关键词

    参数:
        file_keywords_list (list): 每个文件的关键词列表
    返回:
        list: 与目标人物最相关的关键词
    """

    # 基于词频筛选
    high_freq_keywords = filter_by_frequency_and_score(file_keywords_list, top_n=top_n)

    return high_freq_keywords
def jiekou_3(content,userID):
    file_dict,docdict,docdict_plus, file_dict_rev = jiekou_1(content,userID)
    # print(docdict)
    if len(docdict)>1:
        resdoccluster = jiekou_2(content,userID)
        print(1)
        print(resdoccluster)
    elif len(docdict)==1:
        resdoccluster = {}
        for key,value in docdict.items():
            resdoccluster[str(content)+"1"]=[key]
    else:
        return {},[],[]
    docdict = docdict_plus
    result = {}
    if docdict=={}:
        print("没有任何文件！！！")
        return {},[],[]
    for target, filenames in resdoccluster.items():
        relate_filekeyword = []
        temp1=[]
        for filename in filenames:
            # print(filename)
            relate_filekeyword.append(docdict[str(filename)])
            for i11 in docdict[str(filename)]:
                temp1.append(i11)
        unique_data = {item[0]: item for item in temp1}.values()

        temp1 = list(unique_data)
        temp1 = sorted(temp1, key=lambda x: x[1], reverse=True)
        temp = []
        for i in temp1:
            temp.append(i[0])
        relevant_keywords = get_most_relevant_keywords(
            relate_filekeyword,
            top_n=5
        )
        result[target] = [relevant_keywords, filenames]
    # 保存为JSON文件
    return result , file_dict, file_dict_rev

def generate_entity_json(result, file_dict, info, customize_conte):
    output_list = []

    for entity in result:
        # 获取文件ID列表
        file_ids = result[entity][1]

        # 生成minio文件路径列表（网页1、网页2的类型转换思路）
        minio_paths = [file_dict[int(file_id)] for file_id in file_ids]

        # 提取关键词和关联实体（网页3的字典转换思路）
        keywords = result[entity][0]
        merged_keywords = list({k: None for k in keywords}.keys())  # 网页7的去重方法

        # 构建最终结构（网页6的JSON转换技巧）
        output = {
            "minio_file_path_list": minio_paths,
            "entity": entity,
            "entity_with_keyword": f"{entity}: {', '.join(merged_keywords)}",
            "info": info if info is not None else "",
            "customize_conte": customize_conte
        }
        output_list.append(output)

    return output_list


def entity_classification(payload):
    # 接口地址（根据实际环境替换）
    url = "http://114.213.232.140:8000/api/classify/entity/"

    # 请求头配置（网页3、网页6的header设置方法）
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonClient/1.0"  # 网页8的请求头伪装技巧
    }


    try:
        # 发送POST请求（网页1、网页7的核心方法）
        response = requests.post(
            url,
            headers=headers,
            json=payload,  # 网页6推荐的json参数自动序列化
            timeout=10  # 网页9的超时设置
        )

        # 状态码检查（网页4的错误处理机制）
        if response.status_code == 200:
            # 解析JSON响应（网页3、网页7的响应处理方法）
            result = response.json()
            print("接口调用成功，响应数据：")
            print(json.dumps(result, indent=2, ensure_ascii=False))

            # 此处可添加业务逻辑处理
            # 例如：result.get("classification_result")

            return result
        else:
            print(f"接口调用失败，状态码：{response.status_code}")
            print(f"错误详情：{response.text}")
            return False

    except RequestException as e:  # 网页4的异常捕获机制
        print(f"请求异常：{str(e)}")
        return False
    except json.JSONDecodeError:  # 网页7的JSON解析异常处理
        print("响应数据解析失败")
        return False

def create_strict_and_coaser(tx, id, direct):
    query = """
        MATCH (a:Strict) WHERE id(a) = $id    
        MATCH (b:coarse {name: $direct})       
        MERGE (a)-[r:edge]->(b)
         RETURN r IS NOT NULL AS exists   
        """
    # RETURN id(f) AS file_id, id(h) AS hypernode_id, id(k) AS category_id
    result = tx.run(
        query,
        id=id,
        direct=direct
    )
    return result.single()["exists"]

def second_classification(payload):
    # 接口地址（根据实际环境替换）
    url = "http://114.213.232.140:8000/api/classify/big-content_last/"

    # 请求头配置（网页3、网页6的header设置方法）
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonClient/1.0"  # 网页8的请求头伪装技巧
    }
    try:
        # 发送POST请求（网页1、网页7的核心方法）
        response = requests.post(
            url,
            headers=headers,
            json=payload,  # 网页6推荐的json参数自动序列化
            # timeout=10  # 网页9的超时设置
        )

        # 状态码检查（网页4的错误处理机制）
        if response.status_code == 200:
            # 解析JSON响应（网页3、网页7的响应处理方法）
            result = response.json()

            return result
        else:
            print(f"接口调用失败，状态码：{response.status_code}")
            print(f"错误详情：{response.text}")
            return False
    except json.JSONDecodeError:  # 网页7的JSON解析异常处理
        print("响应数据解析失败")
        return False

def first_classification(payload):
    # 接口地址（根据实际环境替换）
    url = "http://114.213.232.140:8000/api/classify/entity/"

    # 请求头配置（网页3、网页6的header设置方法）
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonClient/1.0"  # 网页8的请求头伪装技巧
    }

    try:
        # 发送POST请求（网页1、网页7的核心方法）
        response = requests.post(
            url,
            headers=headers,
            json=payload,  # 网页6推荐的json参数自动序列化
            # timeout=10  # 网页9的超时设置
        )

        # 状态码检查（网页4的错误处理机制）
        if response.status_code == 200:
            # 解析JSON响应（网页3、网页7的响应处理方法）
            result = response.json()

            # 此处可添加业务逻辑处理
            # 例如：result.get("classification_result")

            return result
        else:
            print(f"接口调用失败，状态码：{response.status_code}")
            print(f"错误详情：{response.text}")
            return False

    except json.JSONDecodeError:  # 网页7的JSON解析异常处理
        print("响应数据解析失败")
        return False
def main(request):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    entity = request.GET["name"]
    userID = request.GET["userID"]
    file_dict = {}
    file_dict_rev = {}
    result, _, _ = jiekou_3(entity, userID)
    if result == {}:
        return json.dumps(result, ensure_ascii=False)
    entity_dict = updata_to_mysql_new(result, True)
    file_dict = db.search_file(result, file_dict, file_dict_rev)
    final_output = generate_entity_json(result, file_dict, None, [])
    for payload in final_output:
        first_classify = first_classification(payload)
        second_classify = second_classification(payload)
        try:
            db.connect()
            db.insert_classification_and_entity_data(first_classify, entity_dict[payload["entity"]], file_dict_rev, userID)
            db.upload_direct(payload["entity"], second_classify)

            with driver.session() as session:
                for entity in result:
                    # 获取文件ID列表
                    file_ids = result[entity][1]
                    for file_id in file_ids:
                        direct = second_classify['data']['file_path'][file_dict[file_id].split('/')[-1]]
                        result = session.write_transaction(
                            create_strict_and_coaser,
                            id=file_id,
                            direct=direct if direct != "Root" else "KO目录",
                        )
                        if not result:
                            print("上传失败 error")
        finally:
            # 关闭数据库连接
            db.close()
    return json.dumps(result, ensure_ascii=False)

