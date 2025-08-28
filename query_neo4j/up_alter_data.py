import pymysql
import datetime
# 使用示例
import json
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

    def insert_data_without_primary(self, table_name, data):
        try:
            # # 先检查主键是否存在
            # primary_key = list(data.keys())[0]  # 假设主键在第一个位置
            # primary_key_value = data[primary_key]
            #
            # # 生成检查主键是否存在的 SQL 查询
            # check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s"
            # with self.connection.cursor() as cursor:
            #     cursor.execute(check_query, (primary_key_value,))
            #     result = cursor.fetchone()
            #
            #     if result[0] > 0:
            #         print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
            #         return  # 主键已存在，跳过插入操作

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
def get_name(userID,db):
    query="""
    SELECT username 
FROM tb_user 
WHERE id = %s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID)))
            result = cursor.fetchall()
            return result[0][0]
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def main(request):
    nodeID = request.GET["nodeID"]
    userID = request.GET["userID"]
    nodeName = request.GET["nodeName"]
    alter_xiaoqi_to = request.GET["alter_xiaoqi_to"]
    print(alter_xiaoqi_to)
    altered_xiaoqi_index = request.GET["altered_xiaoqi_index"]
    alter_xiaoqi_to_index = request.GET["alter_xiaoqi_to_index"]
    entity_list=request.GET["entity_list"]
    content=request.GET["content"]

    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    user_name=get_name(userID,db)
    # result=search_content_from_mysql(content,userID,db)
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month
    day = current_date.day
    formatted_date = f"{year}-{month}-{day}"
    data_to_insert = {
        "userid": int(userID),
        "nodeID": int(nodeID),
        "nodeName": str(nodeName),
        "xiaoqi_to_name":str(alter_xiaoqi_to),
        "xiaoqi_from_id":int(altered_xiaoqi_index),
        "xiaoqi_to_id":int(alter_xiaoqi_to_index),
        "username":str(user_name),
        "update_time":str(formatted_date),
        "entity_list":str(entity_list),
        "content":str(content),
        "status":"waiting"
    }
    # print(key)
    db.insert_data_without_primary("user_alter_data", data_to_insert)
    return 0