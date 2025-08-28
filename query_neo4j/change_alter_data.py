import pymysql
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
def change_status(userID,nodeID,content,old_ID,new_id,update_time,db):
    query = """
            UPDATE user_alter_data
            SET status = "rejected"
            WHERE nodeID=%s and xiaoqi_from_id=%s and xiaoqi_to_id=%s;
        """
    try:
        with db.connection.cursor() as cursor:
            # 先删除 entity_to_file 表中的记录
            # print(search_string)
            # print(replace_string)
            cursor.execute(query, (int(nodeID), int(old_ID),int(new_id)))

            # 提交事务
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"修改状态失败：{e}")
        db.connection.rollback()  # 回滚事务
        raise
def delete_info(userID,nodeID,content,old_ID,new_id,update_time,db):
    query = """
                DELETE FROM user_alter_data
                WHERE nodeID=%s and xiaoqi_from_id=%s and xiaoqi_to_id=%s;
            """
    try:
        with db.connection.cursor() as cursor:
            # 先删除 entity_to_file 表中的记录
            # print(search_string)
            # print(replace_string)
            cursor.execute(query, (int(nodeID), int(old_ID), int(new_id)))
            # 提交事务
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"修改状态失败：{e}")
        db.connection.rollback()  # 回滚事务
        raise
def main(request):
    method = request.GET["method"]  # 1表示拒绝，0表示用户直接撤销
    nodeID = request.GET["nodeID"]  # 这个文件的ID
    new_id = request.GET["new_ID"]  # 新实体的编号 比如汪萌1——》汪萌3，这个就是3
    content = request.GET["content"]  # 这个实体的名字
    old_ID = request.GET["old_ID"]  # 老实体的编号 比如汪萌1——》汪萌3，这个就是1
    userID = request.GET["userID"]  # 用户ID，这个不用多说，前端我写了个get_user的函数
    update_time = request.GET["update_time"]
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    method=int(method)
    if (method==1):
        change_status(userID, nodeID, content, old_ID, new_id, update_time,db)
    else:
        delete_info(userID,nodeID,content,old_ID,new_id,update_time,db)
    return 0