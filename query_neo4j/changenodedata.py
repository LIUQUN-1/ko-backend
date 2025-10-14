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
            self.connection = pymysql.connect(**self.config,autocommit=True)
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
def change_dire_name(userID,old_dire_name,new_dire_name,content,db):
    query="""
    UPDATE dir_entity de
JOIN xiaoqi_new xn ON de.entity_id = xn.xiaoqi_id
SET de.dir_private = %s
WHERE 
    xn.xiaoqi_name = %s 
    AND de.userid = %s 
    AND de.dir_private = %s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (new_dire_name,content,int(userID),old_dire_name))
            # result = cursor.fetchall()
            db.connection.commit()
            # db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def add_dire(userID,content,name,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    # print(int(userID))
    query = """
        select xn.xiaoqi_id
        from xiaoqi_new xn
    WHERE 
        xn.xiaoqi_name = %s 
        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (content))
            result_temp = cursor.fetchall()
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    xiaoqi_id=1
    for i in result_temp:
        xiaoqi_id=i[0]
    try:
        file_data = {
            'entity_id': int(xiaoqi_id),
            'dir_private': name,
            'dir_sys': name,
            'userid': int(userID)
        }
        with db.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dir_entity (entity_id, dir_private, dir_sys, userid) VALUES (%s, %s, %s, %s)",
                (int(file_data["entity_id"]),
                 file_data["dir_private"],
                 file_data["dir_sys"],
                 int(file_data["userid"])
                 ))
            # 确保提交事务
            db.connection.commit()  # 假设connection是您的数据库连接对象

            last_id = cursor.lastrowid
            if last_id is None:
                raise ValueError("无法获取插入记录的ID，可能表没有自增主键")

            return last_id

    except ValueError as e:
        # 处理int转换错误
        db.connection.rollback()
        raise ValueError(f"数据类型转换错误: {str(e)}")
    except Exception as e:
        # 其他数据库错误
        db.connection.rollback()
        raise Exception(f"数据库操作失败: {str(e)}")
def delete_dire(userID,dir_name,db):
    query = """
            DELETE FROM dir_entity
WHERE userid = %s
AND dir_private = %s;
        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID), dir_name))
            result = cursor.fetchall()
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def find_dire(userID,dir_name,xiaoqi_id,db):
    query = """
            SELECT id
            FROM dir_entity
WHERE userid = %s
AND entity_id=%s
AND dir_private = %s;

        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID), int(xiaoqi_id),dir_name,))
            result = cursor.fetchall()
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    for i in result:
        return i[0]
def find_xiaoqi_id(db,content):
    query = """
            select xn.xiaoqi_id
            from xiaoqi_new xn
        WHERE 
            xn.xiaoqi_name = %s 
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (content))
            result_temp = cursor.fetchall()
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    xiaoqi_id = 1
    for i in result_temp:
        xiaoqi_id = i[0]
    return xiaoqi_id
# def change_file_dire(db,file_id,old_dire_id,new_dire_id,xiaoqi_id,userID,new_dire,old_dire):
#
#     #绝了，拖拽结束的目标大概率会定位到文件？？？？
#     #     query = """
#     #             Update dir_file
#     #             Set dir_id=%s
#     # WHERE dir_id=%s
#     # AND file_id = %s;
#     #         """
#     if "." in new_dire:
#         query = """
#
#         select df.dir_id
# from dir_file AS df
# Join dir_entity As de ON de.id=df.dir_id and de.userid=%s
# Join `file`
# where `file`.name=%s and `file`.id=df.file_id
#                     """
#         try:
#             with db.connection.cursor() as cursor:
#                 cursor.execute(query, (int(userID),str(new_dire)))
#                 result = cursor.fetchall()
#                 new_dire_id=result[0][0]
#                 # db.connection.commit()
#         except pymysql.MySQLError as e:
#             return "修改失败" + str(e)
#             raise
#     # if "." in old_dire:
#     #         query = """
#     #
#     #         select df.dir_id
#     # from dir_file AS df
#     # Join dir_entity As de ON de.id=df.dir_id and de.userid=%s
#     # Join `file`
#     # where `file`.name=%s and `file`.id=df.file_id
#     #                     """
#     #         try:
#     #             with db.connection.cursor() as cursor:
#     #                 cursor.execute(query, (old_dire, int(userID)))
#     #                 result = cursor.fetchall()
#     #                 old_dire_id = result[0]
#     #                 # db.connection.commit()
#     #         except pymysql.MySQLError as e:
#     #             return "修改失败" + str(e)
#     #             raise
#     print("dire_id is :"+str(new_dire_id))
#     query = """
#                 UPDATE dir_file df
# JOIN dir_entity de ON df.dir_id = de.id
# SET df.dir_id = %s
# WHERE df.file_id = %s
# AND de.entity_id = %s
# AND de.userid=%s;
#             """
#     try:
#         with db.connection.cursor() as cursor:
#             cursor.execute(query, (int(new_dire_id),int(file_id),int(xiaoqi_id),int(userID)))
#             result = cursor.fetchall()
#             db.connection.commit()
#     except pymysql.MySQLError as e:
#         return "修改失败"+str(e)
#         raise
#     return "Success"
def change_file_dire(db, file_id, from_id, from_type, to_id, to_type, xiaoqi_id, userID):

    # 1. 确定删除操作的目标表和 ID 字段
    if from_type == 'dir_entity':
        delete_table = 'dir_file'
        delete_col = 'dir_id' # 对应 dir_entity.id
    else: # 'dir_entity_more'
        delete_table = 'dir_more_file'
        delete_col = 'dir_more_id' # 对应 dir_entity_more.id

    # 2. 执行删除操作 (从旧目录解绑文件)
    delete_query = f"""
    DELETE FROM {delete_table}
    WHERE file_id = %s AND {delete_col} = %s;
    """

    # 3. 确定插入操作的目标表和 ID 字段
    if to_type == 'dir_entity':
        insert_table = 'dir_file'
        insert_col = 'dir_id'
    else: # 'dir_entity_more'
        insert_table = 'dir_more_file'
        insert_col = 'dir_more_id'

    # 4. 执行插入操作 (绑定文件到新目录)
    insert_query = f"""
    INSERT INTO {insert_table} ({insert_col}, file_id)
    VALUES (%s, %s);
    """

    try:
        with db.connection.cursor() as cursor:
            # 执行删除
            cursor.execute(delete_query, (file_id, from_id))

            # 执行插入 (使用新的目标ID和文件ID)
            cursor.execute(insert_query, (to_id, file_id))

        # db.connection.commit() # MySQLDatabase 已经设置为 autocommit=True，理论上不需要
        return "Success"

    except pymysql.MySQLError as e:
        # print(f"文件移动失败：{e}")
        return "文件移动失败：" + str(e)
    except Exception as e:
        # print(f"文件移动失败：{e}")
        return "文件移动失败：" + str(e)
def main(request):
    userID = int(request.GET["userID"])
    file_id = int(request.GET["file_id"]) # 文件 ID

    from_id = int(request.GET["from_id"])     # 源目录 ID
    from_type = request.GET["from_type"]      # 'dir_entity' 或 'dir_entity_more'

    to_id = int(request.GET["to_id"])         # 目标目录 ID
    to_type = request.GET["to_type"]          # 'dir_entity' 或 'dir_entity_more'
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    try:
        # 获取 xiaoqi_id
        # 因为文件ID已经包含了 xiaoqi_id 的信息，这里从 file_id 对应的 xiaoqi_id 处获取
        query_xiaoqi = """
        SELECT xn.xiaoqi_id
        FROM xiaoqi_new xn
        JOIN xiaoqi_to_file xtf ON xn.xiaoqi_id = xtf.xiaoqi_id
        WHERE xtf.file_id = %s LIMIT 1;
        """
        with db.connection.cursor() as cursor:
            cursor.execute(query_xiaoqi, (file_id,))
            xiaoqi_id_result = cursor.fetchone()
            if not xiaoqi_id_result:
                return "文件找不到对应实体"
            xiaoqi_id = xiaoqi_id_result[0]


        result = change_file_dire(db, file_id, from_id, from_type, to_id, to_type, xiaoqi_id, userID)
        return result
    except Exception as e:
        print(f"拖拽操作失败：{e}")
        return "拖拽操作失败：" + str(e)
    finally:
        if 'db' in locals() and db.connection:
            db.connection.close()
