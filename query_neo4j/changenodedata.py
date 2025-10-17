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
def move_directory(db, folder_id_to_move, destination_folder_id):
    """
    移动一个文件夹到另一个文件夹下。
    该函数会自动判断目标是顶层目录还是子目录，并相应地设置parent_id。
    """
    # 验证1：一个文件夹不能被移动到它自己内部
    if folder_id_to_move == destination_folder_id:
        return "移动失败：不能将文件夹移动到其自身内部。"

    try:
        with db.connection.cursor() as cursor:
            # 新增逻辑：判断 destination_folder_id 的类型
            # 查询 dir_entity_more 表中 id 字段是否存在 destination_folder_id
            check_id_query = "SELECT COUNT(*) FROM dir_entity_more WHERE id = %s"
            cursor.execute(check_id_query, (destination_folder_id,))
            is_sub_directory = cursor.fetchone()[0] > 0

            new_parent_id = None
            if is_sub_directory:
                # 如果在 id 字段中找到了，说明目标是一个子目录，parent_id 就是它自己
                new_parent_id = destination_folder_id
            else:
                # 如果在 id 字段中没找到，需要进一步确认它是不是一个顶层目录的ID
                # (根据您的逻辑，此处我们假设它就是顶层目录，因此parent_id应为NULL)
                # 您也可以增加对 dir_entity 表的查询来确保其有效性
                print(f"目标ID {destination_folder_id} 被识别为顶层目录，将parent_id设置为NULL。")
                new_parent_id = None

            # 验证2：防止将父目录移动到其子目录中 (这是一个更复杂的检查，暂时简化)
            # 在实际生产中，需要从 folder_id_to_move 向上遍历其所有父节点，
            # 确保 destination_folder_id 不在其中。

            # 核心更新逻辑
            update_query = """
            UPDATE dir_entity_more
            SET parent_id = %s
            WHERE id = %s;
            """
            # 传入计算出的 new_parent_id 和要移动的文件夹ID
            cursor.execute(update_query, (new_parent_id, folder_id_to_move))

        # autocommit=True, 无需手动 commit
        print(f"文件夹 {folder_id_to_move} 已成功移动。新的 parent_id 为: {new_parent_id}")
        return "Success"

    except pymysql.MySQLError as e:
        print(f"文件夹移动失败：{e}")
        return f"数据库操作失败：{e}"
    except Exception as e:
        return f"未知错误：{e}"
def main(request):
    # ==================== 新增：获取拖拽类型 ====================
    drag_type = request.GET.get("drag_type", "file") # 默认为 file 以兼容旧版前端

    userID = int(request.GET["userID"])

    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",
        password="DMiC-4092",
        database="db_hp"
    )
    db.connect()

    try:
        # ==================== 新增：根据类型进行路由 ====================
        if drag_type == "folder":
            # --- 处理文件夹移动 ---
            folder_id_to_move = int(request.GET["folder_id_to_move"])
            destination_folder_id = int(request.GET["destination_folder_id"])

            # 这里可以添加更多验证，例如检查目标文件夹是否存在且属于该用户

            result = move_directory(db, folder_id_to_move, destination_folder_id)
            return result

        else: # drag_type == "file" 或默认情况
            # --- 处理文件移动（保持原有逻辑） ---
            file_id = int(request.GET["file_id"])
            from_id = int(request.GET["from_id"])
            from_type = request.GET["from_type"]
            to_id = int(request.GET["to_id"])
            to_type = request.GET["to_type"]

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
        return f"拖拽操作失败：{e}"
    finally:
        if 'db' in locals() and db.connection:
            db.connection.close()
