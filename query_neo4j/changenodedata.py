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
def change_file_dire(db,file_id,old_dire_id,new_dire_id,xiaoqi_id,userID,new_dire,old_dire):

    #绝了，拖拽结束的目标大概率会定位到文件？？？？
    #     query = """
    #             Update dir_file
    #             Set dir_id=%s
    # WHERE dir_id=%s
    # AND file_id = %s;
    #         """
    if "." in new_dire:
        query = """
        
        select df.dir_id
from dir_file AS df
Join dir_entity As de ON de.id=df.dir_id and de.userid=%s
Join `file`
where `file`.name=%s and `file`.id=df.file_id 
                    """
        try:
            with db.connection.cursor() as cursor:
                cursor.execute(query, (int(userID),str(new_dire)))
                result = cursor.fetchall()
                new_dire_id=result[0][0]
                # db.connection.commit()
        except pymysql.MySQLError as e:
            return "修改失败" + str(e)
            raise
    # if "." in old_dire:
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
    #                 cursor.execute(query, (old_dire, int(userID)))
    #                 result = cursor.fetchall()
    #                 old_dire_id = result[0]
    #                 # db.connection.commit()
    #         except pymysql.MySQLError as e:
    #             return "修改失败" + str(e)
    #             raise
    print("dire_id is :"+str(new_dire_id))
    query = """
                UPDATE dir_file df
JOIN dir_entity de ON df.dir_id = de.id
SET df.dir_id = %s
WHERE df.file_id = %s
AND de.entity_id = %s
AND de.userid=%s;
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(new_dire_id),int(file_id),int(xiaoqi_id),int(userID)))
            result = cursor.fetchall()
            db.connection.commit()
    except pymysql.MySQLError as e:
        return "修改失败"+str(e)
        raise
    return "Success"
def main(request):
    userID = int(request.GET["userID"])
    content=request.GET["content"]
    old_dire_name = request.GET["old_dire_name"]
    new_dire_name = request.GET["new_dire_name"]
    fileid=int(request.GET["fileid"])



    print(userID)
    print(old_dire_name)
    print(new_dire_name)
    print(content)
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    # result=search_content_from_mysql(content,userID,db)
    current_date = datetime.datetime.now()
    year = current_date.year
    month = current_date.month
    day = current_date.day
    formatted_date = f"{year}-{month}-{day}"
    temp_xiaoqi_id=find_xiaoqi_id(db,content)
    old_dire_id=find_dire(userID,old_dire_name,temp_xiaoqi_id,db)
    new_dire_id=find_dire(userID,new_dire_name,temp_xiaoqi_id,db)
    print(old_dire_id)
    print(new_dire_id)
    # if new_dire_id==None:
    #     return "修改失败，目标目录为空！！！"
    return change_file_dire(db,fileid,old_dire_id,new_dire_id,temp_xiaoqi_id,userID,new_dire_name,old_dire_name)