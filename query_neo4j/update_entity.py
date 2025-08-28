import pymysql
import datetime
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

def update_new_data_mysql(nodeID,content,new_id,entity_list,userID,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    data_to_insert = {
        "xiaoqi_name": str(content)+str(new_id),  # 替换为实际数据
        "key_words": str(entity_list)
    }
    # print(key)
    db.insert_data("xiaoqi_new", data_to_insert)
    db.connection.commit()
    search_string=str(content)+str(new_id)
    query = """
                INSERT INTO xiaoqi_to_file (xiaoqi_id, file_id) 
    SELECT 
        xnew.xiaoqi_id, %s 
    FROM 
        xiaoqi_new xnew 
    WHERE 
        xnew.xiaoqi_name = %s
    LIMIT 1; 
                """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(nodeID), search_string))
            db.connection.commit()
            # 检查受影响的行数
            if cursor.rowcount > 0:
                print(f'更新成功，受影响的行数: {cursor.rowcount}')
            else:
                print('没有找到匹配的行，更新未执行。')
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    return
def update_to_old_data_mysql(nodeID,content,new_id,userID,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    print("add!")
    search_string =str(content) + str(new_id)  # 匹配居然还要加通配符
    # print(add_string)
    print(search_string)
    print(nodeID)
    # print(int(userID))
    query = """
            INSERT INTO xiaoqi_to_file (xiaoqi_id, file_id) 
SELECT 
    xnew.xiaoqi_id, %s 
FROM 
    xiaoqi_new xnew 
WHERE 
    xnew.xiaoqi_name = %s
LIMIT 1; 
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(nodeID), search_string))
            db.connection.commit()
            # 检查受影响的行数
            if cursor.rowcount > 0:
                print(f'更新成功，受影响的行数: {cursor.rowcount}')
            else:
                print('没有找到匹配的行，更新未执行。')
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise

def delete_old_data_mysql(nodeID,content,old_id,userID,db):
    """
        根据给定的 content 查询 entity_to_file 表中的 file_id，
        并返回 file 表中所有匹配的行的信息。
        """
    delete_xiaoqi_query = """
        DELETE FROM xiaoqi_to_file  
        WHERE file_id = %s 
          AND xiaoqi_id = (
              SELECT xiaoqi_id 
              FROM xiaoqi_new 
              WHERE xiaoqi_name = %s 
              LIMIT 1
          );
    """
    try:
        with db.connection.cursor() as cursor:
            # 先删除 entity_to_file 表中的记录
            # print(search_string)
            # print(replace_string)
            search_string = str(content) + str(old_id)
            cursor.execute(delete_xiaoqi_query, (int(nodeID),search_string))

            # 提交事务
            db.connection.commit()
    except pymysql.MySQLError as e:
        print(f"删除失败：{e}")
        db.connection.rollback()  # 回滚事务
        raise
def change_status(userID,nodeID,content,old_ID,new_id,update_time,db):
    query = """
            UPDATE user_alter_data
            SET status = "executed"
            WHERE nodeID=%s and xiaoqi_from_id=%s and xiaoqi_to_id=%s
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
        print(f"删除失败：{e}")
        db.connection.rollback()  # 回滚事务
        raise
def main(request):
    method = request.GET["method"] #1表示修改到已有的实体，0表示修改到新的实体
    nodeID = request.GET["nodeID"]#这个文件的ID
    new_id = request.GET["new_ID"]#新实体的编号 比如汪萌1——》汪萌3，这个就是3
    content= request.GET["content"]#这个实体的名字
    old_ID = request.GET["old_ID"]#老实体的编号 比如汪萌1——》汪萌3，这个就是1
    userID=request.GET["userID"]#用户ID，这个不用多说，前端我写了个get_user的函数
    update_time=request.GET["update_time"]
    print(method)
    if (int(method)==0):
        entity_list=request.GET["entity_list"]#用户输入的标识词列表，最好搞成数组格式后再转string传过来，最次也得有个逗号分隔不同的标识词
    else:
        entity_list="[]"
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    if (int(method)==0):
        update_new_data_mysql(nodeID,content,new_id,entity_list,userID,db)
    else:
        update_to_old_data_mysql(nodeID,content,new_id,userID,db)
    delete_old_data_mysql(nodeID,content,old_ID,userID,db)
    change_status(userID,nodeID,content,old_ID,new_id,update_time,db)