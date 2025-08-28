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
                print("数据插入成功！")

        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            raise
        except Exception as e:
            print(f"插入数据时发生未知错误：{e}")
            raise

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
                print("数据插入成功！")

        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            raise
        except Exception as e:
            print(f"插入关系数据时发生未知错误：{e}")
            connection.rollback()  # 回滚事务
            raise

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭！")
def search_content_from_mysql_new(xiaoqi_id,userID,db):
    query = """
            SELECT
            xnf.xiaoqi_name,
            xnf.directory,
            df.file_id
			FROM
			dir_entity de 
        JOIN
            dir_file df ON df.dir_id=de.id
        JOIN 
            file f ON (df.file_id = f.id AND (f.private = 0 OR f.userid = %s))
        JOIN 
            xiaoqi_new xnf ON de.entity_id = xnf.xiaoqi_id
		JOIN 
			xiaoqi_to_file xtf ON df.file_id=xtf.file_id and xtf.xiaoqi_id=xnf.xiaoqi_id
        WHERE 
            xnf.xiaoqi_id=%s and de.userid=%s;
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID),int(xiaoqi_id),int(userID)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询消歧实体失败：{e}")
        raise
def get_content_info(content_id,db):#用来处理用户没有共享文件但是还要显示实体信息的情况
    query = """
                SELECT
                xnf.xiaoqi_name,
                xnf.directory
    			FROM
    			xiaoqi_new xnf
            WHERE 
                xnf.xiaoqi_id=%s
                """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(content_id)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询消歧实体失败：{e}")
        raise
def chuli_result(data,content):
    result = {}
    print(data)
    for name, attributes, file_id in data:
        # 将字符串转换为列表
        if (attributes==None):
            attributes="0"
        # attributes_list = eval(attributes)  # 使用 eval 将字符串转换为列表
        key = str(name)+str(attributes)  # 以前两列作为键
        key = key.replace("[","(")
        key = key.replace("]",")")
        if key not in result:
            result[key] = []  # 初始化文件 ID 列表
        if (int(file_id)==0):
            continue
        result[key].append(int(file_id))  # 添加文件 ID
    final_data=[]
    for key,value in result.items():
        temp={}
        temp["name"]=key
        temp["file_id"]=value
        temp["id"]=key[key.find(content) + len(content):key.find('(')]
        final_data.append(json.dumps(temp, ensure_ascii=False))
    return final_data
def search_content_mysql(content,db):
    query = """
    SELECT DISTINCT
		xnf.xiaoqi_id,
		xnf.directory
   FROM xiaoqi_new xnf
	    JOIN file As f
        JOIN xiaoqi_to_file xtf ON f.id=xtf.file_id and xtf.xiaoqi_id=xnf.xiaoqi_id
   WHERE 
        xnf.xiaoqi_name LIKE %s
                """
    try:
        with db.connection.cursor() as cursor:
            search_string = str(content) + "%"
            cursor.execute(query, (search_string,))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询消歧实体失败：{e}")
        raise
def search_content_user_mysql(xiaoqi_id,userID,db):
    query = """
SELECT DISTINCT
		xnf.xiaoqi_name,
		xnf.directory,
		f.id,
		f.private
FROM xiaoqi_new xnf
JOIN file f ON f.private=0 or f.userid=%s
JOIN xiaoqi_to_file xtf ON f.id=xtf.file_id 
WHERE 
xtf.xiaoqi_id=%s and xnf.xiaoqi_id=%s
                    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID),int(xiaoqi_id),int(xiaoqi_id)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询消歧实体对应文件失败：{e}")
        raise
def check_user_private_dire(xiaoqi_id,userID,db):
    query = """
    SELECT DISTINCT
			de.id
    FROM dir_entity As de
    WHERE 
    de.entity_id=%s and de.userid=%s
                        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(xiaoqi_id),int(userID)))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询消歧实体对应文件失败：{e}")
        raise
def create_user_private_dire(entity_id,userID,db):
    try:
        with db.connection.cursor() as cursor:
            sql2 = "SELECT second FROM dir_to_entity WHERE entity_id = %s"
            print(entity_id)
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
                cursor.execute(insert_sql, (entity_id, third, third, userID))

        return ["success"]

    except pymysql.MySQLError as e:
        print(f"创建个人目录失败：{e}")
        raise
    except Exception as e:
        print(f"创建个人目录时发生未知错误：{e}")
        raise
def add_file_mysql(file_id,entity_id,userID,db):
    try:
        with db.connection.cursor() as cursor:
            sql1 = """
            SELECT DisTinct de.dir_private, de.dir_sys
FROM dir_file df
JOIN dir_entity de ON df.dir_id = de.id and de.entity_id=%s
WHERE df.file_id = %s;
            """
            cursor.execute(sql1, (int(entity_id),int(file_id),))
            dire_result = cursor.fetchall()
            if not dire_result:
                return "没有匹配目录"

            sql2 = "SELECT second FROM dir_to_entity WHERE entity_id = %s"
            cursor.execute(sql2, (entity_id,))
            second_result = cursor.fetchone()
            if not second_result:
                return "没有匹配的二级目录"

            # 查 directory 表获取 third
            sql3 = "SELECT third FROM directory WHERE second = %s and third=%s"
            for i in dire_result:
                cursor.execute(sql3, (second_result[0],i[1]))
                third_result = cursor.fetchone()
                if not third_result:#代表可能是个人目录
                    check_sql="""
                   SELECT id FROM dir_entity WHERE userid=%s and dir_private=%s and entity_id=%s
                    """
                    cursor.execute(check_sql, (int(userID), i[1], entity_id))
                    temp_result = cursor.fetchone()
                    if not temp_result:
                        insert_sql = """
                                INSERT INTO dir_entity (entity_id, dir_private, dir_sys, userid) 
                                VALUES (%s, %s, %s, %s)
                        """
                        cursor.execute(insert_sql, (entity_id, i[1], None, userID))

                    sql4 = "SELECT id FROM dir_entity WHERE userid=%s and dir_private=%s and entity_id=%s"
                    cursor.execute(sql4, (int(userID), i[1], entity_id))
                    user_dire_result = cursor.fetchone()
                    if not user_dire_result:
                        return "匹配不到用户已有目录"
                    insert_sql = """
                                            INSERT INTO dir_file (dir_id,file_id) 
                                            VALUES (%s, %s)
                                        """
                    cursor.execute(insert_sql, (user_dire_result[0], int(file_id)))
                else:#代表是存在的三级目录
                    sql4="SELECT id FROM dir_entity WHERE userid=%s and dir_sys=%s and entity_id=%s"
                    cursor.execute(sql4, (int(userID), third_result[0],entity_id))
                    user_dire_result = cursor.fetchone()
                    if not user_dire_result:
                        return "匹配不到用户已有目录"
                    insert_sql1 = """
                        INSERT INTO dir_file (dir_id,file_id) 
                        VALUES (%s, %s)
                    """
                    # print(str(user_dire_result[0])+" "+str(file_id))
                    cursor.execute(insert_sql1, (int(user_dire_result[0]),int(file_id)))
                    # print("执行插入")

        return "Success"

    except pymysql.MySQLError as e:
        print(f"添加个人文件信息失败：{e}")
        raise
    except Exception as e:
        print(f"添加个人文件信息时发生未知错误：{e}")
        raise
def main(request):
    content = request.GET["content"]
    userID = request.GET["userID"]
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()

    # 开始事务
    connection = db.connection
    try:
        # 开始事务
        connection.begin()

        result_content = search_content_mysql(content, db)  # 检测消歧实体是否存在 xiaoqi_id和目录
        xiaoqi_ids = []
        if (len(result_content) > 0):
            result = []
            for i in result_content:
                xiaoqi_ids.append(i[0])
            print(xiaoqi_ids)
            # 这个消歧实体是有过的
            for i in xiaoqi_ids:
                # print(str(i)+"xiaoqi——id")
                result_content_user = search_content_user_mysql(int(i), userID, db)  # 检查下面有没有该用户的共享文件 xiaoqi_name 目录,文件id，文件权限
                create_new = False  # 标记是否新建文件夹了
                if (len(check_user_private_dire(i, userID, db)) == 0):
                    create_user_private_dire(i, userID, db)  # 如果没有就为用户新建个人文件夹
                    create_new = True
                print("个人可用文件为 "+str(result_content_user))
                if (len(result_content_user) > 0):  # 存在共享文件
                    # print(result_content_user)
                    if (create_new == True):
                        file_ids = []
                        for i1 in result_content_user:
                            if int(i1[3]) == 0:
                                file_ids.append(int(i1[2]))
                                temp = add_file_mysql(int(i1[2]), int(i), int(userID), db)
                                if (temp != "Success"):
                                    print(str(temp) + " " + str(i1[2]))
                        print(file_ids)
                        result1 = []
                        for i1 in file_ids:
                            result1.append((result_content_user[0][0], result_content_user[0][1], i1))
                        print(result1)
                        for i1 in result1:
                            result.append(i1)
                    else:
                        result1 = search_content_from_mysql_new(i, userID, db)
                        print(i)
                        print(result1)
                        if len(result1) > 0:
                            for i1 in result1:
                                result.append(i1)
                        else:
                            print(str(i)+"没有文件，但尝试显示")
                            result2 = get_content_info(i, db)
                            for i1 in result2:
                                result.append((i1[0], i1[1], 0)) #0是一个不存在的文件
                            # return chuli_result(result1,content)
                            # print(chuli_result(result1, content))
                else:
                    print(i)
                    result2=get_content_info(i,db)
                    for i1 in result2:
                        result.append((i1[0],i1[1],0))#0是一个不存在的文件

            # 所有操作成功，提交事务
            connection.commit()
            result = chuli_result(result, content)
            return result
            # print(result)
        else:
            # print("不存在")
            connection.commit()
            return "不存在"

    except Exception as e:
        # 发生错误，回滚事务
        connection.rollback()
        print(f"主函数执行失败，事务已回滚：{e}")
        raise
    finally:
        # 确保连接关闭
        db.close()

# def search_content_from_mysql(content,userID,db):
#     """
#     根据给定的 content 查询 entity_to_file 表中的 file_id，
#     并返回 file 表中所有匹配的行的信息。
#     """
#     query = """
#     SELECT data.*
#     FROM xiaoqi_data AS data
#     WHERE data.entity = %s and data.userid=%s;
#     """
#     try:
#         with db.connection.cursor() as cursor:
#             cursor.execute(query, (content,int(userID),))
#             result = cursor.fetchall()
#             return result
#     except pymysql.MySQLError as e:
#         print(f"查询失败：{e}")
#         raise
# def search_content_from_mysql_new(content,userID,db):
#     """
#     根据给定的 content 查询 entity_to_file 表中的 file_id，
#     并返回 file 表中所有匹配的行的信息。
#     """
# #     query = """
# #     SELECT
# #     xnf.xiaoqi_name,
# #     xnf.key_words,
# #     etf.file_id
# # FROM
# #     entity_to_file etf
# # JOIN
# #     file f ON (etf.file_id = f.id and (f.private=0 or f.userid=%s))
# # JOIN
# #     xiaoqi_to_file xtf ON etf.file_id = xtf.file_id
# # JOIN
# #     xiaoqi_new xnf ON xtf.xiaoqi_id = xnf.xiaoqi_id
# # WHERE
# #     etf.entity = %s and xnf.xiaoqi_name like %s;
# #     """
#     query = """
#             SELECT
#             xnf.xiaoqi_name,
#             xnf.directory,
#             df.file_id
# 			FROM
# 			dir_entity de
#         JOIN
#             dir_file df ON df.dir_id=de.id
#         JOIN
#             file f ON (df.file_id = f.id AND (f.private = 0 OR f.userid = %s))
#         JOIN
#             xiaoqi_new xnf ON de.entity_id = xnf.xiaoqi_id
# 		JOIN
# 			xiaoqi_to_file xtf ON df.file_id=xtf.file_id and xtf.xiaoqi_id=xnf.xiaoqi_id
#         WHERE
#             xnf.xiaoqi_name LIKE %s and de.userid=%s;
#             """
#     try:
#         with db.connection.cursor() as cursor:
#             search_string=str(content)+"%"
#             cursor.execute(query, (int(userID),search_string,int(userID)))
#             result = cursor.fetchall()
#             return result
#     except pymysql.MySQLError as e:
#         print(f"查询消歧实体失败：{e}")
#         raise
# # def search_public_content_from_mysql(content,userID,db):
# #     """
# #     根据给定的 content 查询 entity_to_file 表中的 file_id，
# #     并返回 file 表中所有匹配的行的信息。
# #     """
# #     query = """
# #     SELECT data.*
# #     FROM xiaoqi_data AS data
# #     WHERE data.entity = %s and data.userid=0;
# #     """
# #     try:
# #         with db.connection.cursor() as cursor:
# #             cursor.execute(query, (content))
# #             result = cursor.fetchall()
# #             return result
# #     except pymysql.MySQLError as e:
# #         print(f"查询失败：{e}")
# #         raise
# def chuli_result(data,content):
#     result = {}
#     print(data)
#     for name, attributes, file_id in data:
#         # 将字符串转换为列表
#         if (attributes==None):
#             attributes="0"
#         # attributes_list = eval(attributes)  # 使用 eval 将字符串转换为列表
#         key = str(name)+str(attributes)  # 以前两列作为键
#         key = key.replace("[","(")
#         key = key.replace("]",")")
#         if key not in result:
#             result[key] = []  # 初始化文件 ID 列表
#
#         result[key].append(int(file_id))  # 添加文件 ID
#     final_data=[]
#     for key,value in result.items():
#         temp={}
#         temp["name"]=key
#         temp["file_id"]=value
#         temp["id"]=key[key.find(content) + len(content):key.find('(')]
#         final_data.append(json.dumps(temp, ensure_ascii=False))
#     return final_data
# # def check_exist(content,db):
# #
# def get_dir_private_list(db, entity_id, userid):
#     """
#     查询dir_entity，如果没结果则查询dir_to_entity -> directory，并写入新内容后返回third列表
#     """
#     with db.connection.cursor() as cursor:
#         # 先查 dir_entity
#         sql1 = """
#             SELECT dir_private
#             FROM dir_entity
#             WHERE entity_id = %s AND userid = %s
#         """
#         cursor.execute(sql1, (entity_id, userid))
#         result = cursor.fetchall()
#         if result:
#             return [row[0] for row in result]
#
#         # 查不到 -> 查 dir_to_entity 获取 second
#         sql2 = "SELECT second FROM dir_to_entity WHERE entity_id = %s"
#         cursor.execute(sql2, (entity_id,))
#         second_result = cursor.fetchone()
#         if not second_result:
#             return []  # 如果连 second 都查不到，直接返回空列表
#
#         second = second_result[0]
#
#         # 查 directory 表获取 third
#         sql3 = "SELECT third FROM directory WHERE second = %s"
#         cursor.execute(sql3, (second,))
#         third_results = cursor.fetchall()
#         third_list = [row[0] for row in third_results]
#
#         # 插入到 dir_entity 表
#         insert_sql = """
#             INSERT INTO dir_entity (entity_id, dir_private, dir_sys, userid)
#             VALUES (%s, %s, %s, %s)
#         """
#         for third in third_list:
#             cursor.execute(insert_sql, (entity_id, third, third, userid))
#
#         db.connection.commit()
#
#         return third_list
# def main(request):
#     content = request.GET["content"]
#     userID = request.GET["userID"]
#     db = MySQLDatabase(
#         host="114.213.234.179",
#         user="koroot",  # 替换为您的用户名
#         password="DMiC-4092",  # 替换为您的密码
#         database="db_hp"  # 替换为您的数据库名
#     )
#     db.connect()
#     # result=search_content_from_mysql(content,userID,db)
#     result=search_content_from_mysql_new(content,userID,db)
#     # if len(result)>0:
#     #     chuli_user_private_dire(content,userID,db)
#     print(result)
#     if len(result)>0:
#         return chuli_result(result,content)
#     else:
#         check_exist(content,db)#确认一下是这个用户没有还是干脆就没有这个实体
#         return "不存在"
