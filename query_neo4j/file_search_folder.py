import json
import pymysql
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
                cursor.execute(check_query, (primary_key_value, last_key_value,))
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

def find_index(node, target_index, target_name=None):
    for i in node:
        if (target_name != None and target_name == i["name"]):  # 三级目录一样的合并一下
            return i
        if (target_index == i["id"]):
            return i

    return None
def test2(content, full_name, userID):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )
    db.connect()
    query = """
SELECT 
		xn.xiaoqi_id,
    de.id AS directory_id,
    de.dir_private,
    x.file_id AS xiaoqi_file_id,
		f.name AS file_name,
		f.private As file_private
FROM 
    xiaoqi_new xn
JOIN 
    xiaoqi_to_file x ON xn.xiaoqi_id = x.xiaoqi_id
JOIN 
    dir_file df ON x.file_id = df.file_id
JOIN 
    dir_entity de ON df.dir_id = de.id and de.entity_id=xn.xiaoqi_id and de.userid=%s
JOIN 
    file f ON x.file_id = f.id and (f.private=0 or f.userid=%s)
WHERE 
    xn.xiaoqi_name = %s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID), int(userID), str(content)))
            result = cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    node = []
    dire_list = []
    global entity_id
    entity_id = 1234
    for i in result:
        # 在确保一个文件确实存储到mysql且不会有多个目录的前提下
        temp_file = {}
        temp_file["name"] = i[4]
        temp_file["type"] = "file"
        temp_file["id"] = "file" + str(i[3])
        temp_file["private"]=i[5]
        node.append(temp_file)
        k = find_index(node, "folder" + str(i[1]), str(i[2]))  # 三级目录一样的合并一下
        if k == None:
            temp_dire = {}
            temp_dire["name"] = i[2]
            temp_dire["type"] = "folder"
            temp_dire["id"] = "folder" + str(i[1])
            temp_dire["children"] = [temp_file]
            dire_list.append(temp_dire["id"])
            node.append(temp_dire)
        else:
            k["children"].append(temp_file)
        entity_id = int(i[0])

    #保证新建文件夹能刷新出来的。
    query = """
    SELECT
    de.dir_private,
	de.id
    FROM
        xiaoqi_new xn
    JOIN
		dir_entity de on de.entity_id=xn.xiaoqi_id and de.userid=%s
    WHERE
        xn.xiaoqi_name = %s;
        """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (int(userID), str(content)))
            result = cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    xiugai_list = []  # 0是原来，1是新的
    for i in result:
        xiugai_list.append((i[0], i[1]))
    for j in xiugai_list:
        flag=False
        for i in node:
            if i["name"] == j[0]:
                flag=True
                break
        if flag==False:
            temp_dire = {}
            temp_dire["name"] = j[0]
            temp_dire["type"] = "folder"
            temp_dire["id"] = "folder" + str(j[1])
            temp_dire["children"] = []
            dire_list.append(temp_dire["id"])
            node.append(temp_dire)
    query = """
        SELECT
        xn.directory
        FROM
            xiaoqi_new xn
        WHERE
            xn.xiaoqi_name = %s;
            """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (str(content)))
            result1 = cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    for i in result1:
        if i[0]!=None:
            full_name=content+i[0]
    temp_entity = {}
    temp_entity["name"] = full_name
    temp_entity["type"] = "entity"
    temp_entity["id"] = "entity" + str(entity_id)
    temp_entity["children"] = []
    for i in dire_list:
        k = find_index(node, i)
        if k != None:
            temp_entity["children"].append(k)
    query = """
            SELECT COUNT(f.id)
FROM xiaoqi_new xn
JOIN xiaoqi_to_file xtf ON xtf.xiaoqi_id = xn.xiaoqi_id
JOIN `file` f ON f.id = xtf.file_id and f.private=1
WHERE xn.xiaoqi_name = %s;
                """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (str(content)))
            result1 = cursor.fetchall()
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
    return json.dumps(temp_entity, ensure_ascii=False),result1[0][0]
def main(request):
    content = request.GET["content"]
    full_name = request.GET["full_name"]
    userID = request.GET["userID"]
    result,private_num = test2(content, full_name, userID)
    if (len(result) < 1):
        return json.dumps({"result": "文件为空", "private_num": private_num}, ensure_ascii=False)
    else:
        return json.dumps({"result": result, "private_num": private_num}, ensure_ascii=False)
