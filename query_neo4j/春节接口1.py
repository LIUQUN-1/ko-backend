import pymysql

class DatabaseHandler:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish a database connection."""
        self.connection = pymysql.connect(**self.db_config)

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()

    def fetch_entities_by_keyword(self, keyword,userID):
        """Fetch entities associated with a specific keyword."""
        try:
            with self.connection.cursor() as cursor:
                # 查询文件名和文件ID
                sql_query = """
                    SELECT
                        t1.name,
                        t2.file_id
                    FROM
                        `file` AS t1
                    INNER JOIN
                        `entity_to_file` AS t2 ON t1.id = t2.file_id
                    WHERE
                        t2.entity = %s 
                        AND (t1.private = 0 OR t1.userid = %s);
                """
                # 使用参数化查询，避免 SQL 注入
                cursor.execute(sql_query, (keyword, int(userID)))
                initial_result = cursor.fetchall()

                docdict = {}
                docdict_plus={}
                file_dict = {}
                file_dict_rev = {}
                # 遍历初始结果集
                for row in initial_result:
                    file_name, file_id = row
                    # 查询与文件ID关联的实体
                    sql_query = """
                    SELECT
                        t1.id,
                        t1.name,
                        t2.entity,
                        t2.sim
                    FROM
                        `file` AS t1
                    INNER JOIN
                        `entity_to_file` AS t2 ON t1.id = t2.file_id
                    WHERE
                        t2.file_id = %s
                        AND (t1.private = 0 OR t1.userid = %s);
                    """
                    cursor.execute(sql_query, (file_id, int(userID)))
                    entities = cursor.fetchall()

                    # 如果文件名不在字典中，初始化一个空列表
                    if file_name not in docdict:
                        docdict[file_id] = []
                        docdict_plus[file_id]=[]

                    # 将实体添加到文件名对应的列表中
                    for entity_row in entities:
                        # print(entity_row)
                        entity = entity_row[2]
                        file_dict[file_id] = entity_row[1]
                        docdict[file_id].append(entity)
                        docdict_plus[file_id].append([entity,entity_row[3]])
                        file_dict_rev[entity_row[1]] = file_id
                return file_dict, docdict,docdict_plus, file_dict_rev
        except pymysql.MySQLError as e:
            print(f"Error: {e}")
            return None

# # 使用示例
# db_config = {
#     'host': '114.213.234.179',
#     'user': 'koroot',
#     'password': 'DMiC-4092',
#     'database': 'db_hp',
# }
#
# db_handler = DatabaseHandler(db_config)
# db_handler.connect()
# docdict = db_handler.fetch_entities_by_keyword('水稻',6000622)
# print(docdict)
# db_handler.close()
