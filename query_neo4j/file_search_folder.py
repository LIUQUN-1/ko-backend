import json
import pymysql
import json
import logging
from django.http import JsonResponse # 假设这是在Django环境，但我们只用作参考，实际未调用

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')

# -----------------------------------------------------------
# 假设 MySQLDatabase 类已在文件头部定义
# -----------------------------------------------------------
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
            # print("数据库连接成功！") # 生产环境建议注释
        except pymysql.MySQLError as e:
            # print(f"数据库连接失败：{e}") # 生产环境建议注释
            raise

    # ... (其他方法如 insert_data 等保持不变，但在此省略以聚焦核心逻辑)

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            # print("数据库连接已关闭！") # 生产环境建议注释

# -----------------------------------------------------------
# 辅助函数 1：获取实体下所有目录数据
# -----------------------------------------------------------
def get_all_entity_dirs(connection, xiaoqi_name, userID):
    """
    获取指定实体下的所有 dir_entity 和 dir_entity_more 目录数据，并扁平化存储。
    返回:
      - all_nodes: {node_id: node_dict} 扁平目录节点字典
      - entity_id: 实体ID
    """

    # 1. 查找 xiaoqi_id
    query_xiaoqi_id = """
    SELECT xiaoqi_id FROM xiaoqi_new WHERE xiaoqi_name = %s LIMIT 1
    """
    entity_id = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(query_xiaoqi_id, (str(xiaoqi_name)))
            result = cursor.fetchone()
            if result:
                entity_id = int(result[0])
            else:
                return {}, None
    except pymysql.MySQLError as e:
        logger.error(f"查询 xiaoqi_id 失败: {e}")
        raise

    # 2. 查询所有一级目录 (来自 dir_entity)
    query_top_dirs = """
    SELECT
        de.id, de.dir_private
    FROM
        dir_entity de
    WHERE
        de.entity_id = %s AND de.userid = %s;
    """

    # 3. 查询所有子目录 (来自 dir_entity_more)
    query_sub_dirs = """
    SELECT
        dem.id, dem.dir, dem.dir_entity_id, dem.parent_id
    FROM
        dir_entity_more dem
    JOIN
        dir_entity de ON dem.dir_entity_id = de.id
    WHERE
        de.entity_id = %s AND de.userid = %s;
    """

    # 用于存储所有节点的字典：{id: node_dict}
    all_nodes = {}

    try:
        with connection.cursor() as cursor:
            # 执行一级目录查询
            cursor.execute(query_top_dirs, (entity_id, int(userID)))
            top_dirs = cursor.fetchall()

            # 初始化一级目录节点
            for dir_entity_id, dir_name in top_dirs:
                node_id = "folder_e" + str(dir_entity_id) # 标记为 dir_entity 节点
                all_nodes[node_id] = {
                    "id": node_id,
                    "name": dir_name,
                    "type": "folder",
                    "children": [],
                    "dir_entity_id": dir_entity_id,
                    "parent_id": None # 一级目录的父节点在树结构中是实体节点
                }

            # 执行子目录查询
            cursor.execute(query_sub_dirs, (entity_id, int(userID)))
            sub_dirs = cursor.fetchall()

            # 初始化子目录节点
            for dem_id, dem_name, dir_entity_id, parent_id in sub_dirs:
                node_id = "folder_m" + str(dem_id) # 标记为 dir_entity_more 节点

                # 父节点ID的格式化：如果是 NULL/0，则父节点是一级目录 (dir_entity)
                if parent_id is None or parent_id == 0:
                    parent_node_id = "folder_e" + str(dir_entity_id) # 关联到它所属的一级目录
                else:
                    parent_node_id = "folder_m" + str(parent_id) # 关联到另一个 dir_entity_more 节点

                all_nodes[node_id] = {
                    "id": node_id,
                    "name": dem_name,
                    "type": "folder",
                    "children": [],
                    "dir_entity_id": dir_entity_id,
                    "parent_id": parent_node_id
                }

            return all_nodes, entity_id

    except pymysql.MySQLError as e:
        logger.error(f"查询目录数据失败：{e}")
        raise

# -----------------------------------------------------------
# 辅助函数 2：将扁平目录列表转换为嵌套树结构
# -----------------------------------------------------------
def build_tree(all_nodes, root_marker="folder_e"):
    """
    根据 parent_id 将扁平节点列表构建为树形结构。
    """
    tree_roots = []

    # 复制字典，防止在迭代时修改
    temp_nodes = all_nodes.copy()

    # 遍历所有节点
    for node_id, node in list(temp_nodes.items()):
        parent_id = node.get("parent_id")

        if parent_id in all_nodes:
            # 找到父节点并添加到其 children 列表中
            # 确保父节点的 children 存在
            if "children" not in all_nodes[parent_id]:
                all_nodes[parent_id]["children"] = []

            # 检查是否重复添加 (理论上不会，但保险起见)
            if node not in all_nodes[parent_id]["children"]:
                all_nodes[parent_id]["children"].append(node)

        elif parent_id is None and node_id.startswith(root_marker):
            # 如果 parent_id 为 None 且是一级目录，则它是一个树的根
            tree_roots.append(node)

    # 返回所有一级目录作为根的树
    return tree_roots

# -----------------------------------------------------------
# 【重写】 test2 主函数
# -----------------------------------------------------------
def test2(content, full_name, userID):
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",
        password="DMiC-4092",
        database="db_hp"
    )
    db.connect()

    try:
        # 1. 获取所有目录节点（扁平化）和实体ID (逻辑保持不变)
        all_nodes, entity_id = get_all_entity_dirs(db.connection, content, userID)

        if entity_id is None:
            return json.dumps({"result": json.dumps({"name": full_name, "type": "entity", "id": "entity1234", "children": []}, ensure_ascii=False), "private_num": 0}, ensure_ascii=False)

        # 2. 【核心修改】查询文件数据 - 联合查询 dir_more_file 和 dir_file
        # 目标：获取 file_id, file_name, file_private, 以及其父目录的节点ID。

        # 联合查询的字段：
        #   - parent_id: 文件的父目录ID (可能是 dir_entity.id 或 dir_entity_more.id)
        #   - is_top_level: 1 表示是顶级目录 (来自 dir_file)，0 表示是多级目录 (来自 dir_more_file)

        query_files = """
        SELECT
            file_data.parent_id,
            file_data.is_top_level,
            f.id AS file_id,
            f.name AS file_name,
            f.private AS file_private
        FROM
            xiaoqi_new xn
        JOIN
            xiaoqi_to_file xtf ON xn.xiaoqi_id = xtf.xiaoqi_id
        JOIN
            file f ON xtf.file_id = f.id AND (f.private = 0 OR f.userid = %s)
        JOIN (
            -- Subquery A: Files mapped to multi-level directories (dir_entity_more)
            SELECT
                demf.dir_more_id AS parent_id,
                demf.file_id,
                0 AS is_top_level  -- 0 for dir_entity_more
            FROM
                dir_more_file demf
            UNION ALL
            -- Subquery B: Files mapped to top-level directories (dir_entity)
            SELECT
                df.dir_id AS parent_id,
                df.file_id,
                1 AS is_top_level  -- 1 for dir_entity
            FROM
                dir_file df
        ) AS file_data ON f.id = file_data.file_id
        WHERE
            xn.xiaoqi_name = %s;
        """

        private_file_count = 0
        with db.connection.cursor() as cursor:
            # 注意: file_id 在 dir_file 中是 varchar，在 file 表中是 int。
            # 这里的查询假设它们可以被比较或隐式转换。
            cursor.execute(query_files, (int(userID), str(content)))
            file_results = cursor.fetchall()

        # 3. 【核心修改】将文件节点挂载到树中的正确位置
        for parent_id, is_top_level, file_id, file_name, file_private in file_results:

            # 确定父目录节点 ID 的格式
            if is_top_level == 1:
                # 顶级目录: dir_entity.id -> "folder_e" + id
                parent_node_id = "folder_e" + str(parent_id)
            else:
                # 多级子目录: dir_entity_more.id -> "folder_m" + id
                parent_node_id = "folder_m" + str(parent_id)

            temp_file = {
                "name": file_name,
                "type": "file",
                "id": "file" + str(file_id),
                "private": file_private
            }

            # 查找对应的目录节点并挂载
            if parent_node_id in all_nodes:
                # 确保 children 列表存在（虽然在 get_all_entity_dirs 中已初始化，但这更健壮）
                if "children" not in all_nodes[parent_node_id]:
                    all_nodes[parent_node_id]["children"] = []

                all_nodes[parent_node_id]["children"].append(temp_file)

            if file_private == 1:
                private_file_count += 1

        # 4. 将扁平目录列表转换为嵌套树结构 (逻辑保持不变)
        final_children = build_tree(all_nodes, root_marker="folder_e")

        # 5. 组装最终返回结构 (逻辑保持不变)

        # 获取 full_name（实体名称+消歧后缀）
        query_directory = """
        SELECT xn.directory
        FROM xiaoqi_new xn
        WHERE xn.xiaoqi_name = %s;
        """

        full_name_suffix = ""
        with db.connection.cursor() as cursor:
            cursor.execute(query_directory, (str(content)))
            result1 = cursor.fetchone()
            if result1 and result1[0] is not None:
                full_name_suffix = result1[0]

        final_full_name = content + full_name_suffix

        temp_entity = {
            "name": final_full_name,
            "type": "entity",
            "id": "entity" + str(entity_id),
            "children": final_children
        }

        # 封装结果以匹配前端原有的 JSON 格式要求
        return json.dumps({
            "result": json.dumps(temp_entity, ensure_ascii=False),
            "private_num": private_file_count
        }, ensure_ascii=False)

    except Exception as e:
        logger.error(f"test2 函数执行失败：{e}")
        # 返回一个带有错误信息的结构
        return json.dumps({
            "result": json.dumps({"name": full_name, "type": "entity", "id": "error", "children": []}, ensure_ascii=False),
            "private_num": 0
        }, ensure_ascii=False)
    finally:
        db.close()

# -----------------------------------------------------------
# 【保留原有入口函数】
# -----------------------------------------------------------
def find_index(node, target_index, target_name=None):
    for i in node:
        if (target_name != None and target_name == i["name"]):  # 三级目录一样的合并一下
            return i
        if (target_index == i["id"]):
            return i

    return None

def main(request):
    content = request.GET["content"]
    full_name = request.GET["full_name"]
    userID = request.GET["userID"]

    # 这里的调用逻辑保持不变
    result = test2(content, full_name, userID)

    if not result:
        return "文件为空"
    else:
        # test2 现在返回的是包含 'result' 和 'private_num' 的 JSON 字符串
        return result