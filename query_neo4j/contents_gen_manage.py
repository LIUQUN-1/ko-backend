import json
import logging
import pymysql
from django.http import JsonResponse
from query_neo4j.disambiguation import MySQLDatabase

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')


class DirectoryDatabase(MySQLDatabase):
    """
    继承 MySQLDatabase 类，添加目录管理相关方法
    """

    def create_directory(self, dir_name, dir_entity_id, parent_id=None, sort_order=None):
        """
        创建新目录到 dir_entity_more 表中，sort_order 代表目录深度。

        参数:
            dir_name: 目录名称
            dir_entity_id: 关联的 dir_entity.id (顶级目录的ID)
            parent_id: 父目录 id (来自 dir_entity_more.id)。None/0 表示父目录是 dir_entity。
            sort_order: 目录深度。必须由前端计算并传入：
                        - 如果父级是 dir_entity，则 sort_order=0
                        - 如果父级是 dir_entity_more，则 sort_order=父级深度+1
        返回:
            dict: {"success": bool, "message": str, "dir_id": int}
        """
        try:
            with self.connection.cursor() as cursor:
                # 1. 检查 dir_entity_id 是否存在 (保持不变)
                check_entity_query = "SELECT COUNT(*) FROM dir_entity WHERE id = %s"
                cursor.execute(check_entity_query, (dir_entity_id,))
                if cursor.fetchone()[0] == 0:
                    return {
                        "success": False,
                        "message": f"dir_entity_id={dir_entity_id} 不存在"
                    }

                # 2. 检查 sort_order (深度) 是否传入
                if sort_order is None or not isinstance(sort_order, int) or sort_order < 0:
                    return {
                        "success": False,
                        "message": "缺少或 sort_order 参数无效（必须由前端传入计算后的目录深度）"
                    }

                # 3. 检查并规范化 parent_id
                final_parent_id = None
                if parent_id is not None:
                    # 尝试将 parent_id 转换为整数，以防前端传入字符串
                    try:
                        temp_parent_id = int(parent_id)
                    except (ValueError, TypeError):
                        temp_parent_id = None

                    # 如果 parent_id 是一个有效的 dir_entity_more ID (非None, 非0)
                    if temp_parent_id is not None and temp_parent_id != 0:
                        # 检查父目录是否存在于 dir_entity_more
                        check_parent_query = "SELECT COUNT(*) FROM dir_entity_more WHERE id = %s"
                        cursor.execute(check_parent_query, (temp_parent_id,))
                        if cursor.fetchone()[0] == 0:
                            return {
                                "success": False,
                                "message": f"父目录 parent_id={temp_parent_id} 不存在于 dir_entity_more 表中"
                            }
                        final_parent_id = temp_parent_id
                    # 否则，如果 parent_id 是 None/0，则 final_parent_id 保持为 None，表示是 dir_entity 的子目录

                # 4. 插入新目录到 dir_entity_more
                insert_query = """
                INSERT INTO dir_entity_more (dir, dir_entity_id, parent_id, sort_order)
                VALUES (%s, %s, %s, %s)
                """

                # 使用传入的 sort_order (深度)
                cursor.execute(insert_query, (dir_name, dir_entity_id, final_parent_id, sort_order))

                new_dir_id = cursor.lastrowid

                return {
                    "success": True,
                    "message": "目录创建成功",
                    "dir_id": new_dir_id
                }

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"创建目录失败：{e}")
            return {
                "success": False,
                "message": f"数据库错误: {str(e)}"
            }

    def delete_directory(self, dir_type, dir_id, user_id):
        """
        删除目录（顶级或子目录），只能删除空目录。

        参数:
            dir_type (str): 目录类型 ('entity' for dir_entity, 'more' for dir_entity_more)
            dir_id (int): 要删除的目录id
            user_id (str): 当前用户ID (用于安全验证顶级目录)
        返回:
            dict: {"success": bool, "message": str}
        """
        try:
            with self.connection.cursor() as cursor:
                if dir_type == 'dir_entity_more':
                    # --- 删除子目录 (dir_entity_more) ---
                    # 1. 检查目录是否存在
                    check_query = "SELECT dir FROM dir_entity_more WHERE id = %s"
                    cursor.execute(check_query, (dir_id,))
                    result = cursor.fetchone()
                    if not result:
                        return {"success": False, "message": f"子目录 id={dir_id} 不存在"}
                    dir_name = result[0]

                    # 2. 检查是否有子目录
                    check_children_query = "SELECT COUNT(*) FROM dir_entity_more WHERE parent_id = %s"
                    cursor.execute(check_children_query, (dir_id,))
                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"目录 '{dir_name}' 包含子目录，无法删除"}

                    # 3. 检查目录是否包含文件 (dir_more_file)
                    check_files_query = "SELECT COUNT(*) FROM dir_more_file WHERE dir_more_id = %s"
                    cursor.execute(check_files_query, (dir_id,))
                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"目录 '{dir_name}' 不为空（包含文件），无法删除"}

                    # 4. 执行删除
                    delete_query = "DELETE FROM dir_entity_more WHERE id = %s"
                    cursor.execute(delete_query, (dir_id,))

                elif dir_type == 'dir_entity':
                    # --- 删除顶级目录 (dir_entity) ---
                    # 1. 检查目录是否存在且属于该用户
                    check_query = "SELECT dir_private FROM dir_entity WHERE id = %s AND userid = %s"
                    cursor.execute(check_query, (dir_id, user_id))
                    result = cursor.fetchone()
                    if not result:
                        return {"success": False, "message": f"顶级目录 id={dir_id} 不存在或不属于当前用户"}
                    dir_name = result[0]

                    # 2. 检查是否有子目录 (在 dir_entity_more 中 parent_id 为 NULL)
                    check_children_query = "SELECT COUNT(*) FROM dir_entity_more WHERE dir_entity_id = %s"
                    cursor.execute(check_children_query, (dir_id,))
                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"目录 '{dir_name}' 包含子目录，无法删除"}

                    # 3. 检查目录是否包含文件 (dir_file)
                    check_files_query = "SELECT COUNT(*) FROM dir_file WHERE dir_id = %s"
                    cursor.execute(check_files_query, (dir_id,))
                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"目录 '{dir_name}' 不为空（包含文件），无法删除"}

                    # 4. 执行删除
                    delete_query = "DELETE FROM dir_entity WHERE id = %s"
                    cursor.execute(delete_query, (dir_id,))

                else:
                    return {"success": False, "message": "无效的目录类型"}

                self.connection.commit()
                return {"success": True, "message": f"目录 '{dir_name}' 删除成功"}

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"删除目录失败：{e}")
            return {"success": False, "message": f"数据库错误: {str(e)}"}

    def rename_directory(self, dir_type, dir_id, new_dir_name, dir_entity_id=None, user_id=None):
        """
        重命名目录（顶级或子目录）

        参数:
            dir_type (str): 目录类型 ('entity' for dir_entity, 'more' for dir_entity_more)
            dir_id (int): 要重命名的目录id
            new_dir_name (str): 新的目录名称
            dir_entity_id (int, optional): 关联的dir_entity.id，用于重命名子目录
            user_id (str, optional): 当前用户ID，用于重命名顶级目录
        返回:
            dict: {"success": bool, "message": str}
        """
        try:
            with self.connection.cursor() as cursor:
                if dir_type == 'dir_entity_more':
                    # --- 重命名子目录 (dir_entity_more) ---
                    check_query = "SELECT dir, parent_id FROM dir_entity_more WHERE id = %s AND dir_entity_id = %s"
                    cursor.execute(check_query, (dir_id, dir_entity_id))
                    result = cursor.fetchone()
                    if not result:
                        return {"success": False, "message": f"子目录 id={dir_id} 不存在或不属于当前实体"}
                    old_dir_name, parent_id = result
                    if old_dir_name == new_dir_name:
                        return {"success": True, "message": "新名称与旧名称相同"}

                    if parent_id is None:
                        check_duplicate_query = "SELECT COUNT(*) FROM dir_entity_more WHERE dir = %s AND dir_entity_id = %s AND parent_id IS NULL AND id != %s"
                        cursor.execute(check_duplicate_query, (new_dir_name, dir_entity_id, dir_id))
                    else:
                        check_duplicate_query = "SELECT COUNT(*) FROM dir_entity_more WHERE dir = %s AND dir_entity_id = %s AND parent_id = %s AND id != %s"
                        cursor.execute(check_duplicate_query, (new_dir_name, dir_entity_id, parent_id, dir_id))

                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"该位置已存在名为 '{new_dir_name}' 的目录"}

                    update_query = "UPDATE dir_entity_more SET dir = %s WHERE id = %s"
                    cursor.execute(update_query, (new_dir_name, dir_id))

                elif dir_type == 'dir_entity':
                    # --- 重命名顶级目录 (dir_entity) ---
                    check_query = "SELECT dir_private, entity_id FROM dir_entity WHERE id = %s AND userid = %s"
                    cursor.execute(check_query, (dir_id, user_id))
                    result = cursor.fetchone()
                    if not result:
                        return {"success": False, "message": f"顶级目录 id={dir_id} 不存在或不属于当前用户"}
                    old_dir_name, entity_id = result
                    if old_dir_name == new_dir_name:
                        return {"success": True, "message": "新名称与旧名称相同"}

                    # 检查同一用户、同一实体下是否有同名顶级目录
                    check_duplicate_query = "SELECT COUNT(*) FROM dir_entity WHERE dir_private = %s AND entity_id = %s AND userid = %s AND id != %s"
                    cursor.execute(check_duplicate_query, (new_dir_name, entity_id, user_id, dir_id))
                    if cursor.fetchone()[0] > 0:
                        return {"success": False, "message": f"已存在名为 '{new_dir_name}' 的顶级目录"}

                    update_query = "UPDATE dir_entity SET dir_private = %s WHERE id = %s"
                    cursor.execute(update_query, (new_dir_name, dir_id))

                else:
                    return {"success": False, "message": "无效的目录类型"}

                self.connection.commit()
                return {"success": True, "message": f"目录重命名成功"}

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"重命名目录失败：{e}")
            return {"success": False, "message": f"数据库错误: {str(e)}"}

    def get_directory_tree(self, dir_entity_id, parent_id=None):
        """
        获取目录树结构

        参数:
            dir_entity_id: 关联的dir_entity表的id
            parent_id: 父目录id，None表示获取根目录
        返回:
            list: 目录列表
        """
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT id, dir, parent_id, sort_order 
                    FROM dir_entity_more 
                    WHERE dir_entity_id = %s AND (parent_id = %s OR (parent_id IS NULL AND %s IS NULL))
                    ORDER BY sort_order, id
                """
                cursor.execute(query, (dir_entity_id, parent_id, parent_id))
                results = cursor.fetchall()

                return results

        except pymysql.MySQLError as e:
            logger.error(f"查询目录树失败：{e}")
            return []


def directory_management(request):
    """
    处理目录管理请求（创建、删除、重命名）
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            db = None # 初始化db
        except json.JSONDecodeError:
            return JsonResponse({"success": False, "message": "无效的JSON格式"}, status=400)

        operation = data.get('operation')

        try:
            db = DirectoryDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            db.connect()

            if operation == 'create':
                dir_entity_id = data.get('dir_entity_id')
                dir_name = data.get('dir_name')
                parent_id = data.get('parent_id')
                sort_order = data.get('sort_order')
                if not all([dir_entity_id, dir_name]):
                    return JsonResponse({"success": False, "message": "创建操作缺少参数：dir_entity_id, dir_name"}, status=400)
                result = db.create_directory(dir_name, dir_entity_id, parent_id, sort_order)

            elif operation == 'delete':
                dir_id = data.get('dir_id')
                dir_type = data.get('dir_type')
                user_id = data.get('user_id') # user_id 对于 'entity' 类型是必需的
                if not all([dir_id, dir_type]):
                    return JsonResponse({"success": False, "message": "删除操作缺少参数：dir_id, dir_type"}, status=400)
                if dir_type == 'entity' and not user_id:
                    return JsonResponse({"success": False, "message": "删除顶级目录需要 user_id"}, status=400)
                result = db.delete_directory(dir_type, dir_id, user_id)

            elif operation == 'rename':
                dir_id = data.get('dir_id')
                dir_type = data.get('dir_type')
                new_dir_name = data.get('new_dir_name')
                if not all([dir_id, dir_type, new_dir_name]):
                    return JsonResponse({"success": False, "message": "重命名操作缺少参数：dir_id, dir_type, new_dir_name"}, status=400)

                # 根据类型获取额外所需参数
                dir_entity_id = data.get('dir_entity_id') # 'more' 类型需要
                user_id = data.get('user_id') # 'entity' 类型需要
                if dir_type == 'more' and not dir_entity_id:
                    return JsonResponse({"success": False, "message": "重命名子目录需要 dir_entity_id"}, status=400)
                if dir_type == 'entity' and not user_id:
                    return JsonResponse({"success": False, "message": "重命名顶级目录需要 user_id"}, status=400)

                result = db.rename_directory(dir_type, dir_id, new_dir_name, dir_entity_id, user_id)

            else:
                return JsonResponse({"success": False, "message": f"不支持的操作类型：{operation}"}, status=400)

            return JsonResponse(result, status=200 if result.get('success') else 400)

        except Exception as e:
            logger.error(f"目录管理操作失败：{e}")
            return JsonResponse({"success": False, "message": f"服务器内部错误: {str(e)}"}, status=500)
        finally:
            if db and db.connection:
                db.connection.close()
    else:
        return JsonResponse({"success": False, "message": "仅支持POST请求"}, status=405)



def get_directory_tree(request):
    """
    获取目录树结构

    GET参数:
        dir_entity_id: dir_entity表的id（直接传入）
        parent_id: 父目录id（可选，不传表示获取根目录）
    """
    if request.method == 'GET':
        dir_entity_id = request.GET.get('dir_entity_id')
        parent_id = request.GET.get('parent_id')

        if not dir_entity_id:
            return JsonResponse({
                "status": "error",
                "message": "缺少必要参数：dir_entity_id"
            }, status=400)

        try:
            db = DirectoryDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            db.connect()

            # 获取目录树
            results = db.get_directory_tree(dir_entity_id, parent_id)

            # 格式化返回数据
            directories = []
            for row in results:
                directories.append({
                    "id": row[0],
                    "dir": row[1],
                    "parent_id": row[2],
                    "sort_order": row[3]
                })

            return JsonResponse({
                "status": "success",
                "data": directories
            }, status=200)

        except Exception as e:
            logger.error(f"获取目录树失败：{e}")
            return JsonResponse({
                "status": "error",
                "message": f"操作失败: {str(e)}"
            }, status=500)
        finally:
            if 'db' in locals():
                db.connection.close()

    else:
        return JsonResponse({
            "status": "error",
            "message": "仅支持GET请求"
        }, status=405)