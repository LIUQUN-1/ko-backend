import json
import logging
import pymysql
from django.http import HttpRequest, JsonResponse
from query_neo4j.disambiguation import MySQLDatabase  # 假设 MySQLDatabase 在这个路径下
from collections import defaultdict

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')

class FileManagerDatabase(MySQLDatabase):
    """
    继承 MySQLDatabase 类，添加用于文件管理查询的方法。
    """

    def get_files_by_second_and_user(self, second, user_id):
        """
        根据 second 目录名和 user_id 获取所有相关实体及其文件列表。

        查询逻辑:
        1. (dir_to_entity -> dir_entity -> dir_file -> file)
        2. (dir_to_entity -> dir_entity -> dir_entity_more -> dir_more_file -> file)
        使用 UNION 合并两个路径的结果。

        修改：
        通过 JOIN xiaoqi_new 表，使用 xiaoqi_name 替换 entity_id 作为返回字典的键。

        参数:
            second: 目录名 (dir_to_entity.second)
            user_id: 用户ID (dir_entity.userid)

        返回:
            dict: {xiaoqi_name: [file_info_list]}
        """
        results_map = defaultdict(list)
        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # 路径 A: 通过 dir_file 关联
                query_path_a = """
                    SELECT f.id, f.name, f.path, f.url, xn.xiaoqi_name
                    FROM file AS f
                    JOIN dir_file AS df ON f.id = df.file_id
                    JOIN dir_entity AS de ON df.dir_id = de.id
                    JOIN dir_to_entity AS dte ON de.entity_id = dte.entity_id
                    JOIN xiaoqi_new AS xn ON de.entity_id = xn.xiaoqi_id
                    WHERE dte.second = %s AND (f.private = 0 OR de.userid = %s)
                    """

                # 路径 B: 通过 dir_more_file 关联
                query_path_b = """
                    SELECT f.id, f.name, f.path, f.url, xn.xiaoqi_name
                    FROM file AS f
                    JOIN dir_more_file AS dmf ON f.id = dmf.file_id
                    JOIN dir_entity_more AS dem ON dmf.dir_more_id = dem.id
                    JOIN dir_entity AS de ON dem.dir_entity_id = de.id
                    JOIN dir_to_entity AS dte ON de.entity_id = dte.entity_id
                    JOIN xiaoqi_new AS xn ON de.entity_id = xn.xiaoqi_id
                    WHERE dte.second = %s AND (f.private = 0 OR de.userid = %s)
                    """

                # 使用 UNION 合并两个查询结果
                # UNION 会自动去重
                full_query = f"({query_path_a}) UNION ({query_path_b})"

                cursor.execute(full_query, (second, user_id, second, user_id))
                results = cursor.fetchall()

                if not results:
                    return {}

                # 将结果组织成 {xiaoqi_name: [file_list]} 的格式
                for row in results:
                    xiaoqi_name = row['xiaoqi_name']
                    file_info = {
                        "id": row['id'],
                        "name": row['name'],
                        "path": row['path'],
                        "url": row['url']
                    }
                    # 避免重复添加同一个文件（如果 UNION 没去重）
                    if file_info not in results_map[xiaoqi_name]:
                        results_map[xiaoqi_name].append(file_info)

                return dict(results_map)

        except pymysql.MySQLError as e:
            logger.error(f"查询文件失败：{e}")
            raise  # 抛出异常，由视图函数捕获
        finally:
            # 确保连接在操作后关闭
            if self.connection:
                self.connection.close()


def main(request: HttpRequest):
    """
    处理文件管理的主函数，根据请求方法分发任务。
    - GET: 根据 second 目录和 user_id 返回文件信息。
    - POST: (待实现) 处理文件上传、修改等操作。
    """

    # 处理 GET 请求
    if request.method == 'GET':
        second = request.GET.get('second_dir_name')
        user_id = request.GET.get('user_id')

        if not second or not user_id:
            return JsonResponse({"status": "error", "message": "缺少 'second' 或 'user_id' 参数"}, status=400)

        try:
            # 使用 knowledge_subscription.py 中的数据库连接配置
            db = FileManagerDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            db.connect()
            data = db.get_files_by_second_and_user(second, user_id)

            if not data:
                return JsonResponse({"status": "success", "message": "未找到匹配的文件", "data": {}}, status=200)

            return JsonResponse({"status": "success", "data": data}, status=200)

        except pymysql.MySQLError as e:
            logger.error(f"数据库连接或查询失败: {e}")
            return JsonResponse({"status": "error", "message": f"数据库操作失败: {str(e)}"}, status=500)
        except Exception as e:
            logger.error(f"处理GET请求时发生未知错误: {e}")
            return JsonResponse({"status": "error", "message": f"服务器内部错误: {str(e)}"}, status=500)

    # 处理 POST 请求
    elif request.method == 'POST':
        # 这里是 POST 请求的逻辑占位符
        # 你可以在此解析 request.body 来处理文件上传、修改、删除等操作
        try:
            # data = json.loads(request.body)
            # ... 未来的 POST 逻辑 ...
            return JsonResponse({"status": "info", "message": "POST 功能待实现"}, status=501) # 501 Not Implemented

        except json.JSONDecodeError:
            return JsonResponse({"status": "error", "message": "无效的JSON格式"}, status=400)
        except Exception as e:
            logger.error(f"处理POST请求时发生未知错误: {e}")
            return JsonResponse({"status": "error", "message": f"服务器内部错误: {str(e)}"}, status=500)

    # 处理其他所有不支持的请求方法
    else:
        logger.warning(f"不支持的请求方法: {request.method}")
        return JsonResponse({"status": "error", "message": f"不支持的请求方法: {request.method}"}, status=405) # 405 Method Not Allowed