import json
import logging
import pymysql
from django.http import HttpRequest, JsonResponse
from query_neo4j.disambiguation import MySQLDatabase
# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')
class EnhancedMySQLDatabase(MySQLDatabase):
    """
    继承 MySQLDatabase 类，添加向 user_keywords 表插入数据的方法。
    """

    def insert_user_keyword(self, user_id, email, keyword, subscriptionTime):
        """
        向 user_keywords 表检查重复记录,插入数据，

        参数:
            user_id: 用户ID
            email: 用户邮箱
            keyword: 关键词
            subscriptionTime: 添加订阅时间字段
        返回:
            message: str
        """
        try:
            with self.connection.cursor() as cursor:
                # 检查重复记录
                check_query = """
                    SELECT COUNT(*) FROM user_keywords 
                    WHERE userid = %s AND keyword = %s AND email = %s
                """
                cursor.execute(check_query, (user_id, keyword, email))
                result = cursor.fetchone()

                if result[0] > 0:
                    return f"您的邮箱{email}之前已经订阅了（{keyword}）相关的知识，系统会每天定时发送邮件，请耐心等待"

                # 插入新记录
                insert_query = """
                    INSERT INTO user_keywords (userid, email, keyword,subscriptionTime)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (user_id, email, keyword,subscriptionTime))
                self.connection.commit()

                return  f"您已启动知识订阅服务，您的邮箱{email}成功订阅了（{keyword}）相关的知识，系统会每天定时发送邮件"

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"插入user_keywords失败：{e}")
            return  f"数据库错误: {str(e)}"
        finally:
            # 确保连接在操作后关闭
            self.connection.close()
    def delete_user_keyword(self, user_id, email, keyword):
        """
        删除 user_keywords 表中指定用户的记录。

        参数:
            user_id: 用户ID
            email: 用户邮箱
            keyword: 关键词

        返回:
             message: str
        """
        try:
            with self.connection.cursor() as cursor:
                # 删除指定用户的记录
                delete_query = """
                    DELETE FROM user_keywords 
                    WHERE userid = %s AND email = %s AND keyword = %s
                """
                cursor.execute(delete_query, (user_id, email, keyword))
                self.connection.commit()

                if cursor.rowcount > 0:
                    return  "删除成功"
                else:
                    return "没有找到匹配的记录"

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"删除user_keywords失败：{e}")
            return  f"数据库错误: {str(e)}"
        finally:
            # 确保连接在操作后关闭
            self.connection.close()
    def delete_user_keywords(self, user_id, keyword):
        """
        删除 user_keywords 表中指定用户的多个关键词记录。

        参数:
            user_id: 用户ID
            keywords: 关键词列表

        返回:
             message: str
        """
        try:
            with self.connection.cursor() as cursor:
                # 删除指定用户的多个关键词记录
                delete_query = """
                    DELETE FROM user_keywords 
                    WHERE userid = %s AND keyword = %s
                """
                cursor.execute(delete_query, (user_id, keyword))
                self.connection.commit()

                if cursor.rowcount > 0:
                    return "删除成功"
                else:
                    return "没有找到匹配的记录"

        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"删除user_keywords失败：{e}")
            return f"数据库错误: {str(e)}"
        finally:
            # 确保连接在操作后关闭
            self.connection.close()
    def get_all_user_keywords(self):
        """
        查询 user_keywords 表并返回所有记录。

        返回:
            list: 包含所有记录的列表，每条记录是一个字典格式
        """
        try:
            with self.connection.cursor() as cursor:
                # 查询所有记录
                select_query = "SELECT userid, email, keyword FROM user_keywords"
                cursor.execute(select_query)
                results = cursor.fetchall()

                return results

        except pymysql.MySQLError as e:
            logger.error(f"查询user_keywords失败：{e}")
            return []  # 返回空列表表示查询失败
        finally:
            # 确保连接在操作后关闭
            self.connection.close()
    def get_user_keywords(self, userid):
        """
        查询 user_keywords 表并返回指定用户的记录。
        参数:
            userid: 用户ID
        返回:
            list: 包含指定用户记录的列表，每条记录是一个字典格式
        """
        try:
            with self.connection.cursor() as cursor:
                # 查询指定用户的记录
                select_query = "SELECT email, keyword,subscriptionTime FROM user_keywords WHERE userid = %s"
                cursor.execute(select_query, (userid,))
                results = cursor.fetchall()
                return results

        except pymysql.MySQLError as e:
            logger.error(f"查询user_keywords失败：{e}")
            return []
    def get_xiaoqi_by_name(self, keyword):
        """
        查询 xiaoqi_data 表并返回包含指定关键词的名称记录。
        参数:
            keyword: 搜索关键词
        返回:
            list: 包含匹配记录的列表
        """
        try:
            with self.connection.cursor() as cursor:
                # 使用 LIKE 进行模糊查询，添加通配符 %
                select_query = "SELECT DISTINCT directory FROM xiaoqi_new WHERE xiaoqi_name = %s"
                cursor.execute(select_query, (keyword,))  # 直接使用keyword
                results = cursor.fetchall()
                return results

        except pymysql.MySQLError as e:
            logger.error(f"查询失败：{e}")
            return []
def knowledge_subscription(request):
    """
    处理用户启用知识订阅服务的请求，存入数据库
    :param request:
        receiver_email: 收件人邮箱
        user_id: 用户ID（用于去重检查）
        name: 搜索关键词
        subscriptionTime: 添加订阅时间字段
        operation : 操作类型（insert 或 delete）
    :return:
    """
    # 接收参数 接收方，
    if request.method == 'POST':
        try:
            data = json.loads(request.body)  # 手动解析JSON
        except json.JSONDecodeError:
            return JsonResponse({"status": "error", "message": "无效的JSON格式"}, status=400)

        receiver_email = data['receiver_email']# 收件人邮箱
        name = data['name']  # 搜索关键词
        user_id = data['user_id']  # 用户ID
        operation = data['operation']
        if operation == 'insert':
            """
            启用知识订阅服务
            """
            subscriptionTime = data['subscription_time']
            # 检查数据库 订阅是否已存在
            try:
                db = EnhancedMySQLDatabase(
                    host="114.213.234.179",
                    user="koroot",
                    password="DMiC-4092",
                    database="db_hp"
                )
                # 建立数据库连接
                db.connect()
                message = db.insert_user_keyword(user_id, receiver_email, name,subscriptionTime)

                return JsonResponse({"status": "success", "message": message}, status=200)
            except Exception as e:
                return JsonResponse({"status": "error", "message": f"数据库操作失败: {str(e)}"}, status=500)
        elif operation == 'delete':
            """
            删除订阅信息
            """
            keyword = data['name']
            try:
                db = EnhancedMySQLDatabase(
                    host="114.213.234.179",
                    user="koroot",
                    password="DMiC-4092",
                    database="db_hp"
                )
                # 建立数据库连接
                db.connect()
                if receiver_email == '':
                    message = db.delete_user_keywords(user_id, keyword)
                else:
                    message = db.delete_user_keyword(user_id, receiver_email,keyword)
                return JsonResponse({"status": "success", "message": message}, status=200)
            except Exception as e:
                return JsonResponse({"status": "error", "message": f"数据库操作失败: {str(e)}"}, status=500)
        else:
            return JsonResponse({"status": "error", "message": f"请确定操作"}, status=500)
def get_subscriptionInfo(request):
    """
    获取用户订阅信息
    :param request:
        user_id: 用户ID
        可选：name: 搜索关键词
    :return:
    """
    if request.method == 'GET':
        user_id = request.GET.get('user_id')
        name = request.GET.get('name')
        if not user_id:
            return JsonResponse({"status": "error", "message": "缺少用户ID"}, status=400)

        try:
            db = EnhancedMySQLDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            # 建立数据库连接
            db.connect()
            if name:
                # 如果提供了name参数，则查询特定关键词
                results = db.get_user_keywords(user_id)
                results = [result for result in results if result[1] == name]
                if len(results) > 0:
                    subscribed = True
                else:
                    subscribed = False
                return JsonResponse({"status": "success", "subscribed": subscribed}, status=200)
            else:
                # 否则查询所有关键词
                results = db.get_user_keywords(user_id)
                final_results = []
                for email, name,  time in results:
                    time = time.strftime('%Y-%m-%d')
                    # 以name作为keyword调用查询
                    query_result = db.get_xiaoqi_by_name(name)
                    # 将查询结果添加到最终结果中
                    final_results.append((email, name, query_result[0][0],time))  # (邮箱, 原始名称, 查询到的名称)
                return JsonResponse({"status": "success", "data": final_results}, status=200)
        except Exception as e:
            return JsonResponse({"status": "error", "message": f"数据库操作失败: {str(e)}"}, status=500)








