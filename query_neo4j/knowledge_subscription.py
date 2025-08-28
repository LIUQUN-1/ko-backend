import json
import logging
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pymysql
from django.http import HttpRequest, JsonResponse
import schedule

from query_neo4j.search_urls import search_urls
from query_neo4j.crawl_pages import crawl_pages
from query_neo4j.disambiguation import MySQLDatabase
# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')
def send_email_with_attachment(sender_email, receiver_email, smtp_server, smtp_port, sender_password,files,attachment_file=None):
    """
    发送带附件的电子邮件
    :param sender_email: 发件人邮箱
    :param receiver_email: 收件人邮箱
    :param attachment_file: 附件文件路径
    :param smtp_server: SMTP服务器地址
    :param smtp_port: SMTP服务器端口
    :param sender_password: 发件人邮箱密码
    """

    subject = 'KO知识订阅'  # 邮件主题
    body = '感谢您启用KO知识订阅服务。\n通过网络检索与系统数据库对比，我们选取了几个您可能感兴趣的网站'  # 邮件正文

    for f in files:
        body += f'  \n{f}'

    # 创建 MIME 邮件对象
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # 添加邮件正文
    msg.attach(MIMEText(body, 'plain'))

    # 添加附件
    try:
        if attachment_file is not None:
            with open(attachment_file, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={attachment_file.split("/")[-1]}')
                msg.attach(part)
    except Exception as e:
        print(f"附件添加失败: {e}")
        return

    # 连接到 SMTP 服务器并发送邮件
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 启动 TLS 加密
        server.login(sender_email, sender_password)  # 登录发件人邮箱
        text = msg.as_string()  # 转换邮件对象为字符串
        server.sendmail(sender_email, receiver_email, text)  # 发送邮件
        print("邮件发送成功！")
    except Exception as e:
        print(f"邮件发送失败: {e}")
    finally:
        server.quit()  # 退出 SMTP 服务器
def send_email_task():
    """定时任务函数：从数据库中检索数据并调用 send_email 发送邮件
    sender_email: 发件人邮箱
    receiver_email: 收件人邮箱
    smtp_server:SMTP服务器地址
    smtp_port:SMTP服务器端口
    sender_password:SMTP服务器密码

    调用search_urls接口需要：
        - name: 搜索关键词
        - num_pages_to_crawl: 需要爬取的页面数量（默认100）
        - user_id: 用户ID（用于去重检查）
    """
    try:
        db = EnhancedMySQLDatabase(
            host="114.213.234.179",
            user="koroot",
            password="DMiC-4092",
            database="db_hp"
        )
        # 建立数据库连接
        db.connect()
        user_keywords = db.get_all_user_keywords()  # 从数据库中获取所有用户的关键词订阅信息
    
        if not user_keywords or len(user_keywords) == 0:
            logger.info("没有检索到用户数据")
            return
    
        for entry in user_keywords:
            user_id = entry[0]
            receiver_email = entry[1]
            name = entry[2]

    
            # 构造伪造的 GET 请求参数
            fake_get_request = HttpRequest()
            fake_get_request.GET = {
                "name": name,
                "userID": user_id,
                "num_pages_to_crawl": "5",  # 必须是字符串（模拟 QueryDict）
                "offline_search": "false",  # 是否离线搜索
            }
    
            # 爬取网页链接，调用 search_urls 并获取响应
            search_response = search_urls(fake_get_request)
    
            # 标题+网址
            files_str = []
            files = []
            for file in search_response['data']['unique_urls']:
                files_str.append(file['title'] + ':  ' + file['url'])
                temp_dict = {}
                temp_dict["url"] = file['url']
                temp_dict["file_name"] = file['title']
                files.append(temp_dict)
    
            # 爬取网页文件并上传
            fake_post_request = HttpRequest()
            fake_post_request.method = 'POST'
            fake_post_request._body = json.dumps({
                "url_list": files,
                "name": name,
                "userid": user_id,
            }).encode('utf-8')  # 必须编码为 bytes
            fake_post_request.content_type = 'application/json'
    
            crawl_result = crawl_pages(fake_post_request)
    
            # 设置发件人的邮箱信息
            sender_email = 'a1275305462@163.com'
            smtp_server = 'smtp.163.com'
            smtp_port = 25
            sender_password = 'CPe3g37hzgVmx4xU'  # 发件人邮箱密码或应用密码
    
            # 发送邮件
            send_email_with_attachment(sender_email, receiver_email, smtp_server, smtp_port, sender_password, files_str)

        logger.info(f"邮件已发送至 {receiver_email}")
    except Exception as e:
        logger.error(f"发送邮件任务失败: {e}")
    
import django
import os
def start_scheduled_task():
    """启动定时任务，每天执行一次发送邮件任务"""
    # 设置 Django 环境变量（替换 'your_project' 为你的项目名）
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'KoDjango.settings')
    django.setup()  # 加载 Django 配置

    schedule.every().day.at("19:09").do(send_email_task)  # 设定每天10点执行任务

    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次任务
class EnhancedMySQLDatabase(MySQLDatabase):
    """
    继承 MySQLDatabase 类，添加向 user_keywords 表插入数据的方法。
    """

    def insert_user_keyword(self, user_id, email, keyword):
        """
        向 user_keywords 表检查重复记录,插入数据，

        参数:
            user_id: 用户ID
            email: 用户邮箱
            keyword: 关键词

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
                    INSERT INTO user_keywords (userid, email, keyword)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (user_id, email, keyword))
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
                select_query = "SELECT email, keyword FROM user_keywords WHERE userid = %s"
                cursor.execute(select_query, (userid,))
                results = cursor.fetchall()
                return results

        except pymysql.MySQLError as e:
            logger.error(f"查询user_keywords失败：{e}")
            return []
def knowledge_subscription(request):
    """
    处理用户启用知识订阅服务的请求，存入数据库
    :param request:
        receiver_email: 收件人邮箱
        user_id: 用户ID（用于去重检查）
        name: 搜索关键词
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
                message = db.insert_user_keyword(user_id, receiver_email, name)

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
    :return:
    """
    if request.method == 'GET':
        user_id = request.GET.get('user_id')
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
            results = db.get_user_keywords(user_id)
            return JsonResponse({"status": "success", "data": results}, status=200)
        except Exception as e:
            return JsonResponse({"status": "error", "message": f"数据库操作失败: {str(e)}"}, status=500)


if __name__ == '__main__':

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'KoDjango.settings')
    django.setup()  # 加载 Django 配置
    send_email_task()
    # start_scheduled_task()



