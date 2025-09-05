# send_email_task.py
from datetime import date, datetime

import pymysql
import logging
import requests
import schedule
import time

# ------------------- 配置区 -------------------

# 数据库配置
DB_HOST = "114.213.234.179"
DB_USER = "koroot"
DB_PASSWORD = "DMiC-4092"
DB_NAME = "db_hp"

# 邮件服务器配置
SMTP_SERVER = 'smtp.163.com'
SMTP_PORT = 25
SENDER_EMAIL = 'a1275305462@163.com'
SENDER_PASSWORD = 'CPe3g37hzgVmx4xU' # 实际项目中建议使用环境变量

# Django API 配置
# DJANGO_API_BASE_URL = 'http://127.0.0.1:8000/' # Django项目的地址
DJANGO_API_BASE_URL = 'http://114.213.232.140:38000/'
# ------------------- 日志配置 -------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ------------------- 数据库模块 -------------------

def get_all_user_keywords():
    """查询并返回所有用户的关键词订阅信息"""
    try:
        connection = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        with connection.cursor() as cursor:
            query = "SELECT userid, email, keyword,subscriptionTime,frequency FROM user_keywords"
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    except pymysql.MySQLError as e:
        logger.error(f"从数据库查询订阅信息失败: {e}")
        return []
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


# ------------------- API 客户端模块 -------------------

API_HEADERS = {'Content-Type': 'application/json'}

def call_search_urls(payload):
    """
    调用Django的search_urls接口 (使用GET方法)
    """
    url = f"{DJANGO_API_BASE_URL}/query_neo4j/searchUrls" # 确认URL路径正确
    try:
        # 使用 GET 方法，并通过 params 参数传递数据
        response = requests.get(url, params=payload, headers=API_HEADERS)
        response.raise_for_status() # 如果状态码不是2xx，则抛出异常
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"调用search_urls API失败: {e}")
        return None

def call_crawl_pages(payload):
    """调用Django的crawl_pages接口 (使用POST方法)"""
    url = f"{DJANGO_API_BASE_URL}/query_neo4j/crawlPages" # 确认URL路径正确
    try:
        response = requests.post(url, json=payload, headers=API_HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"调用crawl_pages API失败: {e}")
        return None
# ------------------- 发送邮件模块 -------------------
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_subscription_email(sender_email, receiver_email, smtp_server, smtp_port, sender_password, files,keyword):
    """
    发送更新后的知识订阅电子邮件。

    :param sender_email: 发件人邮箱
    :param receiver_email: 收件人邮箱
    :param smtp_server: SMTP服务器地址
    :param smtp_port: SMTP服务器端口
    :param sender_password: 发件人邮箱密码
    :param files: 要在邮件正文中列出的文件标题列表 (list of strings)
    """

    subject = '【Chace-KO】您的知识订阅已更新'  # 更新了邮件主题，更具品牌识别度

    # --- 邮件正文构建 ---

    # 1. 核心通知部分
    body_content = (
        f"尊敬的用户，您好：\n\n"
        f"感谢您使用 Chace-KO 知识订阅服务。您可以通过以下网址管理您的订阅： https://ko.zhonghuapu.com/Subscriptions\n\n"
        f"系统已通过网络检索与数据库对比，为您订阅的知识实体：{keyword}更新了下列相关文件：\n"
    )

    # 2. 文件列表
    file_list_str = ""
    for f in files:
        file_list_str += f'  \n- {f}'

    # 3. 页脚与服务介绍
    footer = (
        f"\n\n---\n"
        f"本邮件由 Chace-KO 系统自动发送，请勿直接回复。\n\n"
        f"Chace-KO: 一片连通、综合、容纳、制衡、演化的知识海洋。笃志问题求解，贯通古今中外。\n"
        f"本系统服务的范围及用途均适用于并遵循中华人民共和国法律和相关法规。\n\n"
        f"Copyright 2012 - 2025 由 合肥工业大学大数据知识工程实验室 提供技术支持\n"
        f"All Rights Reserved | 皖ICP备19024429号-1"
    )

    # 组合成最终的邮件正文
    final_body = body_content + file_list_str + footer

    # --- 邮件发送逻辑 (与之前相同) ---

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(final_body, 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        print(f"邮件成功发送至 {receiver_email}！")
    except Exception as e:
        print(f"向 {receiver_email} 发送邮件失败: {e}")
    finally:
        if 'server' in locals():
            server.quit()

# ------------------- 核心服务模块 -------------------

def process_all_subscriptions():
    """主服务函数，执行所有订阅处理流程"""
    logger.info("开始处理用户订阅任务...")
    user_keywords = get_all_user_keywords()
    if not user_keywords:
        logger.info("没有找到任何用户订阅。")
        return
    today = date.today() # 获取今天的日期
    for user_id, email, keyword,subscription_time,frequency in user_keywords:
        try:
            # --- 新增的调度逻辑判断 ---
            if not all([frequency, subscription_time]):
                logger.warning(f"用户 {user_id} 的订阅 '{keyword}' 缺少 frequency 或 subscription_time，跳过处理。")
                continue

            if not isinstance(frequency, int) or frequency <= 0:
                logger.warning(f"用户 {user_id} 的订阅 '{keyword}' 的 frequency 值 ({frequency}) 无效，跳过处理。")
                continue

            # 将数据库返回的 subscription_time (可能是 datetime 对象) 转换为 date 对象
            subscription_date = subscription_time.date() if isinstance(subscription_time, datetime) else subscription_time

            days_diff = (today - subscription_date).days # 计算天数差

            # 如果天数差为负（即订阅日期在未来），或者天数差不能被频率整除，则跳过
            if days_diff < 0 or days_diff % frequency != 0:
                logger.info(f"用户 {user_id} 的订阅 '{keyword}' 今天不是预定的更新日，跳过。")
                continue
            logger.info(f"处理用户 {user_id} 的关键词 '{keyword}'...")

            # 1. 调用API获取链接
            search_payload = {
                "name": keyword[:-1],
                "xiaoqi_name": keyword,
                "userID": user_id,
                "num_pages_to_crawl": "5",
                "offline_search": "false",
                "enable_deduplication":"false",
            }
            search_result = call_search_urls(search_payload)
            logger.info(f"用户 {user_id} 的 search_urls 返回结果: {search_result}")

            if not search_result or search_result.get('status') != 'success':
                logger.error(f"获取链接失败: {getattr(search_result, 'get', lambda k, d: '格式错误')('message', '未知错误')}")
                continue

            # 1. 先安全地获取 'data' 键对应的内容
            data_content = search_result.get('data')
            # 2. 判断 data_content 的类型
            if isinstance(data_content, dict):
                # 如果是字典，说明是情况一，再从字典里获取 'unique_urls'
                unique_urls = data_content.get('unique_urls', [])
            elif isinstance(data_content, list):
                # 如果是列表，说明是情况二，它本身就是我们要的结果
                unique_urls = data_content
            # ------------------------

            if not unique_urls:
                logger.warning(f"关键词 '{keyword}' 在API返回结果中未找到唯一链接(unique_urls)。")
                continue

            # 2. 调用API爬取页面
            crawl_payload = {
                "url_list": [{"url": f['url'], "file_name": f['file_name']} for f in unique_urls],
                "name": keyword,
                "userid": user_id,
                'private': 0,
            }
            crawl_result = call_crawl_pages(crawl_payload)
            logger.info(f"用户 {user_id} 的 crawl_pages 返回结果: {crawl_result}")

            if not crawl_result or crawl_result.get('status') not in ['success', 'completed']:
                logger.error(f"爬取页面失败: {getattr(crawl_result, 'get', lambda k, d: '格式错误')('message', '未知错误')}")
                continue

            # 3. 发送邮件 (这里的邮件发送逻辑需要您补充完整)
            file_titles = [f['title'] for f in unique_urls]
            send_subscription_email(SENDER_EMAIL, email, SMTP_SERVER, SMTP_PORT , SENDER_PASSWORD, file_titles,keyword)
            logger.info(f"为用户 {email} 成功处理关键词 '{keyword}'。邮件发送功能待实现。")

        except Exception as e:
            logger.error(f"处理用户 {user_id} 时发生未知异常: {e}", exc_info=True)

    logger.info("所有订阅处理完毕。")


# ------------------- 主程序入口 -------------------

if __name__ == "__main__":
    print("定时任务服务已启动，将在每天02:40执行...")

    # 在这里可以先执行一次用于测试
    process_all_subscriptions()

    schedule.every().day.at("02:40").do(process_all_subscriptions)

    while True:
        # 运行所有已到期的任务
        schedule.run_pending()

        # 正确地获取空-闲时间的值。
        idle_time = schedule.idle_seconds()

        if idle_time is not None and idle_time > 0:
            # 休眠计算出的精准时长
            time.sleep(idle_time)
        else:
            # 如果没有更多任务，则休眠一个默认时间
            time.sleep(60)