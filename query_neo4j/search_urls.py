import logging
import random
import time
import os
import urllib3
import pymysql
import requests
import json
import ast
from bs4 import BeautifulSoup
import re
from urllib.parse import unquote

# from elasticsearch import Elasticsearch  # 不再直接使用ES客户端，通过SearXNG的elasticsearch引擎查询

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')


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
        except pymysql.MySQLError as e:
            print(f"数据库连接失败：{e}")
            raise


def clean_filename(title):
    """清理文件名中的非法字符"""
    return "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in title)


def is_meaningless_filename(filename):
    """
    检测文件名是否为乱码或无意义

    参数:
    - filename: 文件名（不包含扩展名）

    返回:
    - bool: True表示文件名无意义，需要替换
    """
    if not filename or len(filename.strip()) == 0:
        return True

    # 移除常见的无意义后缀
    cleaned = filename.strip().replace('_', '').replace('-', '').replace('.', '')

    # 检查是否全是数字或字母数字组合（可能是UUID、哈希值等）
    if len(cleaned) > 10 and (cleaned.isalnum() and not any('\u4e00' <= c <= '\u9fff' for c in cleaned)):
        # 检查是否像UUID（包含连字符的长字符串）
        if len(filename) > 25 and ('-' in filename or '_' in filename):
            return True
        # 检查是否全是十六进制字符
        try:
            int(cleaned, 16)
            if len(cleaned) > 15:  # 长的十六进制字符串
                return True
        except ValueError:
            pass

    # 检查常见的无意义文件名
    meaningless_names = [
        '附件', 'attachment', 'file', 'document', 'doc', 'untitled',
        '无标题', '文档', '新建文档', 'new document'
    ]

    if cleaned.lower() in [name.lower() for name in meaningless_names]:
        return True

    # 检查是否主要包含特殊字符或乱码
    chinese_chars = sum(1 for c in filename if '\u4e00' <= c <= '\u9fff')
    english_chars = sum(1 for c in filename if c.isalpha())
    total_meaningful = chinese_chars + english_chars

    if len(filename) > 5 and total_meaningful < len(filename) * 0.3:
        return True

    return False


def generate_filename_from_content(content, file_type=None):
    """
    从content内容生成有意义的文件名

    参数:
    - content: 内容文本
    - file_type: 文件类型扩展名

    返回:
    - str: 生成的文件名
    """
    if not content or not content.strip():
        base_name = "搜索结果"
    else:
        # 提取content的前50个字符作为文件名
        content_clean = content.strip()
        # 移除换行符和多余空格
        content_clean = ' '.join(content_clean.split())
        # 截取前50个字符
        base_name = content_clean[:50]
        # 清理文件名
        base_name = clean_filename(base_name)

    # 如果清理后为空，使用默认名称
    if not base_name.strip():
        base_name = "搜索结果"

    # 添加文件扩展名
    if file_type:
        return f"{base_name}.{file_type.lower()}"
    else:
        return f"{base_name}.html"


def parse_searx_html(html_content, max_results=None):
    """
    解析SearX HTML搜索结果

    参数:
    - html_content: HTML内容
    - max_results: 最大结果数量

    返回:
    - list: 解析后的搜索结果列表
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []

        # 尝试多种可能的选择器
        result_selectors = [
            '.result',  # 标准SearX结果
            '.result-item',  # 某些实例使用的类名
            '.search-result',  # 另一种可能的类名
            'article',  # 有些使用article标签
            '.urls'  # 备选选择器
        ]

        result_elements = []
        used_selector = None

        for selector in result_selectors:
            result_elements = soup.select(selector)
            if result_elements:
                used_selector = selector
                logger.info(f"使用选择器 '{selector}' 找到 {len(result_elements)} 个HTML结果")
                break

        if not result_elements:
            # 如果没有找到标准结果，尝试查找所有外部链接
            logger.info(f"未找到标准结果容器，尝试查找外部链接")
            all_links = soup.find_all('a', href=True)
            external_links = []

            for link in all_links:
                href = link.get('href', '')
                if (href.startswith('http') and
                        'searx' not in href.lower() and
                        'search' not in href.lower()):
                    external_links.append(link)

            # 限制结果数量
            if max_results:
                external_links = external_links[:max_results]

            for i, link in enumerate(external_links):
                href = link.get('href', '')
                title = link.get_text(strip=True) or f"搜索结果 {i + 1}"
                results.append({
                    'url': href,
                    'title': title[:100] + '...' if len(title) > 100 else title,
                    'content': f"来自 {title} 的搜索结果"
                })

            logger.info(f"通过外部链接解析到 {len(results)} 个结果")
            return results

        # 限制解析的元素数量
        if max_results:
            result_elements = result_elements[:max_results]

        # 解析找到的结果
        for i, result_elem in enumerate(result_elements):
            try:
                # 提取URL
                url = None
                link_selectors = [
                    'h3 a',  # 标题链接
                    '.result-title a',  # 结果标题链接
                    'a.result-link',  # 结果链接
                    'a[href^="http"]',  # 任何外部链接
                    'a:first-child'  # 第一个链接
                ]

                for link_selector in link_selectors:
                    link_element = result_elem.select_one(link_selector)
                    if link_element and link_element.get('href'):
                        url = link_element.get('href')
                        break

                if not url:
                    continue

                # 清理URL
                if '/url?q=' in url:
                    # 提取q参数的值
                    match = re.search(r'[?&]q=([^&]+)', url)
                    if match:
                        url = unquote(match.group(1))

                # 提取标题
                title = None
                title_selectors = ['h3', '.result-title', '.title', 'h2', 'h4']

                for title_selector in title_selectors:
                    title_element = result_elem.select_one(title_selector)
                    if title_element:
                        title = title_element.get_text(strip=True)
                        break

                if not title:
                    # 尝试从链接文本获取标题
                    link_element = result_elem.select_one('a')
                    if link_element:
                        title = link_element.get_text(strip=True)

                title = title or f"搜索结果 {i + 1}"

                # 提取内容片段
                content = None
                content_selectors = ['.content', '.snippet', '.description', 'p']

                for content_selector in content_selectors:
                    content_element = result_elem.select_one(content_selector)
                    if content_element:
                        content = content_element.get_text(strip=True)
                        break

                content = content or f"来自 {title} 的搜索结果"

                if url and url.startswith('http'):
                    results.append({
                        'url': url,
                        'title': title[:100] + '...' if len(title) > 100 else title,
                        'content': content[:200] + '...' if len(content) > 200 else content
                    })

            except Exception as e:
                logger.error(f"解析HTML结果 {i + 1} 时出错: {str(e)}")
                continue

        logger.info(f"成功解析HTML内容，获得 {len(results)} 个有效结果")
        return results

    except Exception as e:
        logger.error(f"HTML解析出错: {str(e)}")
        return []


def check_url_exists_for_user_and_xiaoqi(url_list, user_id, xiaoqi_name):
    """
    批量检查URL列表是否在数据库中已存在且属于指定用户和指定小奇项目
    通过一次性查询所有相关文件的URL，然后与传入的URL列表取差集来提高效率

    参数:
    - url_list: 要检查的URL列表
    - user_id: 用户ID
    - xiaoqi_name: 小奇项目名称

    返回:
    - dict: {
        "duplicate_urls": [],      # 重复的URL及其文件信息
        "unique_urls": [],         # 不重复的URL
        "xiaoqi_duplicate": bool
      }
    """
    try:
        # 连接数据库
        db = MySQLDatabase(
            host="114.213.234.179",
            user="koroot",
            password="DMiC-4092",
            database="db_hp"
        )

        db.connect()

        with db.connection.cursor() as cursor:
            # 1. 根据xiaoqi_name查询xiaoqi_new表获取xiaoqi_id
            xiaoqi_query = "SELECT xiaoqi_id FROM xiaoqi_new WHERE xiaoqi_name = %s"
            cursor.execute(xiaoqi_query, (xiaoqi_name,))
            xiaoqi_result = cursor.fetchone()

            if not xiaoqi_result:
                logger.info(f"未找到小奇项目: {xiaoqi_name}，所有URL都为唯一")
                # 如果没有找到xiaoqi项目，所有URL都是唯一的
                return {
                    "duplicate_urls": [],
                    "unique_urls": url_list,
                    "xiaoqi_duplicate": False
                }

            xiaoqi_id = xiaoqi_result[0]
            logger.info(f"找到小奇项目: {xiaoqi_name}, ID: {xiaoqi_id}")

            # 2. 根据xiaoqi_id查询xiaoqi_file表获取关联的file_id列表
            file_ids_query = "SELECT file_id FROM xiaoqi_to_file WHERE xiaoqi_id = %s"
            cursor.execute(file_ids_query, (xiaoqi_id,))
            file_ids_results = cursor.fetchall()

            if not file_ids_results:
                logger.info(f"小奇项目 {xiaoqi_name} 暂无关联文件，所有URL都为唯一")
                # 如果没有关联文件，所有URL都是唯一的
                return {
                    "duplicate_urls": [],
                    "unique_urls": url_list,
                    "xiaoqi_duplicate": False
                }

            file_ids = [result[0] for result in file_ids_results]
            logger.info(f"小奇项目 {xiaoqi_name} 关联文件数量: {len(file_ids)}")

            # 3. 一次性查询所有相关文件的URL信息
            if file_ids:
                # 构建IN查询语句，获取所有相关文件的信息
                placeholders = ','.join(['%s'] * len(file_ids))
                file_query = f"""
                SELECT id, name, path, timestamp, private, userid, url 
                FROM file 
                WHERE id IN ({placeholders}) AND (private = 0 OR (private = 1 AND userid = %s))
                """

                # 参数列表：file_ids + user_id
                params = file_ids + [str(user_id)]
                cursor.execute(file_query, params)
                results = cursor.fetchall()

                # 创建URL到文件信息的映射
                existing_url_to_file = {}
                for result in results:
                    file_id, file_name, minio_path, timestamp, private, userid, file_url = result
                    existing_url_to_file[file_url] = {
                        "file_id": file_id,
                        "file_name": file_name,
                        "minio_path": minio_path,
                        "timestamp": timestamp,
                        "private": private,
                        "userid": userid,
                        "url": file_url,
                        "xiaoqi_id": xiaoqi_id,
                        "xiaoqi_name": xiaoqi_name
                    }

                logger.info(f"小奇项目 {xiaoqi_name} 中共有 {len(existing_url_to_file)} 个文件URL")

                # 4. 与传入的URL列表取差集
                duplicate_urls = []
                unique_urls = []

                for item in url_list:
                    url = item.get('url') if isinstance(item, dict) else item
                    if url in existing_url_to_file:
                        # URL重复，添加到重复列表
                        duplicate_item = item.copy() if isinstance(item, dict) else {"url": item}
                        duplicate_item.update({
                            "duplicate_reason": f"URL已存在于项目 '{xiaoqi_name}' 中",
                            "existing_file_info": existing_url_to_file[url],
                            "duplicate_type": "xiaoqi_duplicate"
                        })
                        duplicate_urls.append(duplicate_item)
                        logger.debug(f"发现重复URL: {url}")
                    else:
                        # URL唯一，添加到唯一列表
                        unique_urls.append(item)

                logger.info(f"去重结果 - 总计: {len(url_list)}, 重复: {len(duplicate_urls)}, 唯一: {len(unique_urls)}")

                return {
                    "duplicate_urls": duplicate_urls,
                    "unique_urls": unique_urls,
                    "xiaoqi_duplicate": True if duplicate_urls else False
                }
            else:
                # 如果没有关联文件，所有URL都是唯一的
                return {
                    "duplicate_urls": [],
                    "unique_urls": url_list,
                    "xiaoqi_duplicate": False
                }

    except Exception as e:
        logger.error(f"批量检查URL去重时发生错误: {str(e)}")
        # 出错时返回所有URL为唯一
        return {
            "duplicate_urls": [],
            "unique_urls": url_list,
            "xiaoqi_duplicate": False
        }
    finally:
        if 'db' in locals() and hasattr(db, 'connection') and db.connection:
            db.connection.close()


def check_url_exists_for_user(url, user_id):
    """
    检查URL是否在数据库中已存在且属于指定用户

    参数:
    - url: 要检查的URL
    - user_id: 用户ID
    返回:
    - dict: {"exists": bool, "file_info": dict or None}
    """
    try:
        # 连接数据库
        db = MySQLDatabase(
            host="114.213.234.179",
            user="koroot",
            password="DMiC-4092",
            database="db_hp"
        )

        db.connect()

        # 查询数据库中是否存在该URL且符合权限条件（public或属于指定用户）
        with db.connection.cursor() as cursor:
            query = "SELECT id, name, path, timestamp, private, userid FROM file WHERE url = %s AND (private = 0 OR (private = 1 AND userid = %s))"
            cursor.execute(query, (url, user_id))
            result = cursor.fetchone()

            if not result:
                return {
                    "exists": False,
                    "file_info": None
                }

            file_id, file_name, minio_path, timestamp, private, userid = result
            logger.info(f"找到用户 {user_id} 的重复URL: {url}, 文件路径: {minio_path}")

            file_info = {
                "file_id": file_id,
                "file_name": file_name,
                "minio_path": minio_path,
                "timestamp": timestamp,
                "private": private,
                "userid": userid,
                "url": url
            }

            return {
                "exists": True,
                "file_info": file_info
            }

    except Exception as e:
        logger.error(f"检查URL去重时发生错误: {str(e)}")
        return {
            "exists": False,
            "file_info": None
        }
    finally:
        if 'db' in locals() and hasattr(db, 'connection') and db.connection:
            db.connection.close()


def process_urls_with_user_deduplication(url_list, user_id, xiaoqi_name=None):
    """
    对URL列表进行基于用户的去重处理，支持小奇项目去重

    参数:
    - url_list: URL列表，每个元素包含url、title、file_name等信息
    - user_id: 用户ID
    - xiaoqi_name: 小奇项目名称（可选）
    返回:
    - dict: {
        "unique_urls": [],  # 去重后的URL列表
        "duplicate_urls": []  # 重复的URL列表
      }
    """
    logger.info(f"开始对用户 {user_id} 的 {len(url_list)} 个URL进行去重检查" +
                (f"，小奇项目: {xiaoqi_name}" if xiaoqi_name else ""))

    # 使用新的批量去重函数
    check_result = check_url_exists_for_user_and_xiaoqi(url_list, user_id, xiaoqi_name)

    unique_urls = check_result.get("unique_urls", [])
    duplicate_urls = check_result.get("duplicate_urls", [])

    logger.info(f"去重完成 - 用户: {user_id}" +
                (f", 小奇项目: {xiaoqi_name}" if xiaoqi_name else "") +
                f", 总数: {len(url_list)}, 唯一: {len(unique_urls)}, 重复: {len(duplicate_urls)}")

    return {
        "unique_urls": unique_urls,
        "duplicate_urls": duplicate_urls
    }


def search_urls(request):
    """
    根据name搜索URL，并返回URL列表和对应将要保存的文件名
    支持基于用户ID的去重操作：检查数据库中是否已存在相同URL且属于当前用户
    支持文件类型过滤：可以搜索特定类型的文件（如pdf、doc等）

    参数:
    - name: 搜索关键词
    - num_pages_to_crawl: 需要爬取的页面数量（默认100）
    - user_id: 用户ID（用于去重检查）
    - enable_deduplication: 是否启用去重功能（默认True）
    - files: 文件类型过滤（可选，如"pdf"、"doc"等，默认为None表示搜索所有类型）

    返回:
    - 包含去重后URL列表和重复URL列表的结果
    """
    try:
        name = request.GET.get("name")
        num_pages_to_crawl = int(request.GET.get("num_pages_to_crawl", 100))  # 增加默认值到100
        user_id = request.GET.get("userID")
        enable_deduplication = request.GET.get("enable_deduplication", "true").lower() == "true"
        files = request.GET.get("files", None)  # 文件类型参数
        xiaoqi_name = request.GET.get("xiaoqi_name", None)
        enable_key_words = request.GET.get("enable_key_words", "false").lower() == "true"
        offline_search = request.GET.get("offline_search", "false").lower() == "true"

        if not name:
            return {"status": "error", "message": "缺少搜索关键词"}

        # 如果启用关键词增强功能，使用专业领域信息增强搜索关键词
        if enable_key_words and xiaoqi_name:
            keywords_list = enhance_keywords_with_domain(name, xiaoqi_name)
        else:
            keywords_list = [name]
        # 使用SearXNG搜索（支持elasticsearch引擎）
        search_result = fetch_search_results_with_searx(
            query=name,
            num_pages_to_crawl=num_pages_to_crawl,
            file_type=files,
            offline_search=offline_search
        )

        if not search_result:
            return {"status": "error", "message": "搜索结果为空"}

        allowed_file_types = ['doc', 'docx', 'ppt', 'pptx', 'txt', 'pdf', 'html']
        filtered_results = []
        for result in search_result:
            url = result.get('url', '').lower()

            url_without_params = url.split('?')[0].split('#')[0]
            match = re.search(r'\.([a-zA-Z0-9]+)$', url_without_params)

            if match:
                file_ext = match.group(1).lower()
                if file_ext in allowed_file_types:
                    filtered_results.append(result)
                else:
                    logger.debug(f"丢弃不符合类型要求的文件: {url} (扩展名: {file_ext})")
            else:
                filtered_results.append(result)

        search_result = filtered_results
        logger.info(f"文件类型过滤后剩余 {len(filtered_results)} 个结果")

        # 如果启用去重功能且提供了用户ID
        if enable_deduplication and user_id and xiaoqi_name:

            dedup_result = process_urls_with_user_deduplication(search_result, user_id, xiaoqi_name)
            unique_urls = dedup_result["unique_urls"]
            duplicate_urls = dedup_result["duplicate_urls"]

            return {
                "status": "success",
                "data": unique_urls,
                "duplicate_urls": duplicate_urls,
                "search_params": {
                    "keyword": name,
                    "file_type": files,
                    "pages_crawled": num_pages_to_crawl,
                    "xiaoqi_name": xiaoqi_name
                },
                "deduplication_summary": {
                    "total_urls": len(search_result),
                    "unique_count": len(unique_urls),
                    "duplicate_count": len(duplicate_urls),
                    "user_id": user_id,
                    "deduplication_enabled": True
                },
                "message": f"搜索完成，找到 {len(unique_urls)} 个唯一URL，{len(duplicate_urls)} 个重复URL" +
                           (f"，文件类型: {files}" if files else "") +
                           (f"，小奇项目: {xiaoqi_name}" if xiaoqi_name else "")
            }
        else:
            if enable_deduplication and not user_id:
                logger.warning("启用了去重功能但未提供用户ID，将返回所有搜索结果")

            return {
                "status": "success",
                "data": {
                    "unique_urls": search_result,
                    "duplicate_urls": []
                },
                "deduplication_summary": {
                    "total_urls": len(search_result),
                    "unique_count": len(search_result),
                    "duplicate_count": 0,
                    "user_id": user_id,
                    "deduplication_enabled": False
                },
                "search_params": {
                    "keyword": name,
                    "file_type": files,
                    "pages_crawled": num_pages_to_crawl,
                    "xiaoqi_name": xiaoqi_name
                },
                "message": f"搜索完成，共找到 {len(search_result)} 个URL" +
                           (f"，文件类型: {files}" if files else "") +
                           (f"，小奇项目: {xiaoqi_name}" if xiaoqi_name else "")
            }

    except Exception as e:
        logger.error(f"搜索URL时发生错误: {str(e)}")
        return {"status": "error", "message": f"搜索过程中出错: {str(e)}"}


def fetch_search_results_with_searx(query: str,
                                    num_pages_to_crawl: int = 10,
                                    file_type: str = None,
                                    offline_search: bool = False):
    """
    直接通过HTTP接口访问SearxNG获取搜索结果
    支持文件类型过滤功能和动态格式选择

    参数:
    - query: 搜索关键词
    - num_pages_to_crawl: 返回结果数量
    - file_type: 文件类型过滤（如"pdf"、"doc"等，None表示不过滤）
    - engines: 搜索引擎，如 "baidu,bing" 或 "elasticsearch" 或 "baidu,bing,elasticsearch"
    """
    # SearXNG服务器配置
    search_url_list = []
    searx_host_list = []
    searx_result_list = []
    accept_header = ''
    engines = []
    format_type = ''
    # 构建搜索查询，如果指定了文件类型则添加filetype过滤
    if file_type in ['doc', 'docx', 'ppt', 'pptx', 'txt', 'pdf']:
        format_type = 'html'
        formatted_query = f"{query} filetype:{file_type}"
        logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
        engines = ['google']
        searx_host_list = ['https://searx.bndkt.io/', 'http://114.213.232.140:18081/']
    elif file_type in ['pdf']:
        format_type = 'html'
        formatted_query = f"{query} filetype:{file_type}"
        logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
        engines = ['baidu', 'bing']
        searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
    else:
        formatted_query = f"{query}"
        logger.info(f"进行常规搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
        if offline_search:
            format_type = 'json'
            engines.append('elasticsearch')
            searx_host_list = ['http://114.213.232.140:18081/']
        else:
            format_type = 'json'
            engines.append('baidu')
            engines.append('bing')
            searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
    search_url_list = [searx_host + 'search' for searx_host in searx_host_list]
    # 统一处理所有引擎（包括elasticsearch）
    if format_type.lower() == 'html':
        accept_header = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    else:
        accept_header = 'application/json'
    engines_str = ','.join(engines)
    params = {
        'q': formatted_query,
        'format': format_type,
        'engines': engines_str,
        'categories': 'general',
        'language': 'auto',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': accept_header,
    }

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            search_url = search_url_list[attempt % len(search_url_list)]

            # 发送HTTP请求
            response = requests.get(
                search_url,
                params=params,
                headers=headers,
                timeout=3,
                verify=False,  # 禁用SSL验证，适用于本地开发
            )

            # 检查响应状态
            if response.status_code == 200:
                logger.info(f"成功获取SearxNG响应，状态码: {response.status_code}, 格式: {format_type}")

                if format_type.lower() == 'html':
                    html_results = parse_searx_html(response.text, num_pages_to_crawl)

                    if html_results:
                        logger.info(f"HTML解析成功，获得 {len(html_results)} 个初步结果")

                        # 转换为标准格式并进行文件类型过滤
                        result_list = []
                        for i, item in enumerate(html_results):
                            url = item.get('url', '')
                            title = item.get('title', '未知标题')
                            content = item.get('content', '')

                            # 如果指定了文件类型，进行过滤
                            if file_type:
                                if not url.lower().endswith(f".{file_type.lower()}"):
                                    logger.debug(f"跳过不匹配文件类型的URL: {url} (期望: {file_type})")
                                    continue

                            # 确保content不为空
                            if not content.strip():
                                content = f"来自 {title} 的搜索结果"

                            title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
                            if is_meaningless_filename(title_without_ext):
                                logger.debug(f"检测到无意义文件名 '{title}'，使用content生成文件名")
                                file_name = generate_filename_from_content(content, file_type)
                            else:
                                # 使用原始title作为文件名
                                safe_title = clean_filename(title_without_ext)
                                if file_type:
                                    file_name = f"{safe_title}.{file_type.lower()}"
                                else:
                                    file_name = f"{safe_title}.html"

                            searx_result_list.append({
                                "url": url,
                                "title": title,
                                "file_name": file_name,
                                "index": i,
                                "content": content.strip(),
                                "file_type": file_type if file_type else "html"
                            })

                        if searx_result_list:
                            actual_count = len(searx_result_list)
                            logger.info(f"HTML解析成功，获得 {actual_count}/{num_pages_to_crawl} 个搜索结果" +
                                        (f"（文件类型: {file_type}）" if file_type else ""))
                            return searx_result_list
                        else:
                            logger.warning(f"文件类型过滤后结果为空: {formatted_query}" +
                                           (f"（文件类型: {file_type}）" if file_type else ""))
                    else:
                        logger.warning(f"HTML解析未找到结果")
                        # 显示部分HTML内容用于调试
                        html_preview = response.text[:500].replace('\n', ' ')
                        logger.debug(f"HTML预览: {html_preview}...")

                else:
                    # JSON解析（默认）
                    try:
                        # 解析JSON响应
                        data = response.json()

                        # 检查是否有搜索结果
                        if 'results' in data:
                            search_results = data['results']
                            logger.info(f"SearxNG原始返回结果数量: {len(search_results)}")

                            # 分析原始结果的基本信息
                            valid_count = 0
                            invalid_count = 0
                            for i, item in enumerate(search_results):
                                if isinstance(item, dict):
                                    url = item.get('url') or item.get('link') or item.get('href')
                                    if url and url.startswith("http"):
                                        valid_count += 1
                                    else:
                                        invalid_count += 1
                                        logger.debug(f"发现无效URL (索引{i}): {url}")

                            logger.info(f"原始结果分析 - 有效URL: {valid_count}, 无效URL: {invalid_count}")

                            # 解析搜索结果
                            searx_result_list.extend(parse_searx_results(search_results, num_pages_to_crawl, file_type))

                            if searx_result_list:
                                actual_count = len(searx_result_list)
                                logger.info(f"JSON解析成功，获得 {actual_count}/{num_pages_to_crawl} 个搜索结果" +
                                            (f"（文件类型: {file_type}）" if file_type else ""))

                                return searx_result_list
                            else:
                                logger.warning(f"搜索结果解析后为空: {formatted_query}")
                        else:
                            logger.warning(f"SearxNG响应中没有找到results字段")
                            logger.debug(f"响应内容: {data}")

                    except ValueError as json_error:
                        logger.error(f"解析SearxNG响应JSON失败: {json_error}")
                        logger.debug(f"响应内容: {response.text[:500]}...")

            else:
                logger.error(f"SearxNG HTTP请求失败，状态码: {response.status_code}")
                logger.debug(f"响应内容: {response.text[:500]}...")

            # 如果本次尝试失败，等待后重试
            if attempt < max_attempts - 1:
                logger.info(f"等待 1 秒后重试...")

        except requests.exceptions.RequestException as e:
            logger.error(f"SearxNG HTTP请求异常 ({attempt + 1}/{max_attempts}): {str(e)}")
            if attempt < max_attempts - 1:
                logger.info(f"等待 1 秒后重试...")
                time.sleep(1)
        except Exception as e:
            logger.error(f"SearxNG搜索过程异常 ({attempt + 1}/{max_attempts}): {str(e)}")
            if attempt < max_attempts - 1:
                logger.info(f"等待 1 秒后重试...")
                time.sleep(1)

    logger.error(f"在 {max_attempts} 次尝试后仍无法获取搜索结果")
    return []


def parse_searx_results(search_results, num_pages_to_crawl: int, file_type: str = None):
    """
    解析SearXNG搜索结果
    增加文件类型验证功能

    参数:
    - search_results: 搜索结果数据
    - num_pages_to_crawl: 需要的结果数量
    - file_type: 指定的文件类型，用于额外验证
    """
    try:
        result_list = []

        # SearxSearchWrapper.results()直接返回结构化的结果列表
        if isinstance(search_results, list):
            results = search_results[:num_pages_to_crawl]
        elif isinstance(search_results, dict) and 'results' in search_results:
            results = search_results['results'][:num_pages_to_crawl]
        elif isinstance(search_results, str):
            # 如果是字符串，尝试解析为JSON
            try:
                import json
                data = json.loads(search_results)
                if isinstance(data, dict) and 'results' in data:
                    results = data['results'][:num_pages_to_crawl]
                elif isinstance(data, list):
                    results = data[:num_pages_to_crawl]
                else:
                    logger.error(f"未知的JSON搜索结果格式: {type(data)}")
                    return []
            except json.JSONDecodeError:
                # 如果不是JSON，使用文本解析
                return parse_text_results(search_results, num_pages_to_crawl, file_type)
        else:
            logger.error(f"未知的搜索结果格式: {type(search_results)}")
            return []

        for i, item in enumerate(results):
            if isinstance(item, dict):
                engine = item.get('engine', 'unknown')

                # 处理elasticsearch引擎的特殊格式
                if engine == 'elasticsearch' and item.get('template') == 'keyvalue.html' and 'kvmap' in item:
                    # 从kvmap中提取数据
                    kvmap = item['kvmap']
                    title = kvmap.get('title', '未知标题')
                    url = kvmap.get('url', '')
                    content = kvmap.get('content', '')
                    file_type_es = kvmap.get('file_type', 'html')
                    minio_path = kvmap.get('minio_path', '')
                    upload_time = kvmap.get('upload_time', '')
                    file_name = kvmap.get('minio_path', '').split('/')[-1] if minio_path else title

                    logger.debug(f"[ES] 处理kvmap格式数据: {title}")
                else:
                    # 处理标准格式的搜索结果
                    # 尝试多个可能的URL字段名
                    url = item.get('link') or item.get('url') or item.get('href')
                    title = item.get('title', '未知标题')

                    content = (item.get('snippet') or
                               item.get('content') or
                               item.get('description') or
                               item.get('abstract') or
                               f"来自 {title} 的搜索结果")  # 默认内容

                    file_name = None  # 将在后面处理

                if url:
                    if url.startswith('http:http'):
                        url = url.replace('http:http', 'http')
                    elif url.startswith('https:https'):
                        url = url.replace('https:https', 'https')

                    # URL解码
                    try:
                        from urllib.parse import unquote
                        if '%' in url:
                            url = unquote(url)
                    except:
                        pass

                    # 验证URL格式
                    if url.startswith("http"):
                        # 如果指定了文件类型，进行额外验证（参考test.py的逻辑）
                        if file_type:
                            if not url.lower().endswith(f".{file_type.lower()}"):
                                logger.debug(f"跳过不匹配文件类型的URL: {url} (期望: {file_type})")
                                continue

                        # 确保content不为空
                        if not content.strip():
                            content = f"来自 {title} 的搜索结果"

                        # 只对非elasticsearch引擎进行智能文件命名
                        if engine != 'elasticsearch' and file_name is None:
                            # 智能文件命名：检查title是否为乱码，如果是则使用content
                            title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
                            if is_meaningless_filename(title_without_ext):
                                logger.debug(f"检测到无意义文件名 '{title}'，使用content生成文件名")
                                file_name = generate_filename_from_content(content, file_type)
                            else:
                                # 使用原始title作为文件名
                                safe_title = clean_filename(title_without_ext)
                                if file_type:
                                    file_name = f"{safe_title}.{file_type.lower()}"
                                else:
                                    file_name = f"{safe_title}.html"

                        # 确定来源标识和文件类型
                        if engine == 'elasticsearch':
                            source = "elasticsearch"
                            # 对于elasticsearch引擎，使用kvmap中的file_type
                            result_file_type = locals().get('file_type_es', 'html')
                        else:
                            source = f"web-{engine}"
                            result_file_type = file_type if file_type else "html"

                        result_list.append({
                            "url": url,
                            "title": title,
                            "file_name": file_name,
                            "index": i,
                            "content": content.strip(),
                            "file_type": result_file_type,
                            "source": source,  # 标识来源
                            "engine": engine,  # 搜索引擎信息
                            "score": item.get('score', 0)  # 添加评分信息
                        })
                        logger.debug(f"[{engine.upper()}] 添加搜索结果: {title} - {url}" +
                                     (f" (文件类型: {result_file_type})" if result_file_type else ""))
                    else:
                        logger.warning(f"跳过无效URL: {url}")
                else:
                    logger.warning(f"跳过缺失URL的结果: {title}")
            else:
                logger.warning(f"跳过非字典格式的结果项: {type(item)}")

        logger.info(f"成功解析 {len(result_list)} 个有效搜索结果" +
                    (f"（文件类型: {file_type}）" if file_type else ""))

        # 按照score进行降序排序（分数越高排在前面）
        result_list.sort(key=lambda x: x.get('score', 0), reverse=True)

        return result_list

    except Exception as e:
        logger.error(f"解析搜索结果时出错: {str(e)}")
        return []


def parse_text_results(text_results: str, num_pages_to_crawl: int, file_type: str = None):
    """
    解析文本格式的搜索结果
    支持文件类型过滤

    参数:
    - text_results: 文本格式的搜索结果
    - num_pages_to_crawl: 需要的结果数量
    - file_type: 指定的文件类型，用于设置文件扩展名
    """
    try:
        result_list = []
        lines = text_results.split('\n')

        current_result = {}
        result_count = 0

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 简单的文本解析逻辑，根据实际返回格式调整
            if line.startswith('Title:'):
                if current_result and 'title' in current_result:
                    # 保存上一个结果
                    if result_count < num_pages_to_crawl:
                        result_list.append(format_result(current_result, result_count, file_type))
                        result_count += 1

                current_result = {'title': line[6:].strip()}
            elif line.startswith('URL:') or line.startswith('Link:'):
                current_result['url'] = line.split(':', 1)[1].strip()

            elif line.startswith('Content:') or line.startswith('Snippet:'):
                current_result['content'] = line.split(':', 1)[1].strip()

        if current_result and 'title' in current_result and result_count < num_pages_to_crawl:
            result_list.append(format_result(current_result, result_count, file_type))

        # 按照score进行降序排序（分数越高排在前面）
        result_list.sort(key=lambda x: x.get('score', 0), reverse=True)
        logger.info(f"文本结果已按评分排序，共 {len(result_list)} 个结果")

        return result_list

    except Exception as e:
        logger.error(f"解析文本搜索结果时出错: {str(e)}")
        return []


def format_result(result_data: dict, index: int, file_type: str = None):
    """
    格式化单个搜索结果
    支持根据文件类型设置合适的文件扩展名

    参数:
    - result_data: 搜索结果数据
    - index: 结果索引
    - file_type: 文件类型，用于设置文件扩展名
    """
    title = result_data.get('title', '未知标题')
    url = result_data.get('url', '')
    content = result_data.get('content', '')

    # 确保content不为空
    if not content.strip():
        content = f"来自 {title} 的搜索结果"

    # 智能文件命名：检查title是否为乱码，如果是则使用content
    title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
    if is_meaningless_filename(title_without_ext):
        file_name = generate_filename_from_content(content, file_type)
    else:
        # 使用原始title作为文件名
        safe_title = clean_filename(title_without_ext)
        if file_type:
            file_name = f"{safe_title}.{file_type.lower()}"
        else:
            file_name = f"{safe_title}.html"

    return {
        "url": url,
        "title": title,
        "file_name": file_name,
        "index": index,
        "content": content,
        "file_type": file_type if file_type else "html",
        "score": 0  # 文本格式解析的结果默认分数为0
    }


def enhance_keywords_with_domain(name, xiaoqi_name):
    """
    根据小奇项目名称查询数据库，获取专业领域信息并与关键词拼接

    参数:
    - name: 原始搜索关键词
    - xiaoqi_name: 小奇项目名称

    返回:
    - str: 增强后的关键词，如果查询失败则返回原始关键词
    """
    if not xiaoqi_name:
        logger.warning("未提供小奇项目名称，使用原始关键词")
        return name

    try:
        # 连接数据库
        db = MySQLDatabase(
            host="114.213.234.179",
            user="koroot",
            password="DMiC-4092",
            database="db_hp"
        )
        db.connect()

        with db.connection.cursor() as cursor:
            # 查询xiaoqi_new表的key_words字段
            query_sql = "SELECT key_words FROM xiaoqi_new WHERE xiaoqi_name = %s"
            cursor.execute(query_sql, (xiaoqi_name,))
            result = cursor.fetchone()

            if result and result[0]:
                # 检查key_words字段是否为空或None
                keywords_str = result[0]
                if not keywords_str or keywords_str.strip() == '':
                    logger.warning(f"项目 {xiaoqi_name} 的关键词信息为空，使用原始关键词")
                    return name

                # 清理关键词字符串
                cleaned_keywords = keywords_str.strip()
                # 去掉外层引号 (包括各种Unicode引号)
                if (cleaned_keywords.startswith(("'", '"', ''', '"', ''', '"')) and
                        cleaned_keywords.endswith(("'", '"', ''', '"', ''', '"'))):
                    cleaned_keywords = cleaned_keywords[1:-1]

                # 去掉方括号
                if cleaned_keywords.startswith('[') and cleaned_keywords.endswith(']'):
                    cleaned_keywords = cleaned_keywords[1:-1]

        db.connection.close()

    except Exception as e:
        logger.error(f"查询专业领域信息时出错: {str(e)}")
        # 查询失败时继续使用原始关键词
        return name

    return name
