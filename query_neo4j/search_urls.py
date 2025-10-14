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

def filter_search_results(search_results, query, use_first_word_only=True):
    """
    过滤搜索结果
    :param search_results: 原始搜索结果列表
    :param query: 查询字符串
    :param use_first_word_only: 是否只使用第一个词进行过滤
    :return: 过滤后的结果列表
    """
    if not query or not query.strip():
        return search_results  # 或者返回 []

    query = query.strip()

    if use_first_word_only and ' ' in query:
        # 使用第一个词
        search_term = query.split()[0]
    else:
        # 使用整个query
        search_term = query

    return [
        result for result in search_results
        if search_term.lower() in result.get('title', '').lower()
           or search_term.lower() in result.get('content', '').lower()
    ]
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
        num_pages_to_crawl = int(request.GET.get("num_pages_to_crawl", 20))
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
            offline_search=offline_search,
        )
        # print("search_result",search_result)
        # urls_count = len(search_result)
        # print(f"fetch_search_results_with_sear的輸出search_result的数量: {urls_count}")

        search_result = filter_search_results(search_result, name, use_first_word_only=True)
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
                                    num_pages_to_crawl: int = 60,
                                    file_type: str = None,
                                    offline_search: bool = False):
    """
    直接通过HTTP接口访问SearxNG获取搜索结果
    优化实例选择策略，避免实例2频繁403，并添加详细实例使用追踪
    """
    # 配置增强的User-Agent池
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    ]

    # 构建搜索查询
    if file_type in ['doc', 'docx', 'ppt', 'pptx', 'txt', 'pdf']:
        format_type = 'html'
        formatted_query = f"{query} filetype:{file_type}"
        logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
        engines = ['google']
        # 优先使用实例1，实例2作为备用
        searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
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
            engines = ['elasticsearch']
            # 离线搜索只使用实例1
            searx_host_list = ['http://114.213.232.140:18081/']
        else:
            format_type = 'json'
            engines = ['baidu','bing','360search', 'quark', 'sogou']
        ####'bing'
            # 常规搜索优先使用实例1
            searx_host_list = ['http://114.213.232.140:18081/']###, 'https://searx.bndkt.io/'

    search_url_list = [searx_host + 'search' for searx_host in searx_host_list]

    # 实例性能统计
    instance_stats = {
        url: {'success': 0, 'fail': 0, 'last_403': 0}
        for url in search_url_list
    }

    # 详细页面实例使用记录
    page_detailed_stats = {}

    if format_type.lower() == 'html':
        accept_header = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    else:
        accept_header = 'application/json'

    engines_str = ','.join(engines)

    max_attempts = 3
    timeout_config = 5
    results_per_page = 20
    pages_to_fetch = max(1, (num_pages_to_crawl + results_per_page - 1) // results_per_page)

    logger.info(f"需要获取 {pages_to_fetch} 页来满足 {num_pages_to_crawl} 个结果的需求")
    logger.info(f"可用实例: {search_url_list}")

    # 创建会话
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=2)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    searx_result_list = []
    page_status = {}
    consecutive_empty_pages = 0
    max_consecutive_empty_pages = 2

    for page in range(pages_to_fetch):
        current_page = page + 1
        logger.info(f"=== 开始获取第 {current_page}/{pages_to_fetch} 页 ===")

        # 初始化当前页的详细记录
        page_detailed_stats[current_page] = {
            'attempts': [],  # 记录每次尝试的详细信息
            'final_instance': None,  # 最终成功的实例
            'success': False,
            'results_count': 0
        }

        params = {
            'q': formatted_query,
            'format': format_type,
            'engines': engines_str,
            'categories': 'general',
            'language': 'auto',
            'pageno': current_page,
            '_': str(int(time.time() * 1000))[-6:]
        }

        page_success = False
        page_results_count = 0
        page_error = None

        # 智能选择实例：优先选择成功率高的实例
        sorted_instances = sorted(
            search_url_list,
            key=lambda url: (
                -instance_stats[url]['success'],  # 成功率高的优先
                instance_stats[url]['last_403']  # 最近403时间久的优先
            )
        )

        logger.info(f"第{current_page}页实例优先级: {[url.split('//')[-1].split('/')[0] for url in sorted_instances]}")

        for searx_instance_idx, search_url in enumerate(sorted_instances):
            if page_success:
                break

            # 如果该实例最近有403错误，增加等待时间
            last_403_time = instance_stats[search_url]['last_403']
            if last_403_time > 0 and time.time() - last_403_time < 300:  # 5分钟内有过403
                wait_403 = 5  # 等待10秒
                logger.info(f"实例 {search_url} 最近有过403错误，等待{wait_403}秒后使用")
                time.sleep(wait_403)

            for attempt in range(max_attempts):
                # 记录每次尝试的详细信息
                attempt_info = {
                    'instance': search_url,
                    'instance_index': searx_instance_idx + 1,
                    'attempt_number': attempt + 1,
                    'status_code': None,
                    'success': False,
                    'error': None,
                    'timestamp': time.time(),
                    'results_count': 0
                }

                try:
                    # 为实例2使用更保守的请求头
                    if 'searx.bndkt.io' in search_url:
                        # 实例2需要更真实的浏览器头
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'DNT': '1',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Cache-Control': 'max-age=0'
                        }
                    else:
                        # 实例1可以使用更宽松的请求头
                        headers = {
                            'User-Agent': random.choice(user_agents),
                            'Accept': accept_header,
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                            'DNT': random.choice(['0', '1']),
                            'Connection': 'keep-alive'
                        }

                    logger.debug(
                        f"第{current_page}页 尝试{attempt + 1}/{max_attempts} 使用实例 {searx_instance_idx + 1}: {search_url}")

                    # 发送请求
                    response = session.get(
                        search_url,
                        params=params,
                        headers=headers,
                        timeout=timeout_config,
                        verify=False,
                    )

                    attempt_info['status_code'] = response.status_code

                    if response.status_code == 200:
                        logger.info(f"✅ 第{current_page}页在实例 {search_url} 上HTTP请求成功")
                        instance_stats[search_url]['success'] += 1
                        attempt_info['success'] = True

                        if format_type.lower() == 'html':
                            html_results = parse_searx_html(response.text, num_pages_to_crawl)
                            page_results_count = len(html_results) if html_results else 0
                            attempt_info['results_count'] = page_results_count

                            if html_results and page_results_count > 0:
                                logger.info(f"第{current_page}页HTML解析成功，获得 {page_results_count} 个结果")

                                processed_count = 0
                                for i, item in enumerate(html_results):
                                    url = item.get('url', '')
                                    title = item.get('title', '未知标题')
                                    content = item.get('content', '')

                                    if file_type and not url.lower().endswith(f".{file_type.lower()}"):
                                        continue

                                    if not content.strip():
                                        content = f"来自 {title} 的搜索结果"

                                    title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
                                    if is_meaningless_filename(title_without_ext):
                                        file_name = generate_filename_from_content(content, file_type)
                                    else:
                                        safe_title = clean_filename(title_without_ext)
                                        file_name = f"{safe_title}.{file_type.lower()}" if file_type else f"{safe_title}.html"

                                    searx_result_list.append({
                                        "url": url,
                                        "title": title,
                                        "file_name": file_name,
                                        "index": len(searx_result_list),
                                        "content": content.strip(),
                                        "file_type": file_type if file_type else "html",
                                        "page": current_page,
                                        "instance": search_url  # 记录使用的实例
                                    })
                                    processed_count += 1

                                logger.info(f"第{current_page}页成功处理 {processed_count}/{page_results_count} 个结果")
                                page_success = True
                                consecutive_empty_pages = 0
                                page_detailed_stats[current_page]['final_instance'] = search_url
                                page_detailed_stats[current_page]['success'] = True
                                page_detailed_stats[current_page]['results_count'] = processed_count

                            else:
                                logger.warning(f"第{current_page}页HTML解析未找到结果")
                                page_error = "HTML解析无结果"
                                consecutive_empty_pages += 1
                                attempt_info['error'] = page_error

                        else:
                            # JSON解析
                            try:
                                data = response.json()
                                page_results_count = len(data.get('results', []))
                                attempt_info['results_count'] = page_results_count

                                if 'results' in data and page_results_count > 0:
                                    logger.info(f"第{current_page}页JSON解析成功，获得 {page_results_count} 个原始结果")

                                    parsed_results = parse_searx_results(data['results'], num_pages_to_crawl, file_type)
                                    processed_count = len(parsed_results)

                                    for result in parsed_results:
                                        result['index'] = len(searx_result_list)
                                        result['page'] = current_page
                                        result['instance'] = search_url  # 记录使用的实例
                                        searx_result_list.append(result)

                                    logger.info(
                                        f"第{current_page}页成功处理 {processed_count}/{page_results_count} 个结果")
                                    page_success = True
                                    consecutive_empty_pages = 0
                                    page_detailed_stats[current_page]['final_instance'] = search_url
                                    page_detailed_stats[current_page]['success'] = True
                                    page_detailed_stats[current_page]['results_count'] = processed_count

                                else:
                                    logger.warning(f"第{current_page}页JSON解析未找到结果")
                                    page_error = "JSON解析无结果"
                                    consecutive_empty_pages += 1
                                    attempt_info['error'] = page_error

                            except ValueError as json_error:
                                logger.error(f"第{current_page}页JSON解析失败: {json_error}")
                                page_error = f"JSON解析错误: {json_error}"
                                attempt_info['error'] = page_error

                    elif response.status_code == 403:
                        logger.warning(f"❌ 第{current_page}页在实例 {search_url} 上返回403禁止访问")
                        instance_stats[search_url]['fail'] += 1
                        instance_stats[search_url]['last_403'] = time.time()  # 记录403时间
                        page_error = f"HTTP错误: 403禁止访问"
                        attempt_info['error'] = page_error

                        # 如果是实例2，减少使用频率
                        if 'searx.bndkt.io' in search_url:
                            logger.info("实例2被限制，将降低其优先级")

                        break  # 立即切换到下一个实例

                    elif response.status_code in [429, 503]:
                        logger.warning(f"⚠️ 第{current_page}页在实例 {search_url} 上返回{response.status_code}限流")
                        instance_stats[search_url]['fail'] += 1
                        page_error = f"HTTP错误: {response.status_code}"
                        attempt_info['error'] = page_error
                        wait_time = (attempt + 1) * 5
                        logger.info(f"遇到限流，等待{wait_time}秒后重试...")
                        time.sleep(wait_time)

                    else:
                        logger.error(f"❌ 第{current_page}页在实例 {search_url} 上HTTP失败: {response.status_code}")
                        instance_stats[search_url]['fail'] += 1
                        page_error = f"HTTP错误: {response.status_code}"
                        attempt_info['error'] = page_error

                    if page_success:
                        break

                except requests.exceptions.Timeout:
                    logger.error(f"⏰ 第{current_page}页在实例 {search_url} 上请求超时")
                    instance_stats[search_url]['fail'] += 1
                    page_error = "请求超时"
                    attempt_info['error'] = page_error
                    if attempt < max_attempts - 1:
                        wait_time = (attempt + 1) * 2
                        logger.info(f"等待{wait_time}秒后重试...")
                        time.sleep(wait_time)

                except requests.exceptions.ConnectionError:
                    logger.error(f"🔌 第{current_page}页在实例 {search_url} 上连接错误")
                    instance_stats[search_url]['fail'] += 1
                    page_error = "连接错误"
                    attempt_info['error'] = page_error
                    if attempt < max_attempts - 1:
                        wait_time = (attempt + 1) * 3
                        logger.info(f"等待{wait_time}秒后重试...")
                        time.sleep(wait_time)

                except Exception as e:
                    logger.error(f"💥 第{current_page}页在实例 {search_url} 上异常: {str(e)}")
                    instance_stats[search_url]['fail'] += 1
                    page_error = f"异常: {str(e)}"
                    attempt_info['error'] = page_error
                    if attempt < max_attempts - 1:
                        time.sleep(1)

                finally:
                    # 记录这次尝试的详细信息
                    page_detailed_stats[current_page]['attempts'].append(attempt_info)

        # 记录页面状态
        page_status[current_page] = {
            'success': page_success,
            'results_count': page_results_count,
            'error': page_error,
            'total_so_far': len(searx_result_list)
        }

        # 记录当前页的最终状态
        logger.info(f"=== 第{current_page}页处理汇总 ===")
        logger.info(f"最终状态: {'✅ 成功' if page_success else '❌ 失败'}")
        if page_success:
            logger.info(f"成功实例: {page_detailed_stats[current_page]['final_instance']}")
        logger.info(f"本页获得结果数: {page_detailed_stats[current_page]['results_count']}")
        logger.info(f"累计结果数: {len(searx_result_list)}")

        # 输出当前页的详细尝试记录
        logger.info(f"第{current_page}页详细尝试记录:")
        for attempt in page_detailed_stats[current_page]['attempts']:
            status = "✅ 成功" if attempt['success'] else f"❌ 失败(状态码: {attempt['status_code']})"
            if attempt['error']:
                status += f" - 错误: {attempt['error']}"
            logger.info(
                f"  实例{attempt['instance_index']}({attempt['instance']}) 尝试{attempt['attempt_number']}: {status}")

        # 检查停止条件
        if consecutive_empty_pages >= max_consecutive_empty_pages:
            logger.warning(f"连续 {consecutive_empty_pages} 页无结果，停止分页")
            break

        if len(searx_result_list) >= num_pages_to_crawl:
            logger.info(f"已收集足够结果 ({len(searx_result_list)}/{num_pages_to_crawl})，停止分页")
            break

        # 页面间延迟
        delay = random.uniform(3.0, 6.0)  # 增加延迟避免频繁请求
        logger.info(f"等待{delay:.2f}秒后获取下一页...")
        time.sleep(delay)

    # 关闭会话并输出详细统计
    session.close()

    # 输出实例使用统计
    logger.info("=== 实例使用统计 ===")
    for url, stats in instance_stats.items():
        total = stats['success'] + stats['fail']
        if total > 0:
            success_rate = stats['success'] / total * 100
        else:
            success_rate = 0
        logger.info(f"实例 {url}: 成功 {stats['success']}, 失败 {stats['fail']}, 成功率 {success_rate:.1f}%")

    # 输出详细页面实例使用统计
    logger.info("=== 详细页面实例使用统计 ===")
    successful_pages = 0
    failed_pages = 0

    for page_num, stats in page_detailed_stats.items():
        if stats['success']:
            successful_pages += 1
            logger.info(f"第{page_num}页: ✅ 成功 - 实例: {stats['final_instance']} - 结果数: {stats['results_count']}")
        else:
            failed_pages += 1
            logger.info(f"第{page_num}页: ❌ 失败")

        # 输出该页的所有尝试记录
        for attempt in stats['attempts']:
            status = "成功" if attempt['success'] else f"失败(状态码: {attempt['status_code']})"
            error_info = f" - 错误: {attempt['error']}" if attempt['error'] else ""
            logger.info(f"  → 尝试{attempt['attempt_number']}: 实例{attempt['instance_index']} - {status}{error_info}")

    logger.info(f"=== 搜索总结 ===")
    logger.info(f"成功页面: {successful_pages}, 失败页面: {failed_pages}, 总页面: {pages_to_fetch}")
    logger.info(f"最终获取结果数: {len(searx_result_list)}/{num_pages_to_crawl}")

    # 最终结果处理
    if searx_result_list:
        actual_count = len(searx_result_list)
        if actual_count > num_pages_to_crawl:
            searx_result_list = searx_result_list[:num_pages_to_crawl]
            logger.info(f"截断结果到 {num_pages_to_crawl} 个")

        return searx_result_list
    else:
        logger.warning("所有页面搜索完成后未找到任何结果")
        return []



#
#
# def fetch_search_results_with_searx(query: str,
#                                     num_pages_to_crawl: int = 120,
#                                     file_type: str = None,
#                                     offline_search: bool = False):
#     """
#     直接通过HTTP接口访问SearxNG获取搜索结果
#     支持文件类型过滤功能和动态格式选择
#
#     参数:
#     - query: 搜索关键词
#     - num_pages_to_crawl: 返回结果数量
#     - file_type: 文件类型过滤（如"pdf"、"doc"等，None表示不过滤）
#     - engines: 搜索引擎，如 "baidu,bing" 或 "elasticsearch" 或 "baidu,bing,elasticsearch"
#     """
#     # SearXNG服务器配置
#     search_url_list = []
#     searx_host_list = []
#     searx_result_list = []
#     accept_header = ''
#     engines = []
#     format_type = ''
#
#     # 构建搜索查询，如果指定了文件类型则添加filetype过滤
#     if file_type in ['doc', 'docx', 'ppt', 'pptx', 'txt', 'pdf']:
#         format_type = 'html'
#         formatted_query = f"{query} filetype:{file_type}"
#         logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         engines = ['google']
#         searx_host_list = ['https://searx.bndkt.io/', 'http://114.213.232.140:18081/']
#     elif file_type in ['pdf']:
#         format_type = 'html'
#         formatted_query = f"{query} filetype:{file_type}"
#         logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         engines = ['baidu', 'bing']
#         searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
#     else:
#         formatted_query = f"{query}"
#         logger.info(f"进行常规搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         if offline_search:
#             format_type = 'json'
#             engines.append('elasticsearch')
#             searx_host_list = ['http://114.213.232.140:18081/']
#         else:
#             format_type = 'json'
#             engines.append('360search')
#             engines.append('quark')
#             engines.append('sogou')
#             engines.append('baidu')
#             engines.append('bing')
#             searx_host_list = ['http://114.213.232.140:18081/']#, 'https://searx.bndkt.io/'
#
#     search_url_list = [searx_host + 'search' for searx_host in searx_host_list]
#
#     # 统一处理所有引擎（包括elasticsearch）
#     if format_type.lower() == 'html':
#         accept_header = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
#     else:
#         accept_header = 'application/json'
#
#     engines_str = ','.join(engines)
#
#     # 使用多个User-Agent轮换
#     user_agents = [
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#         'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
#         'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
#     ]
#
#     headers = {
#         'User-Agent': user_agents[0],
#         'Accept': accept_header,
#         'Accept-Language': 'en-US,en;q=0.5',
#         'DNT': '1',
#         'Connection': 'keep-alive',
#     }
#
#     max_attempts = 3  # 增加最大尝试次数
#     timeout_config = 3  # 增加超时时间
#
#     # 计算需要获取的页数
#     results_per_page = 20
#     pages_to_fetch = max(1, (num_pages_to_crawl + results_per_page - 1) // results_per_page)
#
#     logger.info(f"需要获取 {pages_to_fetch} 页来满足 {num_pages_to_crawl} 个结果的需求")
#
#     # 添加页面状态跟踪
#     page_status = {}  # 记录每页的获取状态
#     consecutive_empty_pages = 0  # 连续空页计数器
#     max_consecutive_empty_pages = 2  # 最大允许连续空页数
#
#     for page in range(pages_to_fetch):
#         current_page = page + 1
#         logger.info(f"=== 开始获取第 {current_page}/{pages_to_fetch} 页 ===")
#
#         params = {
#             'q': formatted_query,
#             'format': format_type,
#             'engines': engines_str,
#             'categories': 'general',
#             'language': 'auto',
#             'pageno': current_page
#         }
#
#         page_success = False
#         page_results_count = 0
#         page_error = None
#
#         # 对每个页面尝试多个SearxNG实例
#         for searx_instance_idx, search_url in enumerate(search_url_list):
#             if page_success:
#                 break
#
#             for attempt in range(max_attempts):
#                 try:
#                     # 每次尝试使用不同的User-Agent
#                     headers['User-Agent'] = user_agents[attempt % len(user_agents)]
#
#                     logger.debug(
#                         f"第{current_page}页 尝试{attempt + 1}/{max_attempts} 使用实例 {searx_instance_idx + 1}/{len(search_url_list)}: {search_url}")
#
#                     # 发送HTTP请求
#                     response = requests.get(
#                         search_url,
#                         params=params,
#                         headers=headers,
#                         timeout=timeout_config,
#                         verify=False,
#                     )
#
#                     # 检查响应状态
#                     if response.status_code == 200:
#                         logger.info(f"第{current_page}页 HTTP请求成功，状态码: 200")
#
#                         if format_type.lower() == 'html':
#                             html_results = parse_searx_html(response.text, num_pages_to_crawl)
#                             page_results_count = len(html_results) if html_results else 0
#
#                             if html_results and page_results_count > 0:
#                                 logger.info(f"第{current_page}页HTML解析成功，获得 {page_results_count} 个结果")
#
#                                 # 处理HTML结果
#                                 processed_count = 0
#                                 for i, item in enumerate(html_results):
#                                     url = item.get('url', '')
#                                     title = item.get('title', '未知标题')
#                                     content = item.get('content', '')
#
#                                     # 文件类型过滤
#                                     if file_type and not url.lower().endswith(f".{file_type.lower()}"):
#                                         continue
#
#                                     if not content.strip():
#                                         content = f"来自 {title} 的搜索结果"
#
#                                     title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
#                                     if is_meaningless_filename(title_without_ext):
#                                         file_name = generate_filename_from_content(content, file_type)
#                                     else:
#                                         safe_title = clean_filename(title_without_ext)
#                                         file_name = f"{safe_title}.{file_type.lower()}" if file_type else f"{safe_title}.html"
#
#                                     searx_result_list.append({
#                                         "url": url,
#                                         "title": title,
#                                         "file_name": file_name,
#                                         "index": len(searx_result_list),
#                                         "content": content.strip(),
#                                         "file_type": file_type if file_type else "html",
#                                         "page": current_page  # 添加页码信息
#                                     })
#                                     processed_count += 1
#
#                                 logger.info(f"第{current_page}页成功处理 {processed_count}/{page_results_count} 个结果")
#                                 page_success = True
#
#                             else:
#                                 logger.warning(f"第{current_page}页HTML解析未找到结果或结果为空")
#                                 page_error = "HTML解析无结果"
#                                 consecutive_empty_pages += 1
#
#                         else:
#                             # JSON解析
#                             try:
#                                 data = response.json()
#                                 page_results_count = len(data.get('results', []))
#
#                                 if 'results' in data and page_results_count > 0:
#                                     logger.info(f"第{current_page}页JSON解析成功，获得 {page_results_count} 个原始结果")
#
#                                     # 解析搜索结果
#                                     parsed_results = parse_searx_results(data['results'], num_pages_to_crawl, file_type)
#                                     processed_count = len(parsed_results)
#
#                                     for result in parsed_results:
#                                         result['index'] = len(searx_result_list)
#                                         result['page'] = current_page  # 添加页码信息
#                                         searx_result_list.append(result)
#
#                                     logger.info(
#                                         f"第{current_page}页成功处理 {processed_count}/{page_results_count} 个结果")
#                                     page_success = True
#                                     consecutive_empty_pages = 0  # 重置连续空页计数
#
#                             except ValueError as json_error:
#                                 logger.error(f"第{current_page}页JSON解析失败: {json_error}")
#                                 page_error = f"JSON解析错误: {json_error}"
#                                 # 显示响应内容前200字符用于调试
#                                 logger.debug(f"响应内容预览: {response.text[:200]}...")
#
#                     elif response.status_code == 403:
#                         logger.warning(
#                             f"第{current_page}页 实例 {searx_instance_idx + 1} 返回403禁止访问，尝试下一个实例")
#                         page_error = f"HTTP错误: 403禁止访问"
#                         # 跳过当前实例，尝试下一个
#                         break
#
#                     else:
#                         logger.error(f"第{current_page}页HTTP请求失败，状态码: {response.status_code}")
#                         page_error = f"HTTP错误: {response.status_code}"
#                         logger.debug(f"错误响应: {response.text[:200]}...")
#
#                     # 如果本次尝试成功，跳出重试循环
#                     if page_success:
#                         break
#
#                 except requests.exceptions.Timeout:
#                     logger.error(f"第{current_page}页 尝试{attempt + 1} 请求超时")
#                     page_error = "请求超时"
#                     if attempt < max_attempts - 1:
#                         wait_time = (attempt + 1) * 1  # 指数退避策略 改成2
#                         logger.info(f"等待{wait_time}秒后重试...")
#                         time.sleep(wait_time)
#
#                 except requests.exceptions.ConnectionError:
#                     logger.error(f"第{current_page}页 尝试{attempt + 1} 连接错误")
#                     page_error = "连接错误"
#                     if attempt < max_attempts - 1:
#                         wait_time = (attempt + 1) * 1  # 指数退避策略 改成3
#                         logger.info(f"等待{wait_time}秒后重试...")
#                         time.sleep(wait_time)
#
#                 except requests.exceptions.RequestException as e:
#                     logger.error(f"第{current_page}页 尝试{attempt + 1} 网络请求异常: {str(e)}")
#                     page_error = f"网络异常: {str(e)}"
#                     if attempt < max_attempts - 1:
#                         wait_time = (attempt + 1) * 1  # 指数退避策略
#                         logger.info(f"等待{wait_time}秒后重试...")
#                         time.sleep(wait_time)
#
#                 except Exception as e:
#                     logger.error(f"第{current_page}页 尝试{attempt + 1} 未知异常: {str(e)}")
#                     page_error = f"未知异常: {str(e)}"
#                     if attempt < max_attempts - 1:
#                         logger.info(f"等待1秒后重试...")
#                         time.sleep(1)
#
#         # 记录页面状态
#         page_status[current_page] = {
#             'success': page_success,
#             'results_count': page_results_count,
#             'error': page_error,
#             'total_so_far': len(searx_result_list)
#         }
#
#         logger.info(f"第{current_page}页状态: {'成功' if page_success else '失败'}, "
#                     f"本页结果: {page_results_count}, 累计结果: {len(searx_result_list)}")
#
#         if page_error:
#             logger.warning(f"第{current_page}页错误详情: {page_error}")
#
#         # 检查停止条件
#         if consecutive_empty_pages >= max_consecutive_empty_pages:
#             logger.warning(f"连续 {consecutive_empty_pages} 页无结果，停止分页")
#             break
#
#         if len(searx_result_list) >= num_pages_to_crawl:
#             logger.info(f"已收集足够结果 ({len(searx_result_list)}/{num_pages_to_crawl})，停止分页")
#             break
#
#         # 页面间延迟 - 增加随机性避免被检测为机器人
#         delay = random.uniform(1, 3)#####1.5   3
#         logger.info(f"等待{delay:.2f}秒后获取下一页...")
#         time.sleep(delay)
#
#     # 生成详细的页面状态报告
#     logger.info("=== 分页搜索完成 ===")
#     logger.info(f"总共尝试获取 {len(page_status)} 页")
#
#     successful_pages = sum(1 for status in page_status.values() if status['success'])
#     failed_pages = len(page_status) - successful_pages
#
#     logger.info(f"成功页数: {successful_pages}, 失败页数: {failed_pages}")
#     logger.info(f"最终结果总数: {len(searx_result_list)}")
#
#     # 输出详细的页面状态
#     for page_num, status in page_status.items():
#         status_symbol = "✓" if status['success'] else "✗"
#         logger.info(f"第{page_num}页 {status_symbol} - 结果: {status['results_count']}, "
#                     f"累计: {status['total_so_far']}" +
#                     (f", 错误: {status['error']}" if status['error'] else ""))
#
#     # 最终结果处理
#     if searx_result_list:
#         actual_count = len(searx_result_list)
#         if actual_count > num_pages_to_crawl:
#             searx_result_list = searx_result_list[:num_pages_to_crawl]
#             logger.info(f"截断结果到 {num_pages_to_crawl} 个")
#
#         return searx_result_list
#     else:
#         logger.warning("所有页面搜索完成后未找到任何结果")
#         # 输出所有页面的错误信息用于调试
#         for page_num, status in page_status.items():
#             if status['error']:
#                 logger.debug(f"第{page_num}页错误: {status['error']}")
#         return []


#
#
# def fetch_search_results_with_searx(query: str,
#                                     num_pages_to_crawl: int = 100,
#                                     file_type: str = None,
#                                     offline_search: bool = False):
#     """
#     直接通过HTTP接口访问SearxNG获取搜索结果
#     支持文件类型过滤功能和动态格式选择
#
#     参数:
#     - query: 搜索关键词
#     - num_pages_to_crawl: 返回结果数量
#     - file_type: 文件类型过滤（如"pdf"、"doc"等，None表示不过滤）
#     - engines: 搜索引擎，如 "baidu,bing" 或 "elasticsearch" 或 "baidu,bing,elasticsearch"
#     """
#     # SearXNG服务器配置
#     search_url_list = []
#     searx_host_list = []
#     searx_result_list = []
#     accept_header = ''
#     engines = []
#     format_type = ''
#     # 构建搜索查询，如果指定了文件类型则添加filetype过滤
#     if file_type in ['doc', 'docx', 'ppt', 'pptx', 'txt', 'pdf']:
#         format_type = 'html'
#         formatted_query = f"{query} filetype:{file_type}"
#         logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         engines = ['google']
#         searx_host_list = ['https://searx.bndkt.io/', 'http://114.213.232.140:18081/']
#     elif file_type in ['pdf']:
#         format_type = 'html'
#         formatted_query = f"{query} filetype:{file_type}"
#         logger.info(f"使用文件类型过滤进行搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         engines = ['baidu', 'bing']
#         searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
#     else:
#         formatted_query = f"{query}"
#         logger.info(f"进行常规搜索: {formatted_query}, 预期结果: {num_pages_to_crawl}")
#         if offline_search:
#             format_type = 'json'
#             engines.append('elasticsearch')
#             searx_host_list = ['http://114.213.232.140:18081/']
#         else:
#             format_type = 'json'
#             engines.append('baidu')
#             engines.append('bing')
#             searx_host_list = ['http://114.213.232.140:18081/', 'https://searx.bndkt.io/']
#     search_url_list = [searx_host + 'search' for searx_host in searx_host_list]
#     # 统一处理所有引擎（包括elasticsearch）
#     if format_type.lower() == 'html':
#         accept_header = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
#     else:
#         accept_header = 'application/json'
#     engines_str = ','.join(engines)
#     params = {
#         'q': formatted_query,
#         'format': format_type,
#         'engines': engines_str,
#         'categories': 'general',
#         'language': 'auto',
#     }
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#         'Accept': accept_header,
#     }
#
#     max_attempts = 3
#     timeout_config = 10
#     for attempt in range(max_attempts):
#         try:
#             search_url = search_url_list[attempt % len(search_url_list)]
#
#             # 发送HTTP请求
#             response = requests.get(
#                 search_url,
#                 params=params,
#                 headers=headers,
#                 timeout=timeout_config,
#                 verify=False,  # 禁用SSL验证，适用于本地开发
#             )
#
#             # 检查响应状态
#             if response.status_code == 200:
#                 logger.info(f"成功获取SearxNG响应，状态码: {response.status_code}, 格式: {format_type}")
#
#                 if format_type.lower() == 'html':
#                     html_results = parse_searx_html(response.text, num_pages_to_crawl)
#
#                     if html_results:
#                         logger.info(f"HTML解析成功，获得 {len(html_results)} 个初步结果")
#
#                         # 转换为标准格式并进行文件类型过滤
#                         result_list = []
#                         for i, item in enumerate(html_results):
#                             url = item.get('url', '')
#                             title = item.get('title', '未知标题')
#                             content = item.get('content', '')
#
#                             # 如果指定了文件类型，进行过滤
#                             if file_type:
#                                 if not url.lower().endswith(f".{file_type.lower()}"):
#                                     logger.debug(f"跳过不匹配文件类型的URL: {url} (期望: {file_type})")
#                                     continue
#
#                             # 确保content不为空
#                             if not content.strip():
#                                 content = f"来自 {title} 的搜索结果"
#
#                             title_without_ext = title.rsplit('.', 1)[0] if '.' in title else title
#                             if is_meaningless_filename(title_without_ext):
#                                 logger.debug(f"检测到无意义文件名 '{title}'，使用content生成文件名")
#                                 file_name = generate_filename_from_content(content, file_type)
#                             else:
#                                 # 使用原始title作为文件名
#                                 safe_title = clean_filename(title_without_ext)
#                                 if file_type:
#                                     file_name = f"{safe_title}.{file_type.lower()}"
#                                 else:
#                                     file_name = f"{safe_title}.html"
#
#                             searx_result_list.append({
#                                 "url": url,
#                                 "title": title,
#                                 "file_name": file_name,
#                                 "index": i,
#                                 "content": content.strip(),
#                                 "file_type": file_type if file_type else "html"
#                             })
#
#                         if searx_result_list:
#                             actual_count = len(searx_result_list)
#                             logger.info(f"HTML解析成功，获得 {actual_count}/{num_pages_to_crawl} 个搜索结果" +
#                                         (f"（文件类型: {file_type}）" if file_type else ""))
#                             return searx_result_list
#                         else:
#                             logger.warning(f"文件类型过滤后结果为空: {formatted_query}" +
#                                            (f"（文件类型: {file_type}）" if file_type else ""))
#                     else:
#                         logger.warning(f"HTML解析未找到结果")
#                         # 显示部分HTML内容用于调试
#                         html_preview = response.text[:500].replace('\n', ' ')
#                         logger.debug(f"HTML预览: {html_preview}...")
#
#                 else:
#                     # JSON解析（默认）
#                     try:
#                         # 解析JSON响应
#                         data = response.json()
#
#                         # 检查是否有搜索结果
#                         if 'results' in data:
#                             search_results = data['results']
#                             logger.info(f"SearxNG原始返回结果数量: {len(search_results)}")
#
#                             # 分析原始结果的基本信息
#                             valid_count = 0
#                             invalid_count = 0
#                             for i, item in enumerate(search_results):
#                                 if isinstance(item, dict):
#                                     url = item.get('url') or item.get('link') or item.get('href')
#                                     if url and url.startswith("http"):
#                                         valid_count += 1
#                                     else:
#                                         invalid_count += 1
#                                         logger.debug(f"发现无效URL (索引{i}): {url}")
#
#                             logger.info(f"原始结果分析 - 有效URL: {valid_count}, 无效URL: {invalid_count}")
#
#                             # 解析搜索结果
#                             searx_result_list.extend(parse_searx_results(search_results, num_pages_to_crawl, file_type))
#
#                             if searx_result_list:
#                                 actual_count = len(searx_result_list)
#                                 logger.info(f"JSON解析成功，获得 {actual_count}/{num_pages_to_crawl} 个搜索结果" +
#                                             (f"（文件类型: {file_type}）" if file_type else ""))
#
#                                 return searx_result_list
#                             else:
#                                 logger.warning(f"搜索结果解析后为空: {formatted_query}")
#                         else:
#                             logger.warning(f"SearxNG响应中没有找到results字段")
#                             logger.debug(f"响应内容: {data}")
#
#                     except ValueError as json_error:
#                         logger.error(f"解析SearxNG响应JSON失败: {json_error}")
#                         logger.debug(f"响应内容: {response.text[:500]}...")
#
#             else:
#                 logger.error(f"SearxNG HTTP请求失败，状态码: {response.status_code}")
#                 logger.debug(f"响应内容: {response.text[:500]}...")
#
#             # 如果本次尝试失败，等待后重试
#             if attempt < max_attempts - 1:
#                 logger.info(f"等待 1 秒后重试...")
#
#         except requests.exceptions.RequestException as e:
#             logger.error(f"SearxNG HTTP请求异常 ({attempt + 1}/{max_attempts}): {str(e)}")
#             if attempt < max_attempts - 1:
#                 logger.info(f"等待 1 秒后重试...")
#                 time.sleep(1)
#         except Exception as e:
#             logger.error(f"SearxNG搜索过程异常 ({attempt + 1}/{max_attempts}): {str(e)}")
#             if attempt < max_attempts - 1:
#                 logger.info(f"等待 1 秒后重试...")
#                 time.sleep(1)
#
#     logger.error(f"在 {max_attempts} 次尝试后仍无法获取搜索结果")
#     return []


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


# # 创建一个模拟请求类
# class MockRequest:
#     def __init__(self, params_dict):
#         self.GET = params_dict
# #
# # 设置请求参数
# params = {
#     "name": "吴信东",
#     "num_pages_to_crawl": "40",
# }
# #安徽大学
# # 创建请求对象并调用函数
# request = MockRequest(params)
# t0 = time.time()
# result = search_urls(request)
# t1 = time.time()
# print("搜索时间:", t1-t0)
# print("搜索状态:", result["status"])
# print("搜索结果:", result)
# unique_urls_count = len(result["data"]["unique_urls"])
# print(f"唯一URL数量: {unique_urls_count}")



