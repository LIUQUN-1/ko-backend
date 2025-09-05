from bs4 import BeautifulSoup
import os
import time
import random
import threading
import json
import logging
from query_neo4j.disambiguation import process_file
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
from crawl4ai import AsyncWebCrawler
from urllib.parse import urlparse, quote, urlunparse
import redis
import hashlib

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')

# 保存目录
SAVE_DIR = "D:/upload/"


class CacheManager:
    """Redis缓存管理器"""

    def __init__(self, host='114.213.232.140', port=26379, db=0, expire_time=3600):
        """
        初始化缓存管理器

        参数:
        - host: Redis服务器地址
        - port: Redis端口
        - db: Redis数据库编号
        - expire_time: 缓存过期时间（秒），默认1小时
        """
        self.expire_time = expire_time
        self.redis_client = None
        self.is_connected = False

        try:
            # 尝试连接Redis
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            # 测试连接
            self.redis_client.ping()
            self.is_connected = True
            logger.info(f"✅ Redis连接成功 {host}:{port}")
        except Exception as e:
            logger.warning(f"⚠️ Redis连接失败，将不使用缓存功能: {str(e)}")
            self.redis_client = None
            self.is_connected = False

    def _generate_cache_key(self, url):
        """生成缓存键"""
        key_string = f"crawl_cache:{url}"
        # 使用MD5哈希避免键名过长
        return hashlib.md5(key_string.encode()).hexdigest()

    def check_cache_and_file(self, url):
        """检查Redis缓存和本地文件是否存在"""
        if not self.is_connected:
            return None

        try:
            cache_key = self._generate_cache_key(url)
            cached_data = self.redis_client.get(cache_key)

            if not cached_data:
                logger.info(f"URL未在缓存中找到: {url}")
                return None

            # 解析缓存数据
            cache_info = json.loads(cached_data)
            file_path = cache_info.get('file_path')

            # 检查本地文件是否存在
            if file_path and os.path.exists(file_path):
                logger.info(f"✅ 从缓存中找到URL: {url}, 本地文件存在: {file_path}")
                return {
                    'url': cache_info.get('url'),
                    'file_path': cache_info.get('file_path'),
                    'file_name': cache_info.get('file_name'),
                    'file_type': cache_info.get('file_type'),
                    'cached_time': cache_info.get('cached_time'),
                    'from_cache': True
                }
            else:
                logger.warning(f"⚠️ 缓存中找到URL但本地文件不存在，删除缓存: {url}")
                # 本地文件不存在，删除缓存
                self.redis_client.delete(cache_key)
                return None

        except Exception as e:
            logger.error(f"检查缓存时出错: {str(e)}")
            return None

    def save_to_cache(self, url, file_path, file_name, file_type):
        """保存到Redis缓存"""
        if not self.is_connected:
            return False

        try:
            cache_key = self._generate_cache_key(url)
            cache_data = {
                'url': url,
                'file_path': file_path,
                'file_name': file_name,
                'file_type': file_type,
                'cached_time': int(time.time())
            }

            # 保存到Redis，设置过期时间
            self.redis_client.setex(cache_key, self.expire_time, json.dumps(cache_data))
            logger.info(f"✅ 已缓存URL: {url}, 文件: {file_name}")
            return True

        except Exception as e:
            logger.error(f"保存缓存时出错: {str(e)}")
            return False

    def clear_cache(self, url=None):
        """清理缓存"""
        if not self.is_connected:
            return False

        try:
            if url:
                # 删除特定URL的缓存
                cache_key = self._generate_cache_key(url)
                self.redis_client.delete(cache_key)
                logger.info(f"已清理URL缓存: {url}")
            else:
                # 清理所有爬虫缓存
                keys = self.redis_client.keys("crawl_cache:*")
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"已清理所有爬虫缓存，共 {len(keys)} 个")
            return True
        except Exception as e:
            logger.error(f"清理缓存时出错: {str(e)}")
            return False

    def get_cache_stats(self):
        """获取缓存统计信息"""
        if not self.is_connected:
            return {
                'connected': False,
                'total_keys': 0,
                'crawl_cache_keys': 0
            }

        try:
            crawl_keys = self.redis_client.keys("crawl_cache:*")
            return {
                'connected': True,
                'total_keys': self.redis_client.dbsize(),
                'crawl_cache_keys': len(crawl_keys),
                'expire_time': self.expire_time
            }
        except Exception as e:
            logger.error(f"获取缓存统计时出错: {str(e)}")
            return {'connected': False, 'error': str(e)}


# 初始化全局缓存管理器
cache_manager = CacheManager()


# PROXIES = {
#     "http": "http://127.0.0.1:7897",
#     "https": "http://127.0.0.1:7897",
# }

# # 同时设置环境变量
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:7897"
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7897"
def get_file_type_from_url(url):
    """
    从URL中检测文件类型

    参数:
    - url: 要检测的URL

    返回:
    - str: 文件类型 ('pdf', 'docx', 'doc', 'ppt', 'pptx', 'txt', 'html' 等)
    """
    url_lower = url.lower()
    if url_lower.endswith('.pdf'):
        return 'pdf'
    elif url_lower.endswith('.docx'):
        return 'docx'
    elif url_lower.endswith('.doc'):
        return 'doc'
    elif url_lower.endswith('.pptx'):
        return 'pptx'
    elif url_lower.endswith('.ppt'):
        return 'ppt'
    elif url_lower.endswith('.txt'):
        return 'txt'
    else:
        return 'html'  # 默认为HTML页面


def download_file(url, file_path, file_type='pdf'):
    """
    下载文件到本地

    参数:
    - url: 文件的URL
    - file_path: 本地保存路径
    - file_type: 文件类型

    返回:
    - bool: 下载是否成功
    """
    try:
        # 根据文件类型设置不同的请求头
        if file_type == 'pdf':
            accept_header = 'application/pdf,application/octet-stream,*/*'
        elif file_type in ['doc', 'docx']:
            accept_header = 'application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/octet-stream,*/*'
        elif file_type in ['ppt', 'pptx']:
            accept_header = 'application/vnd.ms-powerpoint,application/vnd.openxmlformats-officedocument.presentationml.presentation,application/octet-stream,*/*'
        elif file_type == 'txt':
            accept_header = 'text/plain,text/*,application/octet-stream,*/*'
        else:
            accept_header = '*/*'

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': accept_header,
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

        logger.info(f"开始下载{file_type.upper()}文件: {url}")

        # response = requests.get(url, headers=headers, proxies=PROXIES, verify=False, stream=True)
        response = requests.get(url, headers=headers, verify=False, stream=True)
        if response.status_code == 200:
            # 检查Content-Type
            content_type = response.headers.get('Content-Type', '').lower()
            logger.info(f"文件Content-Type: {content_type}")

            # 验证文件类型
            expected_types = {
                'pdf': ['application/pdf'],
                'docx': ['application/vnd.openxmlformats-officedocument.wordprocessingml.document'],
                'doc': ['application/msword'],
                'pptx': ['application/vnd.openxmlformats-officedocument.presentationml.presentation'],
                'ppt': ['application/vnd.ms-powerpoint'],
                'txt': ['text/plain', 'text/html', 'text/']
            }

            if file_type in expected_types:
                valid_type = any(expected in content_type for expected in expected_types[file_type])
                if not valid_type and not url.lower().endswith(f'.{file_type}'):
                    logger.warning(f"URL可能不是{file_type.upper()}文件: {url}, Content-Type: {content_type}")

            # 确保保存目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # 验证文件大小
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"下载的{file_type.upper()}文件为空: {file_path}")
                return False

            logger.info(f"{file_type.upper()}文件下载成功: {file_path}, 大小: {file_size} bytes")
            return True
        else:
            logger.error(f"{file_type.upper()}下载失败，状态码: {response.status_code}, URL: {url}")
            return False

    except requests.exceptions.ProxyError as e:
        logger.error(f"代理连接失败，下载{file_type.upper()}文件: {url}, 错误: {str(e)}")
        logger.warning("建议检查代理服务器是否正常运行")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"网络连接失败，下载{file_type.upper()}文件: {url}, 错误: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"下载{file_type.upper()}文件时出错: {url}, 错误: {str(e)}")
        return False


# process_document_file 函数已移除，改为批量处理模式
def ensure_save_dir():
    """确保保存目录存在"""
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)


def clean_filename(title):
    """清理文件名中的非法字符"""
    return "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in title)


def extract_title_from_html(html_content):
    """从HTML内容中提取title作为文件名"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # 优先使用title标签
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            title = title_tag.string.strip()
            if title and len(title) > 0:
                # 清理标题，限制长度
                title = title[:100]  # 限制标题长度
                filename = clean_filename(title)
                if filename and len(filename.strip()) > 0:
                    logger.info(f"从HTML中提取到页面标题: {filename}")
                    return f"{filename}.html"

        # 如果没有有效的title，使用时间戳
        timestamp = int(time.time())
        fallback_name = f"webpage_{timestamp}.html"
        logger.warning(f"未找到有效的页面标题，使用fallback文件名: {fallback_name}")
        return fallback_name

    except Exception as e:
        logger.error(f"从HTML提取标题时发生异常: {str(e)}")
        # 最后的fallback：使用时间戳
        timestamp = int(time.time())
        return f"webpage_{timestamp}.html"


async def fetch_html_with_crawl4ai(url):
    """使用Crawl4AI获取页面内容"""
    try:
        from crawl4ai import AsyncWebCrawler

        logger.info(f"🕷️ 使用Crawl4AI爬取页面: {url}")

        async with AsyncWebCrawler(verbose=False) as crawler:
            # 配置爬取参数
            result = await crawler.arun(
                url=url,
                # 移除不必要的元素
                exclude_tags=['script', 'style', 'nav', 'footer', 'header'],
                # 等待页面加载
                wait_for="body",
                # 提取文本内容
                word_count_threshold=10
            )

            if result.success:
                # 优先使用Crawl4AI提取的Markdown内容
                content = result.markdown
                title = result.metadata.get('title', '')

                # 如果Markdown内容为空，使用原始HTML
                if not content or len(content.strip()) < 50:
                    content = result.html
                    logger.warning(f"Markdown内容过少，使用原始HTML: {url}")

                logger.info(f"✅ Crawl4AI爬取成功: {url}")
                logger.info(f"   📄 标题: {title}")
                logger.info(f"   📊 内容长度: {len(content)} 字符")
                logger.info(
                    f"   🔗 链接数量: {len(result.links.get('internal', []) + result.links.get('external', []))}")

                return {
                    'success': True,
                    'html': result.html,
                    'title': title,
                    'metadata': result.metadata,
                    'links': result.links,
                    'media': result.media
                }
            else:
                logger.error(f"❌ Crawl4AI爬取失败: {url}, 错误: {result.error_message}")
                return {
                    'success': False,
                    'error': result.error_message
                }

    except ImportError:
        logger.warning("⚠️ Crawl4AI未安装，将使用备选方案")
        return {
            'success': False,
            'error': 'Crawl4AI not installed'
        }
    except Exception as e:
        logger.error(f"❌ Crawl4AI爬取异常: {url}, 错误: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def run_async_function_in_thread(async_func, *args):
    """在新线程中运行异步函数"""
    try:
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(async_func(*args))
        except Exception as e:
            return None
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"异步函数执行失败: {str(e)}")
        return None


def save_file(filename, content):
    """保存文件内容的辅助函数"""
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)


# process_urls_batch 函数已移除，现在直接在多线程中调用 process_single_url

def process_single_url(url, file_name, index, name):
    """处理单个URL，支持HTML页面、PDF文件、DOC/DOCX文件、PPT/PPTX文件、TXT文件的爬取和处理"""
    try:
        logger.info(f"正在处理第 {index + 1} 个URL: {url}")

        # 检查缓存
        cached_result = cache_manager.check_cache_and_file(url)
        if cached_result:
            return cached_result

        # 检测文件类型
        file_type = get_file_type_from_url(url)
        logger.info(f"检测到文件类型: {file_type.upper()}")

        # 根据文件类型进行不同的处理
        if file_type in ['pdf', 'doc', 'docx', 'ppt', 'pptx', 'txt']:
            # 处理文档文件
            logger.info(f"开始处理{file_type.upper()}文件: {url}")

            # 只有在没有提供file_name的情况下才进行文件名解析
            if not file_name or file_name.strip() == "":
                logger.info(f"📝 未提供文件名，为{file_type.upper()}文件生成文件名: {url}")
                try:
                    from urllib.parse import urlparse
                    parsed_url = urlparse(url)
                    path = parsed_url.path

                    # 尝试从URL路径中提取文件名
                    if path and path != '/':
                        url_filename = path.split('/')[-1]
                        if url_filename and '.' in url_filename:
                            # 如果URL中有文件名，使用它
                            base_name = url_filename.rsplit('.', 1)[0]
                            file_name = f"{clean_filename(base_name)}.{file_type}"
                        else:
                            # 如果URL中没有明确的文件名，使用URL的最后一段
                            base_name = clean_filename(url_filename) if url_filename else "document"
                            file_name = f"{base_name}.{file_type}"
                    else:
                        # 如果无法从URL提取，使用域名和时间戳
                        domain = parsed_url.netloc.replace('www.', '').replace('.', '_')
                        timestamp = int(time.time())
                        file_name = f"{clean_filename(domain)}_{timestamp}.{file_type}"

                    logger.info(f"✅ 为{file_type.upper()}文件生成的文件名: {file_name}")

                except Exception as e:
                    logger.error(f"生成{file_type.upper()}文件名失败: {str(e)}")
                    # fallback文件名
                    timestamp = int(time.time())
                    file_name = f"document_{timestamp}.{file_type}"
            else:
                logger.info(f"📄 使用提供的文件名: {file_name}")

            # 调整文件名扩展名（确保file_name不为None）
            if file_name and not file_name.endswith(f'.{file_type}'):
                file_name = file_name.replace('.html', f'.{file_type}')
                if not file_name.endswith(f'.{file_type}'):
                    file_name = f"{file_name}.{file_type}"

            document_file_path = os.path.join(SAVE_DIR, file_name)

            # 确保保存目录存在
            ensure_save_dir()

            # 下载文档文件
            download_success = download_file(url, document_file_path, file_type)
            if not download_success:
                return {
                    "status": "error",
                    "message": f"{file_type.upper()}文件下载失败",
                    "url": url,
                    "file_type": file_type
                }
            else:
                # 保存到缓存
                cache_manager.save_to_cache(url, document_file_path, file_name, file_type)
                return {
                    "status": "success",
                    "message": f"{file_type.upper()}文件下载成功",
                    "url": url,
                    "file_path": document_file_path,
                    "file_name": file_name,
                    "file_type": file_type
                }

        else:
            # 处理HTML页面（使用Crawl4AI）
            logger.info(f"处理HTML页面: {url}")

            # 使用Crawl4AI爬取页面
            crawl4ai_result = run_async_function_in_thread(fetch_html_with_crawl4ai, url)

            if not crawl4ai_result or not crawl4ai_result.get('success'):
                error_msg = crawl4ai_result.get('error', '未知错误') if crawl4ai_result else '爬取失败'
                logger.error(f"Crawl4AI获取HTML内容失败: {url}, 错误: {error_msg}")

                # 直接用 requests 兜底
                import requests
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7'
                }
                try:
                    response = requests.get(url, headers=headers, timeout=15, verify=False)
                    html_content = response.text if response.status_code == 200 else None
                except Exception as e:
                    logger.error(f"requests兜底失败: {url}, 错误: {str(e)}")
                    html_content = None
                if html_content and len(html_content) > 100:
                    # 自动生成文件名
                    if not file_name or file_name.strip() == "":
                        file_name = extract_title_from_html(html_content)
                    file_path = os.path.join(SAVE_DIR, file_name)
                    ensure_save_dir()
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    logger.info(f"✅ 已用requests兜底保存HTML文件: {file_path}")
                    # 保存到缓存
                    cache_manager.save_to_cache(url, file_path, file_name, "html")
                    return {
                        "status": "success",
                        "message": "requests兜底爬取成功",
                        "url": url,
                        "file_path": file_path,
                        "file_name": file_name,
                        "file_type": "html"
                    }
                else:
                    return {"status": "error", "message": f"获取页面失败: {error_msg}", "url": url, "file_type": "html"}

            try:
                # 从Crawl4AI结果中获取内容
                html_content = crawl4ai_result.get('html', '')
                page_title = crawl4ai_result.get('title', '')
                metadata = crawl4ai_result.get('metadata', {})

                # 只有在没有提供file_name的情况下才进行文件解析
                if not file_name or file_name.strip() == "":
                    logger.info(f"📝 未提供文件名，开始自动解析: {url}")
                    if page_title and page_title.strip():
                        # 使用Crawl4AI提取的标题
                        title = clean_filename(page_title.strip()[:100])
                        file_name = f"{title}.html"
                        logger.info(f"✅ 使用Crawl4AI提取的标题作为文件名: {file_name}")
                    else:
                        # 从HTML内容中提取title
                        logger.info(f"🔍 从页面内容中提取标题作为文件名: {url}")
                        file_name = extract_title_from_html(html_content)
                        logger.info(f"✅ 提取到的文件名: {file_name}")
                else:
                    logger.info(f"📄 使用提供的文件名: {file_name}")

                content_to_save = html_content
                logger.warning(f"使用原始HTML内容，长度: {len(content_to_save)} 字符")
                filename = os.path.join(SAVE_DIR, file_name)

                # 确保保存目录存在
                ensure_save_dir()

                # 保存文件（直接保存处理后的内容）
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content_to_save)

                logger.info(f"✅ 已保存HTML文件: {filename}")
                logger.info(f"   📄 页面标题: {page_title}")
                logger.info(f"   📊 内容长度: {len(content_to_save)} 字符")
                logger.info(
                    f"   🔗 提取链接数: {len(crawl4ai_result.get('links', {}).get('internal', []) + crawl4ai_result.get('links', {}).get('external', []))}")

                # 保存到缓存
                cache_manager.save_to_cache(url, filename, file_name, "html")

                # 只保存HTML文件，不立即处理
                return {
                    "status": "success",
                    "message": "Crawl4AI爬取成功",
                    "url": url,
                    "file_path": filename,
                    "file_name": file_name,
                    "file_type": "html",
                    "crawl4ai_metadata": {
                        "title": page_title,
                        "links_count": len(
                            crawl4ai_result.get('links', {}).get('internal', []) + crawl4ai_result.get('links', {}).get(
                                'external', [])),
                        "content_length": len(content_to_save),
                        "metadata": metadata
                    }
                }
            except Exception as e:
                logger.error(f"处理Crawl4AI结果时出错: {str(e)}")
                return {"status": "error", "message": f"处理内容出错: {str(e)}", "url": url, "file_type": "html"}

    except Exception as e:
        logger.error(f"处理URL时发生异常: {url}, 错误: {str(e)}")
        return {"status": "error", "message": f"处理异常: {str(e)}", "url": url}


def process_urls_multithreaded(url_list, name, max_workers=3):
    """
    多线程处理URL列表

    参数:
    - url_list: URL列表
    - name: 搜索关键词
    - max_workers: 最大线程数，默认3个

    返回:
    - 处理结果列表
    """
    if not url_list:
        return []

    # 根据URL数量调整线程数
    optimal_workers = min(max_workers, len(url_list), 10)  # 最多10个线程

    logger.info(f"开始多线程处理，URL总数: {len(url_list)}, 线程数: {optimal_workers}")

    all_results = []

    # 使用ThreadPoolExecutor进行多线程处理
    with ThreadPoolExecutor(max_workers=optimal_workers, thread_name_prefix='URLCrawler') as executor:
        # 为每个URL提交一个任务
        future_to_url = {}
        for item in url_list:
            url = item.get('url')
            file_name = item.get('file_name', None)
            index = item.get('index', 0)

            # 直接提交process_single_url任务
            future = executor.submit(process_single_url, url, file_name, index, name)
            future_to_url[future] = {
                'url': url,
                'index': index,
                'file_name': file_name
            }

        # 收集结果
        completed_count = 0
        total_count = len(url_list)
        for future in as_completed(future_to_url):
            url_info = future_to_url[future]
            try:
                result = future.result(timeout=300)  # 5分钟超时
                all_results.append(result)
                completed_count += 1

                # 记录进度
                if completed_count % 5 == 0 or completed_count == total_count:
                    logger.info(f"多线程处理进度: {completed_count}/{total_count}")

            except Exception as e:
                logger.error(f"处理URL失败: {url_info['url']}, 错误: {str(e)}")
                all_results.append({
                    "status": "error",
                    "message": f"处理超时或失败: {str(e)}",
                    "url": url_info['url'],
                    "index": url_info['index']
                })
                completed_count += 1

    logger.info(f"多线程处理完成，总结果数: {len(all_results)}")
    return all_results


def crawl_pages(request):
    """
    接口函数：接收URL列表爬取网页并调用disambiguation接口

    参数:
    - url_list: URL列表及相关信息（JSON格式的字符串或列表）
    - name: 搜索关键词
    - userid: 用户ID
    - only_name: 是否只使用名称进行分类（可选，默认False）
    - use_multithreading: 是否使用多线程（可选，默认True）
    - max_workers: 最大线程数（可选，默认2）
    - private:保存文件为公开还是私有 0?1（可选，默认1私有）

    返回:
    - 处理结果
    """
    try:
        # 从POST请求中获取参数
        if request.method == 'POST':
            try:
                data = json.loads(request.body)
                url_list = data.get('url_list')
                name = data.get('name')
                userid = data.get('userid')
                use_multithreading = data.get('use_multithreading', True)
                max_workers = data.get('max_workers', 2)
                only_name = data.get('only_name', False)
                private = data.get('private', 1)
            except json.JSONDecodeError:
                return {"status": "error", "message": "无效的JSON格式"}
        else:
            # 从GET请求中获取参数
            url_list_str = request.GET.get('url_list')
            try:
                url_list = json.loads(url_list_str) if url_list_str else None
            except json.JSONDecodeError:
                return {"status": "error", "message": "无效的URL列表JSON格式"}
            name = request.GET.get('name')
            userid = request.GET.get('userid')
            use_multithreading = request.GET.get('use_multithreading', 'true').lower() == 'true'
            max_workers = int(request.GET.get('max_workers', 3))
            only_name = request.GET.get('only_name', 'false').lower() == 'true'
        if not url_list or not isinstance(url_list, list):
            return {"status": "error", "message": "缺少URL列表或格式不正确"}

        if not name:
            return {"status": "error", "message": "缺少name参数"}

        if not userid:
            return {"status": "error", "message": "缺少userid参数"}

        # 验证和清理URL列表
        valid_urls = []
        invalid_items = []

        for i, item in enumerate(url_list):
            if not isinstance(item, dict):
                invalid_items.append({"index": i, "reason": "URL项格式不正确", "item": str(item)})
                continue

            url = item.get('url')
            file_name = item.get('file_name', None)

            # 只要有URL就可以，file_name可以为空（将自动从网页提取）
            if not url:
                invalid_items.append({"index": i, "reason": "URL项缺少url字段", "item": item})
                continue

            # 记录是否需要自动解析文件名
            if not file_name or file_name.strip() == "":
                logger.info(f"📝 URL项 {i} 没有file_name，将自动解析文件名")
            else:
                logger.info(f"📄 URL项 {i} 使用提供的file_name: {file_name}")

            # 设置默认索引
            if 'index' not in item:
                item['index'] = i

            valid_urls.append(item)

        logger.info(f"URL验证完成，有效: {len(valid_urls)}, 无效: {len(invalid_items)}")

        if not valid_urls:
            return {"status": "error", "message": "没有有效的URL项", "invalid_items": invalid_items}

        # 选择处理方式
        if use_multithreading and len(valid_urls) > 1:
            logger.info(f"使用多线程模式处理 {len(valid_urls)} 个URL，最大线程数: {max_workers}")
            results = process_urls_multithreaded(valid_urls, name, max_workers)
        else:
            logger.info(f"使用单线程模式处理 {len(valid_urls)} 个URL")
            results = []
            for item in valid_urls:
                url = item.get('url')
                file_name = item.get('file_name', None)
                index = item.get('index', 0)

                # 直接传递file_name（可能为None），让process_single_url函数处理文件名解析
                result = process_single_url(url, file_name, index, name)
                results.append(result)

        # 批量处理所有成功下载的文件（包括从缓存获取的文件）
        successful_downloads = [r for r in results if r.get('status') == 'success' or r.get('from_cache')]
        if successful_downloads and name and userid:
            cached_count = sum(1 for r in successful_downloads if r.get('from_cache'))
            new_count = len(successful_downloads) - cached_count
            logger.info(f"开始批量处理 {len(successful_downloads)} 个文件（新爬取: {new_count}, 缓存: {cached_count}）")

            # 收集所有文件信息
            file_name_list = []
            file_path_list = []
            url_list = []

            for download in successful_downloads:
                file_name_list.append(download.get('file_name'))
                file_path_list.append(download.get('file_path'))
                url_list.append(download.get('url'))

            try:
                # 调用 disambiguation 的 process_file 函数进行批量处理
                from query_neo4j.disambiguation import process_file
                batch_result = process_file(file_name_list, file_path_list, name, userid, private, url_list=url_list,
                                            only_name=only_name)

                if batch_result and batch_result.get('status') == 'success':
                    logger.info("批量处理成功")
                    # 更新结果状态
                    for i, result in enumerate(results):
                        if result.get('status') == 'success' or result.get('from_cache'):
                            if result.get('from_cache'):
                                result['message'] = f"从缓存获取{result.get('file_type', '').upper()}文件并处理成功"
                            else:
                                result['message'] = f"{result.get('file_type', '').upper()}文件下载并处理成功"
                            result['api_processed'] = True
                else:
                    logger.warning("批量处理失败或返回错误")
                    # 更新结果状态为部分成功
                    for i, result in enumerate(results):
                        if result.get('status') == 'success' or result.get('from_cache'):
                            result['status'] = 'partial'
                            if result.get('from_cache'):
                                result[
                                    'message'] = f"从缓存获取{result.get('file_type', '').upper()}文件成功但API处理失败"
                            else:
                                result['message'] = f"{result.get('file_type', '').upper()}文件下载成功但API处理失败"
                            result['api_processed'] = False

            except Exception as e:
                logger.error(f"批量处理异常: {str(e)}")
                # 更新结果状态为部分成功
                for i, result in enumerate(results):
                    if result.get('status') == 'success' or result.get('from_cache'):
                        result['status'] = 'partial'
                        if result.get('from_cache'):
                            result[
                                'message'] = f"从缓存获取{result.get('file_type', '').upper()}文件成功但API处理异常: {str(e)}"
                        else:
                            result[
                                'message'] = f"{result.get('file_type', '').upper()}文件下载成功但API处理异常: {str(e)}"
                        result['api_processed'] = False

        # 添加无效项到结果中
        for invalid_item in invalid_items:
            results.append({
                "status": "error",
                "message": invalid_item["reason"],
                "item": invalid_item["item"]
            })

        # 汇总结果
        success_count = sum(1 for r in results if r.get('status') == 'success')
        partial_count = sum(1 for r in results if r.get('status') == 'partial')
        error_count = sum(1 for r in results if r.get('status') == 'error')
        cached_count = sum(1 for r in results if r.get('from_cache'))
        new_crawled_count = success_count + partial_count - cached_count

        return {
            "status": "completed",
            "message": f"爬取完成。成功: {success_count}, 部分成功: {partial_count}, 失败: {error_count}",
            "processing_mode": "多线程" if use_multithreading and len(valid_urls) > 1 else "单线程",
            "total_urls": len(url_list),
            "valid_urls": len(valid_urls),
            "invalid_urls": len(invalid_items),
            "cache_stats": {
                "cache_hits": cached_count,
                "new_crawled": new_crawled_count,
                "cache_enabled": cache_manager.is_connected
            },
            "details": results
        }

    except Exception as e:
        logger.error(f"执行爬取任务时发生错误: {str(e)}")
        # 确保出错时也能关闭浏览器
        return {"status": "error", "message": f"执行过程中出错: {str(e)}"}