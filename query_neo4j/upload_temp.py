import os
import json
import time
import pymysql
import jieba
import jieba.analyse
from django.http import JsonResponse
import logging
from collections import Counter, defaultdict # 导入 Counter 和 defaultdict

# 导入 disambiguation.py 的核心处理函数和文本提取函数
try:
    from .disambiguation import process_file
    from .disambiguation import extract_text_with_markitdown_safe
except ImportError:
    # 这是一个备用方案，如果导入失败，将引发错误
    # 在实际部署中，您必须确保 Python 路径正确设置
    logging.error("严重错误：无法从 disambiguation.py 导入 process_file 或 extract_text_with_markitdown_safe")
    process_file = None
    extract_text_with_markitdown_safe = None

# 获取日志记录器
logger = logging.getLogger(__name__)

# --- 数据库连接 (参考 upload.py) ---
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
            logger.info("数据库连接成功！")
        except pymysql.MySQLError as e:
            logger.error(f"数据库连接失败：{e}")
            raise

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭！")

    def find_best_entity_by_frequency(self, words_list):
        """
        [新功能] 检查词语列表，统计每个 (word + '数字') 实体被匹配的频率
        并返回频率最高的 xiaoqi_name。
        匹配逻辑: 词语 "汪萌" 会去匹配数据库中 "汪萌0", "汪萌1", ... "汪萌9"
        """
        if not words_list or not self.connection:
            return None # 返回 None 如果没有实体被找到

        # 1. 去重，并过滤掉太短的词
        words_to_check = set(word for word in words_list if len(word) > 1)
        if not words_to_check:
            logger.info("过滤后没有需要检查的词语。")
            return None

        logger.info(f"开始检查 {len(words_to_check)} 个唯一词语...")

        # 2. 统计频率
        # 警告：此操作仍然会为每个词执行一次查询。
        entity_frequency = Counter()

        # [关键修改] 使用 REGEXP 'word[0-9]$' 来匹配 "word" + "一个数字"
        # The ^ ensures it starts with the word.
        # The $ ensures it ends with the single digit.
        query_template = "SELECT xiaoqi_name FROM xiaoqi_new WHERE xiaoqi_name REGEXP %s"

        try:
            with self.connection.cursor() as cursor:
                logger.info(f"即将开始执行 {len(words_to_check)} 次 REGEXP 查询...")

                word_count = 0
                for word in words_to_check:
                    # [关键修改] 搜索模式: ^word[0-9]$
                    # ^ = 字符串开头, [0-9] = 任意一个数字, $ = 字符串结尾
                    search_pattern = f"^{word}[0-9]$"

                    # 为每个词执行查询
                    cursor.execute(query_template, (search_pattern,))

                    # 这个词匹配到的所有 xiaoqi_name (e.g., '汪萌1', '汪萌2')，频率都+1
                    # [关键修改] 这里的逻辑是：'汪萌'这个词命中了'汪萌1'，'汪萌1'的计数+1
                    for row in cursor.fetchall():
                        name = row[0]
                        entity_frequency[name] += 1

                    word_count += 1
                    if word_count % 100 == 0:
                        logger.info(f"已处理 {word_count}/{len(words_to_check)} 个词...")

                logger.info("所有词语的频率统计完成。")

            if not entity_frequency:
                logger.info("未找到任何匹配的实体。")
                return None

            # 4. 找到频率最高的
            # most_common(1) 返回 [(best_entity, freq)]
            best_entity, max_freq = entity_frequency.most_common(1)[0]

            logger.info(f"匹配完成。频率最高的实体是: '{best_entity}' (频率: {max_freq})")

            return best_entity # 返回一个字符串

        except pymysql.MySQLError as e:
            logger.error(f"查询 xiaoqi_new 失败: {e}")
            return None # 返回 None

# --- 文本提取 (直接调用 disambiguation) ---

def get_text_from_file(file_path, file_name_original, doc_type):
    """
    根据文件类型提取文本 (调用 disambiguation.py)
    """
    logger.info(f"开始从 {file_path} (类型: {doc_type}) 提取文本...")

    if extract_text_with_markitdown_safe is None:
        logger.error("文本提取函数未加载！")
        return ""

    try:
        # 直接调用 disambiguation.py 中最健壮的提取函数
        doc = extract_text_with_markitdown_safe(
            file_path,
            file_name_original,
            doc_type
        )

        # 检查是否只返回了文件名（即提取失败）
        if doc == file_name_original:
            logger.warning(f"文本提取失败，返回内容为文件名: {file_name_original}")
        else:
            logger.info(f"文本提取完成，内容长度: {len(doc)}")

        return doc

    except Exception as e:
        logger.error(f"文件文本提取失败: {e}")
        # 提取失败时，使用文件名作为备用
        return file_name_original

# --- 主视图函数 ---

def upload_and_process(request):
    """
    [已重构]
    1. 接收所有文件，循环处理（保存、提取、匹配实体）。
    2. 将文件按匹配到的实体进行分组。
    3. 循环分派任务，为每个实体调用一次 process_file，并传入其对应的所有文件。
    """
    if process_file is None or extract_text_with_markitdown_safe is None:
        return JsonResponse({"status": "error", "message": "服务器配置错误：无法加载 disambiguation 模块"}, status=500)

    if request.method != 'POST':
        return JsonResponse({"status": "error", "message": "请使用 POST 方法上传文件"}, status=405)

    try:
        # 1. 获取文件列表和参数 (支持多文件)
        file_objs = request.FILES.getlist('file')
        if not file_objs:
            return JsonResponse({"status": "error", "message": "未提供文件"}, status=400)

        userid = request.POST.get("userid")
        if not userid:
            return JsonResponse({"status": "error", "message": "缺少必要参数 'userid'"}, status=400)

        try:
            userid = int(userid)
        except ValueError:
            return JsonResponse({"status": "error", "message": "userid 必须是整数"}, status=400)

        private = int(request.POST.get('private', 1))

        # [关键修改]
        # all_files_results 重命名为 file_processing_errors，用于存储预处理失败的文件
        file_processing_errors = []
        # entity_to_files_map 用于按实体聚合文件
        entity_to_files_map = defaultdict(list)

        db = None
        try:
            # [关键修改] 数据库连接一次
            db = MySQLDatabase(
                host="114.213.234.179",
                user="koroot",
                password="DMiC-4092",
                database="db_hp"
            )
            db.connect()

            # --- 循环 1：文件处理与聚合 ---
            for file_obj in file_objs:

                file_name_original = file_obj.name
                logger.info(f"--- 正在处理文件: {file_name_original} ---")

                # 2. 保存临时文件
                head_path = 'D:/upload/'
                if not os.path.exists(head_path):
                    os.makedirs(head_path)

                file_path = os.path.join(head_path, file_name_original)

                logger.info(f"正在保存临时文件到: {file_path}")
                try:
                    with open(file_path, 'wb+') as f:
                        for chunk in file_obj.chunks():
                            f.write(chunk)
                except IOError as e:
                    logger.error(f"文件保存失败: {file_name_original}, 错误: {e}")
                    file_processing_errors.append({
                        "file_name": file_name_original,
                        "status": "error",
                        "message": f"文件保存失败: {e}"
                    })
                    continue # 继续处理下一个文件

                doc_type = file_name_original.split('.')[-1].lower()

                # 3. 提取文本
                doc = get_text_from_file(file_path, file_name_original, doc_type)
                if not doc.strip() or doc == file_name_original:
                    logger.warning(f"无法从文件提取有效文本: {file_name_original}")
                    file_processing_errors.append({
                        "file_name": file_name_original,
                        "status": "error",
                        "message": "无法从文件中提取到有效文本"
                    })
                    continue

                    # 4. Jieba 分词和去停用词
                stop_words_path = r"./query_neo4j/data/hit_stopwords.txt"
                if os.path.exists(stop_words_path):
                    jieba.analyse.set_stop_words(stop_words_path)
                    stop_words = set()
                    with open(stop_words_path, "r", encoding="utf-8") as f:
                        stop_words.update(line.strip() for line in f)
                else:
                    logger.warning(f"未找到停用词表: {stop_words_path}，将不进行停用词过滤。")
                    stop_words = set()

                words = jieba.lcut(doc)
                filtered_words = [
                    word for word in words
                    if word not in stop_words and word.strip() and len(word) > 1
                ]

                if not filtered_words:
                    logger.warning(f"文本分词过滤后为空: {file_name_original}")
                    file_processing_errors.append({
                        "file_name": file_name_original,
                        "status": "error",
                        "message": "文本分词并过滤后为空，无法匹配实体"
                    })
                    continue

                    # 5. 匹配 xiaoqi_new 表，查找频率最高的实体
                entity_name = None

                # (数据库连接已在循环外建立)
                entity_name = db.find_best_entity_by_frequency(filtered_words)

                if not entity_name:
                    logger.info(f"未找到匹配实体: {file_name_original}")
                    file_processing_errors.append({
                        "file_name": file_name_original,
                        "status": "error",
                        "message": "未在文件中找到任何匹配 xiaoqi_new 表的实体"
                    })
                    continue

                logger.info(f"文件 {file_name_original} 匹配到实体: {entity_name}")

                # [关键修改] 聚合文件，而不是立即分派
                entity_to_files_map[entity_name].append({
                    "name": file_name_original,
                    "path": file_path
                })

        except Exception as e:
            logger.error(f"文件处理循环中发生数据库或其他错误: {e}")
            return JsonResponse({"status": "error", "message": f"文件处理循环失败: {e}"}, status=500)
        finally:
            if db:
                db.close() # 在循环结束后关闭数据库

        # --- 循环 2：任务分派 ---
        logger.info(f"文件处理和聚合完成。聚合到 {len(entity_to_files_map)} 个唯一实体。")
        dispatch_results = []

        if not entity_to_files_map:
            logger.warning("没有成功匹配到任何实体，任务分派结束。")
            # 仍然返回成功，但结果为空

        for entity_name, file_info_list in entity_to_files_map.items():

            # 准备 process_file 所需的参数
            file_name_original_list = [f['name'] for f in file_info_list]
            file_path_list = [f['path'] for f in file_info_list]

            logger.info(f"--- 正在为实体 '{entity_name}' 批量分派 {len(file_name_original_list)} 个文件 ---")

            try:
                result = process_file(
                    file_name_original_list=file_name_original_list,
                    file_path_list=file_path_list,
                    name=entity_name, # 传入聚合后的实体
                    userid=userid,
                    private=private,
                    url_list=None,
                    only_name=False
                )
                logger.info(f"--- 实体 '{entity_name}' 批量处理完成 ---")
                dispatch_results.append({
                    "entity": entity_name,
                    "files_processed": file_name_original_list,
                    "dispatch_status": "success",
                    "result": result
                })

            except Exception as e:
                logger.error(f"--- 实体 '{entity_name}' 批量处理失败: {e} ---")
                dispatch_results.append({
                    "entity": entity_name,
                    "files_failed": file_name_original_list,
                    "dispatch_status": "error",
                    "message": str(e)
                })

        # 7. 返回所有文件的总结果
        return JsonResponse({
            "status": "success",
            "message": f"批量处理完成。共收到 {len(file_objs)} 个文件。成功分派 {len(dispatch_results)} 个实体任务。",
            "files_summary": {
                "tasks_dispatched": dispatch_results,
                "file_processing_errors": file_processing_errors
            }
        })

    except Exception as e:
        logger.error(f"upload_and_process 发生未知错误: {e}", exc_info=True)
        return JsonResponse({"status": "error", "message": f"服务器内部错误: {e}"}, status=500)

