import json
import time
import uuid
import logging
import threading
from datetime import datetime
from typing import List, Dict, Optional
import redis
import pika

logger = logging.getLogger('query_neo4j')

class RabbitMQClientProducer:
    """基于RabbitMQ的消息生产者，替换RocketMQ"""

    def __init__(self,
                 host='114.213.232.140',
                 port=15672,
                 username='admin',
                 password='admin',
                 redis_host='114.213.232.140',
                 redis_port=26379,
                 redis_db=0):
        """
        初始化RabbitMQ客户端生产者

        参数:
        - host: RabbitMQ服务器地址
        - port: RabbitMQ端口
        - username: RabbitMQ用户名
        - password: RabbitMQ密码
        - redis_host: Redis服务器地址（用于状态跟踪）
        - redis_port: Redis端口
        - redis_db: Redis数据库编号
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        # 配置RabbitMQ连接参数
        self.credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=self.credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )

        # Redis客户端用于状态跟踪
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )

        # 队列名称
        self.first_classification_queue = "first_classification_tasks"
        self.second_classification_queue = "second_classification_tasks"
        self.keywords_queue = "auto_get_keywords_tasks"
        self.task_status_prefix = "task_status:"

        # 测试连接
        self._test_connections()

        logger.info(f"✅ RabbitMQ客户端生产者初始化成功: {host}:{port}")

    def _test_connections(self):
        """测试RabbitMQ和Redis连接"""
        # 测试RabbitMQ连接
        try:
            connection = pika.BlockingConnection(self.parameters)
            connection.close()
            logger.info(f"✅ RabbitMQ连接测试成功: {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ RabbitMQ连接失败: {str(e)}")
            raise

        # 测试Redis连接
        try:
            self.redis_client.ping()
            logger.info(f"✅ Redis连接成功: {self.redis_client.connection_pool.connection_kwargs['host']}:{self.redis_client.connection_pool.connection_kwargs['port']}")
        except Exception as e:
            logger.error(f"❌ Redis连接失败: {str(e)}")
            raise

    def send_classification_tasks_and_wait(self,
                                           final_output: List[Dict],
                                           xiaoqi_name: Optional[str] = None,
                                           file_dict_rev: Optional[Dict] = None,
                                           entity_id: Optional[str] = None,
                                           user_id: Optional[str] = None,
                                           timeout: int = 300) -> bool:
        """
        发送分类任务到RabbitMQ并等待完成

        参数:
        - final_output: 需要分类的数据列表
        - file_dict_rev: 文件字典反向映射 {文件路径: 文件ID}
        - entity_id: 实体ID
        - user_id: 用户ID
        - timeout: 超时时间（秒）

        返回:
        - bool: 是否所有任务都成功完成
        """
        if not final_output:
            logger.warning("⚠️ final_output为空，无需发送分类任务")
            return True

        task_id = str(uuid.uuid4())
        total_tasks = len(final_output) * 2  # 每个payload需要一重分类和二重分类

        logger.info(f"🚀 开始发送分类任务 - task_id: {task_id}, payload_count: {len(final_output)}, total_tasks: {total_tasks}")

        # 初始化任务状态
        task_status = {
            'task_id': task_id,
            'total_tasks': total_tasks,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'start_time': datetime.now().isoformat(),
            'status': 'processing',
            'entity_id': entity_id,
            'user_id': user_id,
            'operation': 'insert',
        }

        self.redis_client.hset(f"{self.task_status_prefix}{task_id}", mapping=task_status)
        self.redis_client.expire(f"{self.task_status_prefix}{task_id}", 3600)  # 1小时过期

        try:
            # 建立RabbitMQ连接
            connection = pika.BlockingConnection(self.parameters)
            channel = connection.channel()

            # 声明队列
            channel.queue_declare(queue=self.first_classification_queue, durable=True)
            channel.queue_declare(queue=self.second_classification_queue, durable=True)
            channel.queue_declare(queue=self.keywords_queue, durable=True)
            # 发送任务到RabbitMQ
            for i, payload in enumerate(final_output):
                # 一重分类任务
                first_task = {
                    'task_id': task_id,
                    'sub_task_id': f"{task_id}_first_{i}",
                    'payload': payload,
                    'task_type': 'first_classification',
                    'entity_id': entity_id,
                    'user_id': user_id,
                    'file_dict_rev': file_dict_rev,
                    'timestamp': datetime.now().isoformat()
                }

                # 二重分类任务
                second_task = {
                    'task_id': task_id,
                    'sub_task_id': f"{task_id}_second_{i}",
                    'payload': payload,
                    'task_type': 'second_classification',
                    'entity_id': entity_id,
                    'user_id': user_id,
                    'file_dict_rev': file_dict_rev,
                    'timestamp': datetime.now().isoformat()
                }


                # 发送一重分类消息
                channel.basic_publish(
                    exchange='',
                    routing_key=self.first_classification_queue,
                    body=json.dumps(first_task, ensure_ascii=False),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # 持久化消息
                        message_id=f"first_{task_id}_{i}",
                        headers={'task_type': 'first_classification'}
                    )
                )

                # 发送二重分类消息
                channel.basic_publish(
                    exchange='',
                    routing_key=self.second_classification_queue,
                    body=json.dumps(second_task, ensure_ascii=False),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # 持久化消息
                        message_id=f"second_{task_id}_{i}",
                        headers={'task_type': 'second_classification'}
                    )
                )


                logger.debug(f"📤 已发送任务 {i+1}/{len(final_output)} - task_id: {task_id}")
            result = self._wait_for_task_completion(task_id, total_tasks, timeout)
            for i, payload in enumerate(final_output):
                # 关键词任务
                keywords_task = {
                    'task_id': task_id,
                    'sub_task_id': f"{task_id}_keywords_{i}",
                    'payload': payload,
                    'task_type': 'auto_get_keywords',
                    'entity_id': entity_id,
                    'user_id': user_id,
                    'file_dict_rev': file_dict_rev,
                    'xiaoqi_name': xiaoqi_name,
                    'timestamp': datetime.now().isoformat()
                }
                #发送key_words任务
                channel.basic_publish(
                    exchange='',
                    routing_key=self.keywords_queue,
                    body=json.dumps(keywords_task, ensure_ascii=False),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # 持久化消息
                        message_id=f"keywords_{task_id}_{i}",
                        headers={'task_type': 'keywords'}
                    )
                )
            connection.close()
            logger.info(f"✅ 所有任务已发送到RabbitMQ - task_id: {task_id}")
            # 等待任务完成
            return result

        except Exception as e:
            logger.error(f"❌ 发送任务到RabbitMQ失败 - task_id: {task_id}, error: {str(e)}")
            # 更新任务状态为失败
            self.redis_client.hset(f"{self.task_status_prefix}{task_id}", "status", "failed")
            return False
    def send_first_classification_tasks_and_wait(self,
                                                 final_output: List[Dict],
                                                 file_dict_rev: Optional[Dict] = None,
                                                 entity_id: Optional[str] = None,
                                                 user_id: Optional[str] = None,
                                                 timeout: int = 300) -> bool:
        """
        仅发送一重分类任务到RabbitMQ并等待完成

        参数:
        - final_output: 需要分类的数据列表
        - file_dict_rev: 文件字典反向映射 {文件路径: 文件ID}
        - entity_id: 实体ID
        - user_id: 用户ID
        - timeout: 超时时间（秒）

        返回:
        - bool: 是否所有任务都成功完成
        """
        if not final_output:
            logger.warning("⚠️ final_output为空，无需发送一重分类任务")
            return True

        task_id = str(uuid.uuid4())
        # 主要修改点：现在每个payload只产生一个任务，所以总任务数就是列表长度
        total_tasks = len(final_output)

        logger.info(f"🚀 开始发送一重分类任务 - task_id: {task_id}, total_tasks: {total_tasks}")

        # 初始化任务状态
        task_status = {
            'task_id': task_id,
            'total_tasks': total_tasks,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'start_time': datetime.now().isoformat(),
            'status': 'processing',
            'entity_id': entity_id,
            'user_id': user_id,

        }
        try:
            self.redis_client.hset(f"{self.task_status_prefix}{task_id}", mapping=task_status)
            self.redis_client.expire(f"{self.task_status_prefix}{task_id}", 3600)  # 1小时过期
        except Exception as e:
            print(e)
        try:
            # 建立RabbitMQ连接
            connection = pika.BlockingConnection(self.parameters)
            channel = connection.channel()

            # 声明一重分类队列
            channel.queue_declare(queue=self.first_classification_queue, durable=True)

            # 循环发送所有一重分类任务
            for i, payload in enumerate(final_output):
                # 只创建和发送一重分类任务
                first_task = {
                    'task_id': task_id,
                    'sub_task_id': f"{task_id}_first_{i}",
                    'payload': payload,
                    'task_type': 'first_classification',
                    'entity_id': entity_id,
                    'user_id': user_id,
                    'file_dict_rev': file_dict_rev,
                    'timestamp': datetime.now().isoformat(),
                    'operation': 'update',
                }

                # 发送一重分类消息
                channel.basic_publish(
                    exchange='',
                    routing_key=self.first_classification_queue,
                    body=json.dumps(first_task, ensure_ascii=False),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # 持久化消息
                        message_id=f"first_{task_id}_{i}",
                        headers={'task_type': 'first_classification'}
                    )
                )
                logger.debug(f"📤 已发送一重分类任务 {i+1}/{len(final_output)} - task_id: {task_id}")

            connection.close()
            logger.info(f"✅ 所有一重分类任务已发送到RabbitMQ - task_id: {task_id}")

            # 等待所有任务完成
            return self._wait_for_task_completion(task_id, total_tasks, timeout)

        except Exception as e:
            logger.error(f"❌ 发送一重分类任务到RabbitMQ失败 - task_id: {task_id}, error: {str(e)}")
            # 更新任务状态为失败
            self.redis_client.hset(f"{self.task_status_prefix}{task_id}", "status", "failed")
            return False

    def _wait_for_task_completion(self, task_id: str, total_tasks: int, timeout: int) -> bool:
        """
        等待任务完成

        参数:
        - task_id: 任务ID
        - total_tasks: 总任务数
        - timeout: 超时时间（秒）

        返回:
        - bool: 是否成功完成
        """
        start_time = time.time()
        check_interval = 1  # 每秒检查一次

        logger.info(f"⏳ 开始等待任务完成 - task_id: {task_id}, total_tasks: {total_tasks}, timeout: {timeout}s")

        while time.time() - start_time < timeout:
            try:
                # 获取任务状态
                status_data = self.redis_client.hgetall(f"{self.task_status_prefix}{task_id}")

                if not status_data:
                    logger.warning(f"⚠️ 任务状态不存在 - task_id: {task_id}")
                    return False

                completed_tasks = int(status_data.get('completed_tasks', 0))
                failed_tasks = int(status_data.get('failed_tasks', 0))
                current_status = status_data.get('status', 'unknown')

                logger.debug(f"📊 任务进度 - task_id: {task_id}, completed: {completed_tasks}/{total_tasks}, failed: {failed_tasks}")

                # 检查是否完成
                if completed_tasks + failed_tasks >= total_tasks:
                    if failed_tasks == 0:
                        logger.info(f"✅ 所有任务已成功完成 - task_id: {task_id}, completed: {completed_tasks}")
                        self.redis_client.hset(f"{self.task_status_prefix}{task_id}", "status", "completed")
                        return True
                    else:
                        logger.warning(f"⚠️ 任务部分失败 - task_id: {task_id}, completed: {completed_tasks}, failed: {failed_tasks}")
                        self.redis_client.hset(f"{self.task_status_prefix}{task_id}", "status", "partial_failed")
                        return completed_tasks > 0  # 只要有成功的就返回True

                # 检查是否被标记为失败
                if current_status == 'failed':
                    logger.error(f"❌ 任务被标记为失败 - task_id: {task_id}")
                    return False

                time.sleep(check_interval)

            except Exception as e:
                logger.error(f"❌ 检查任务状态时出错 - task_id: {task_id}, error: {str(e)}")
                time.sleep(check_interval)

        # 超时处理
        logger.warning(f"⏰ 等待任务完成超时 - task_id: {task_id}, timeout: {timeout}s")
        self.redis_client.hset(f"{self.task_status_prefix}{task_id}", "status", "timeout")
        return False

    def mark_task_completed(self, task_id: str, sub_task_id: str, success: bool = True) -> None:
        """
        标记子任务完成

        参数:
        - task_id: 主任务ID
        - sub_task_id: 子任务ID
        - success: 是否成功
        """
        try:
            status_key = f"{self.task_status_prefix}{task_id}"

            if success:
                self.redis_client.hincrby(status_key, "completed_tasks", 1)
                logger.debug(f"✅ 子任务完成 - task_id: {task_id}, sub_task_id: {sub_task_id}")
            else:
                self.redis_client.hincrby(status_key, "failed_tasks", 1)
                logger.warning(f"❌ 子任务失败 - task_id: {task_id}, sub_task_id: {sub_task_id}")

        except Exception as e:
            logger.error(f"❌ 标记任务状态时出错 - task_id: {task_id}, sub_task_id: {sub_task_id}, error: {str(e)}")

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """
        获取任务状态

        参数:
        - task_id: 任务ID

        返回:
        - Dict: 任务状态信息
        """
        try:
            status_data = self.redis_client.hgetall(f"{self.task_status_prefix}{task_id}")
            if status_data:
                # 转换数字字段
                status_data['total_tasks'] = int(status_data.get('total_tasks', 0))
                status_data['completed_tasks'] = int(status_data.get('completed_tasks', 0))
                status_data['failed_tasks'] = int(status_data.get('failed_tasks', 0))
                return status_data
            return None
        except Exception as e:
            logger.error(f"❌ 获取任务状态失败 - task_id: {task_id}, error: {str(e)}")
            return None


# 创建全局生产者实例
try:
    rabbitmq_producer = RabbitMQClientProducer()
    logger.info("✅ 全局RabbitMQ生产者创建成功")
except Exception as e:
    logger.error(f"❌ 创建全局RabbitMQ生产者失败: {str(e)}")
    rabbitmq_producer = None

# 导出兼容接口
def send_classification_tasks_and_wait(final_output: List[Dict], file_dict_rev: Optional[Dict] = None, entity_id: Optional[str] = None, user_id: Optional[str] = None, xiaoqi_name: Optional[str] = None) -> bool:
    """
    发送分类任务并等待完成（兼容原接口）
    """
    if rabbitmq_producer is None:
        logger.error("❌ RabbitMQ生产者未初始化")
        return False

    return rabbitmq_producer.send_classification_tasks_and_wait(final_output,xiaoqi_name, file_dict_rev, entity_id, user_id)