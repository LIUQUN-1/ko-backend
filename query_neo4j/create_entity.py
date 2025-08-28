import requests
import json
import pymysql
import redis
import uuid
import time
import logging
from django.http import JsonResponse

# 获取Django配置的日志记录器
logger = logging.getLogger('query_neo4j')

# Redis分布式锁配置
REDIS_HOST = '114.213.232.140'  # Redis服务器地址
REDIS_PORT = 26379              # Redis端口
REDIS_DB = 0                    # Redis数据库编号
REDIS_PASSWORD = None           # Redis密码

class RedisDistributedLock:
    """Redis分布式锁实现"""

    def __init__(self, redis_client, key, timeout=30, retry_times=50, retry_delay=0.1):
        """
        初始化分布式锁
        :param redis_client: Redis客户端
        :param key: 锁的键名
        :param timeout: 锁的超时时间（秒）
        :param retry_times: 重试次数
        :param retry_delay: 重试间隔（秒）
        """
        self.redis_client = redis_client
        self.key = f"distributed_lock:{key}"
        self.timeout = timeout
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.identifier = None

    def acquire(self, blocking=True):
        """获取锁
        :param blocking: 是否阻塞等待，True为阻塞直到获取到锁，False为非阻塞模式
        """
        identifier = str(uuid.uuid4())

        if blocking:
            # 阻塞模式：使用Redis原生的阻塞机制
            while True:
                # 尝试获取锁
                if self.redis_client.set(self.key, identifier, nx=True, ex=self.timeout):
                    self.identifier = identifier
                    return True

                # 使用Redis的BLPOP实现阻塞等待
                wait_key = f"{self.key}:wait"
                try:
                    # BLPOP会阻塞等待，直到有元素或超时
                    result = self.redis_client.blpop(wait_key, timeout=2)
                    continue
                except Exception:
                    # 如果BLPOP出现异常，降级为短暂sleep
                    time.sleep(0.1)
                    continue
        else:
            # 非阻塞模式
            end_time = time.time() + self.timeout

            for _ in range(self.retry_times):
                if time.time() > end_time:
                    return False

                if self.redis_client.set(self.key, identifier, nx=True, ex=self.timeout):
                    self.identifier = identifier
                    return True

                time.sleep(self.retry_delay)

            return False

    def release(self):
        """释放锁"""
        if not self.identifier:
            return False

        # 使用Lua脚本确保原子性，并在释放锁后通知等待的线程
        lua_script = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            local result = redis.call('DEL', KEYS[1])
            if result == 1 then
                redis.call('LPUSH', KEYS[2], '1')
                redis.call('EXPIRE', KEYS[2], 5)
            end
            return result
        else
            return 0
        end
        """

        try:
            wait_key = f"{self.key}:wait"
            result = self.redis_client.eval(lua_script, 2, self.key, wait_key, self.identifier)
            return result == 1
        except Exception as e:
            logger.error(f"释放锁失败: {e}")
            return False

    def __enter__(self):
        """支持with语句"""
        self.acquire(blocking=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.release()

def get_redis_client():
    """获取Redis客户端"""
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # 测试连接
        client.ping()
        return client
    except Exception as e:
        logger.error(f"Redis连接失败: {e}")
        return None

def get_distributed_lock(entity, timeout=30):
    """获取分布式锁"""
    redis_client = get_redis_client()
    if not redis_client:
        raise Exception("Redis连接失败，无法创建分布式锁")

    return RedisDistributedLock(redis_client, f"create_entity:{entity}", timeout)

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
            print("数据库连接成功！")
        except pymysql.MySQLError as e:
            print(f"数据库连接失败：{e}")
            raise

    def query_xiaoqi_count(self, name):
        """
        查询xiaoqi_new表中xiaoqi_name为指定name的记录数量
        """
        try:
            with self.connection.cursor() as cursor:
                query = "SELECT COUNT(*) FROM xiaoqi_new WHERE xiaoqi_name like %s"
                cursor.execute(query, (name+"%",))
                result = cursor.fetchone()
                return result[0] if result else 0
        except pymysql.MySQLError as e:
            print(f"查询失败：{e}")
            raise

    def insert_xiaoqi_new(self, table_name, data, primary_key='xiaoqi_name'):
        """
        插入新的xiaoqi记录到xiaoqi_new表
        """
        try:
            existing_id = None
            # 获取主键值
            primary_key_value = data.get(primary_key)

            # 1. 检查记录是否已存在
            check_query = f"SELECT {primary_key}, xiaoqi_id FROM {table_name} WHERE {primary_key} = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value,))
                existing_id = cursor.fetchone()

                if existing_id:
                    print(f"主键 {primary_key_value} 已存在，返回现有ID")
                    return existing_id[1]  # 直接返回已存在记录的ID

            # 2. 执行插入操作
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print(f"✅ 成功插入xiaoqi: {primary_key_value}")
                return cursor.lastrowid  # 返回新插入的ID

        except pymysql.MySQLError as e:
            print(f"插入失败：{e}")
            self.connection.rollback()

            # 3. 插入失败时再次查询已存在ID
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(check_query, (primary_key_value,))
                    existing_id = cursor.fetchone()
                    return existing_id[1] if existing_id else None
            except Exception as e:
                print(f"二次查询失败：{e}")
                return None

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭！")

def main(request):
    try:
        data = json.loads(request.body)
        name = data.get('name')

        if not name:
            return JsonResponse({"status": "error", "message": "缺少name参数"})

    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "message": "无效的JSON格式"})

    # 数据库连接
    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",
        password="DMiC-4092",
        database="db_hp"
    )

    try:
        db.connect()

        # 调用redis分布式锁 对这部分进行加锁防止存在并发问题
        try:
            with get_distributed_lock(name, timeout=30) as lock:
                lock.acquire(blocking=True)
                # 在锁保护下执行查询和插入操作
                count = db.query_xiaoqi_count(name)

                # 新建实体为 name+"count+1"的实体 上传到xiaoqi_new文件表上面
                new_entity_name = f"{name}{count + 1}"

                # 准备插入数据
                data_to_insert = {
                    "xiaoqi_name": new_entity_name
                }

                # 插入新实体到xiaoqi_new表
                xiaoqi_id = db.insert_xiaoqi_new("xiaoqi_new", data_to_insert)

                return JsonResponse({
                    "status": "success",
                    "message": f"查询并创建实体成功",
                    "data": {
                        "original_name": name,
                        "existing_count": count,
                        "new_entity_name": new_entity_name,
                        "new_xiaoqi_id": xiaoqi_id
                    }
                })

        except Exception as lock_e:
            logger.error(f"Redis锁操作异常: {str(lock_e)}")
            # 如果Redis锁不可用，降级为无锁操作
            logger.warning(f"Redis锁不可用，降级为无锁操作")

            return JsonResponse({
                "status": "error",
                "message": f"Redis锁不可用",
                "data": {
                    "original_name": name,
                    "error": str(lock_e)
                }
            })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": f"数据库操作失败: {str(e)}"
        })

    finally:
        db.close()


