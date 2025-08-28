"""
基于RocketMQ的实体分类API顺序处理模块
解决并发请求导致的响应顺序不一致问题
"""

import json
import time
import threading
import uuid
from typing import Dict, Optional, Callable
from rocketmq.client import Producer, PushConsumer, ConsumeStatus
from rocketmq.common import Message
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor


class ClassificationMQManager:
    """分类请求的MQ管理器"""
    
    def __init__(self, 
                 mq_server: str = "127.0.0.1:9876",
                 topic: str = "classification_requests",
                 consumer_group: str = "classification_consumer_group"):
        self.mq_server = mq_server
        self.topic = topic
        self.consumer_group = consumer_group
        
        # 初始化生产者
        self.producer = Producer(self.consumer_group + "_producer")
        self.producer.set_namesrv_addr(self.mq_server)
        
        # 初始化消费者
        self.consumer = PushConsumer(self.consumer_group)
        self.consumer.set_namesrv_addr(self.mq_server)
        self.consumer.subscribe(self.topic, self._message_callback)
        
        # 结果存储和回调管理
        self.results: Dict[str, any] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.result_lock = threading.RLock()
        
        # 启动状态
        self._started = False
        
    def start(self):
        """启动MQ服务"""
        if not self._started:
            self.producer.start()
            self.consumer.start()
            self._started = True
            print("🚀 RocketMQ分类服务已启动")
    
    def shutdown(self):
        """关闭MQ服务"""
        if self._started:
            self.producer.shutdown()
            self.consumer.shutdown()
            self._started = False
            print("⏹️ RocketMQ分类服务已关闭")
    
    def submit_classification_request(self, 
                                    payload: dict, 
                                    callback: Optional[Callable] = None,
                                    timeout: int = 30) -> str:
        """
        提交分类请求到MQ
        
        Args:
            payload: 请求数据
            callback: 结果回调函数
            timeout: 超时时间(秒)
            
        Returns:
            请求ID
        """
        request_id = str(uuid.uuid4())
        
        # 构造消息
        message_body = {
            "request_id": request_id,
            "payload": payload,
            "timestamp": time.time(),
            "timeout": timeout
        }
        
        # 创建顺序消息 - 使用同一个sharding key确保顺序
        message = Message(self.topic)
        message.set_body(json.dumps(message_body, ensure_ascii=False))
        message.set_keys(request_id)
        
        # 使用同一个队列选择器确保顺序处理
        sharding_key = "classification_order"  # 固定的分片键保证顺序
        
        try:
            # 发送顺序消息
            send_result = self.producer.send_orderly(message, sharding_key)
            
            if callback:
                with self.result_lock:
                    self.callbacks[request_id] = callback
            
            print(f"📤 [请求-{request_id}] 已提交到MQ队列")
            print(f"📨 发送结果: {send_result}")
            
            return request_id
            
        except Exception as e:
            print(f"❌ [请求-{request_id}] MQ发送失败: {str(e)}")
            raise
    
    def get_result(self, request_id: str, timeout: int = 30) -> Optional[dict]:
        """
        同步获取分类结果
        
        Args:
            request_id: 请求ID
            timeout: 超时时间
            
        Returns:
            分类结果或None
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.result_lock:
                if request_id in self.results:
                    result = self.results.pop(request_id)
                    # 清理回调
                    self.callbacks.pop(request_id, None)
                    return result
            
            time.sleep(0.1)  # 避免CPU占用过高
        
        print(f"⏰ [请求-{request_id}] 获取结果超时")
        return None
    
    def _message_callback(self, message):
        """MQ消息回调处理"""
        try:
            # 解析消息
            message_body = json.loads(message.body.decode('utf-8'))
            request_id = message_body.get("request_id")
            payload = message_body.get("payload")
            timestamp = message_body.get("timestamp", time.time())
            
            print(f"📥 [请求-{request_id}] 开始处理MQ消息")
            print(f"⏱️ 消息延迟: {time.time() - timestamp:.3f}秒")
            
            # 调用原始的分类API
            result = self._call_classification_api(payload, request_id)
            
            # 存储结果
            with self.result_lock:
                self.results[request_id] = result
                
                # 执行回调
                if request_id in self.callbacks:
                    callback = self.callbacks[request_id]
                    try:
                        callback(request_id, result)
                    except Exception as callback_e:
                        print(f"⚠️ [请求-{request_id}] 回调执行失败: {str(callback_e)}")
            
            print(f"✅ [请求-{request_id}] MQ消息处理完成")
            return ConsumeStatus.CONSUME_SUCCESS
            
        except Exception as e:
            print(f"❌ MQ消息处理失败: {str(e)}")
            print(f"📄 消息内容: {message.body.decode('utf-8', errors='ignore')}")
            return ConsumeStatus.RECONSUME_LATER
    
    def _call_classification_api(self, payload: dict, request_id: str) -> dict:
        """调用分类API的内部方法"""
        url = "http://114.213.232.140:8000/api/classify/entity/"
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PythonClient/1.0",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "X-Request-ID": request_id
        }
        
        print(f"🌐 [请求-{request_id}] 调用分类API: {url}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(10, 30),
                    verify=False
                )
                
                print(f"📥 [请求-{request_id}] API响应状态: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        print(f"✅ [请求-{request_id}] API调用成功")
                        return {
                            "success": True,
                            "data": result,
                            "request_id": request_id,
                            "timestamp": time.time()
                        }
                    except json.JSONDecodeError as json_e:
                        print(f"❌ [请求-{request_id}] JSON解析失败: {str(json_e)}")
                        return {
                            "success": False,
                            "error": f"JSON解析失败: {str(json_e)}",
                            "request_id": request_id
                        }
                else:
                    error_msg = f"API调用失败，状态码：{response.status_code}"
                    print(f"❌ [请求-{request_id}] {error_msg}")
                    
                    if response.status_code >= 500 and attempt < max_retries - 1:
                        wait_time = 2 ** (attempt + 1)
                        print(f"🔄 [请求-{request_id}] {wait_time}秒后重试")
                        time.sleep(wait_time)
                        continue
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "request_id": request_id,
                        "response_text": response.text
                    }
                    
            except requests.exceptions.Timeout:
                print(f"⏰ [请求-{request_id}] API请求超时 (第{attempt+1}次)")
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return {
                    "success": False,
                    "error": "请求超时",
                    "request_id": request_id
                }
            except Exception as e:
                print(f"💥 [请求-{request_id}] API请求异常: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return {
                    "success": False,
                    "error": str(e),
                    "request_id": request_id
                }
        
        return {
            "success": False,
            "error": "所有重试都失败了",
            "request_id": request_id
        }


# 全局MQ管理器实例
_mq_manager: Optional[ClassificationMQManager] = None


def init_mq_manager(mq_server: str = "127.0.0.1:9876") -> ClassificationMQManager:
    """初始化全局MQ管理器"""
    global _mq_manager
    if _mq_manager is None:
        _mq_manager = ClassificationMQManager(mq_server=mq_server)
        _mq_manager.start()
    return _mq_manager


def get_mq_manager() -> ClassificationMQManager:
    """获取全局MQ管理器"""
    global _mq_manager
    if _mq_manager is None:
        raise RuntimeError("MQ管理器未初始化，请先调用 init_mq_manager()")
    return _mq_manager


def classification_with_mq(payload: dict, timeout: int = 30) -> dict:
    """
    使用MQ的分类请求（同步接口，保持兼容性）
    
    Args:
        payload: 请求数据
        timeout: 超时时间
        
    Returns:
        分类结果
    """
    manager = get_mq_manager()
    
    # 提交请求
    request_id = manager.submit_classification_request(payload, timeout=timeout)
    
    # 等待结果
    result = manager.get_result(request_id, timeout=timeout)
    
    if result is None:
        return {
            "success": False,
            "error": "请求超时",
            "request_id": request_id
        }
    
    return result


async def classification_with_mq_async(payload: dict, timeout: int = 30) -> dict:
    """
    使用MQ的异步分类请求
    
    Args:
        payload: 请求数据
        timeout: 超时时间
        
    Returns:
        分类结果
    """
    manager = get_mq_manager()
    
    # 提交请求
    request_id = manager.submit_classification_request(payload, timeout=timeout)
    
    # 异步等待结果
    start_time = time.time()
    while time.time() - start_time < timeout:
        with manager.result_lock:
            if request_id in manager.results:
                result = manager.results.pop(request_id)
                manager.callbacks.pop(request_id, None)
                return result
        
        await asyncio.sleep(0.1)
    
    return {
        "success": False,
        "error": "请求超时",
        "request_id": request_id
    }


def classification_with_callback(payload: dict, 
                               callback: Callable[[str, dict], None],
                               timeout: int = 30) -> str:
    """
    使用回调的分类请求
    
    Args:
        payload: 请求数据
        callback: 结果回调函数 callback(request_id, result)
        timeout: 超时时间
        
    Returns:
        请求ID
    """
    manager = get_mq_manager()
    return manager.submit_classification_request(payload, callback=callback, timeout=timeout)


# 使用示例和测试代码
if __name__ == "__main__":
    # 示例1: 同步调用
    def test_sync_classification():
        print("🧪 测试同步分类请求")
        
        # 初始化MQ
        init_mq_manager("127.0.0.1:9876")
        
        # 测试数据
        test_payload = {
            "text": "测试实体分类",
            "entities": ["北京", "清华大学"]
        }
        
        try:
            result = classification_with_mq(test_payload)
            print(f"🎯 分类结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"❌ 测试失败: {str(e)}")
    
    # 示例2: 回调方式
    def test_callback_classification():
        print("🧪 测试回调分类请求")
        
        def result_callback(request_id: str, result: dict):
            print(f"🎯 [回调-{request_id}] 收到结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        
        manager = get_mq_manager()
        
        # 发送多个请求测试顺序
        requests_ids = []
        for i in range(3):
            payload = {
                "text": f"测试实体分类 {i+1}",
                "entities": [f"实体{i+1}"]
            }
            req_id = classification_with_callback(payload, result_callback)
            requests_ids.append(req_id)
            print(f"📤 提交请求 {i+1}: {req_id}")
        
        # 等待处理完成
        time.sleep(10)
    
    # 运行测试
    try:
        test_sync_classification()
        test_callback_classification()
    finally:
        # 清理资源
        if _mq_manager:
            _mq_manager.shutdown() 