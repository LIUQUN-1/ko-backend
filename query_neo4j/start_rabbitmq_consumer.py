#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RabbitMQ消费者启动脚本
支持同步请求-响应模式的分类任务处理
"""

import os
import sys
import signal
import time
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 设置Django环境
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'KoDjango.settings')

from rabbitmq_client_consumer import RabbitMQClientConsumer

# 全局变量
consumer = None
running = True

def signal_handler(signum, frame):
    """信号处理器"""
    global running, consumer
    
    print(f"🛑 接收到信号 {signum}，开始优雅关闭...")
    running = False
    
    if consumer:
        consumer.stop_consuming()
    
    print("👋 RabbitMQ消费者已关闭")
    sys.exit(0)

def check_dependencies():
    """检查依赖"""
    print("🔍 检查系统依赖...")
    
    try:
        import pika
        print("✅ pika库已安装")
    except ImportError:
        print("❌ pika库未安装，请运行: pip install pika")
        return False
    
    try:
        import redis
        print("✅ Redis库已安装")
    except ImportError:
        print("❌ Redis库未安装，请运行: pip install redis")
        return False
    
    return True

def check_rabbitmq_connection():
    """检查RabbitMQ连接"""
    print("🔍 检查RabbitMQ连接...")
    
    try:
        import pika
        credentials = pika.PlainCredentials('admin', 'admin')
        parameters = pika.ConnectionParameters(
            host='114.213.232.140',
            port=15672,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        connection.close()
        print("✅ RabbitMQ连接正常")
        return True
    except Exception as e:
        print(f"❌ RabbitMQ连接失败: {str(e)}")
        print("💡 请确保RabbitMQ服务已启动，用户名密码正确")
        return False

def check_redis_connection():
    """检查Redis连接"""
    print("🔍 检查Redis连接...")
    
    try:
        import redis
        client = redis.Redis(
            host='114.213.232.140',
            port=26379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        client.ping()
        print("✅ Redis连接正常")
        return True
    except Exception as e:
        print(f"❌ Redis连接失败: {str(e)}")
        print("💡 请确保Redis服务已启动，网络连接正常")
        return False

def main():
    """主函数"""
    global consumer, running
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🚀 启动RabbitMQ消费者...")
    print(f"⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查依赖
    if not check_dependencies():
        print("❌ 依赖检查失败，退出程序")
        sys.exit(1)
    
    # 检查RabbitMQ连接
    if not check_rabbitmq_connection():
        print("❌ RabbitMQ连接检查失败，退出程序")
        sys.exit(1)
    
    # 检查Redis连接
    if not check_redis_connection():
        print("❌ Redis连接检查失败，退出程序")
        sys.exit(1)
    
    try:
        # 创建RabbitMQ消费者
        consumer = RabbitMQClientConsumer()
        print("✅ RabbitMQ消费者初始化成功")
        
        # 启动消费者线程
        first_thread, second_thread = consumer.start_consuming()
        
        print("🎯 RabbitMQ消费者已启动，等待分类任务...")
        print("📊 监听队列:")
        print(f"   - 一重分类: {consumer.first_classification_queue}")
        print(f"   - 二重分类: {consumer.second_classification_queue}")
        print("💡 按 Ctrl+C 停止服务")
        
        # 主循环 - 监控线程状态
        last_health_check = time.time()
        health_check_interval = 60  # 每分钟输出一次健康状态
        
        while running:
            try:
                current_time = time.time()
                
                # 检查线程是否还活着
                if not first_thread.is_alive():
                    print("❌ 一重分类线程已停止")
                    break
                
                if not second_thread.is_alive():
                    print("❌ 二重分类线程已停止")
                    break
                
                # 定期输出健康状态
                if current_time - last_health_check >= health_check_interval:
                    print(f"💚 RabbitMQ消费者运行中 - {datetime.now().strftime('%H:%M:%S')}")
                    print(f"   一重分类线程状态: {'运行中' if first_thread.is_alive() else '已停止'}")
                    print(f"   二重分类线程状态: {'运行中' if second_thread.is_alive() else '已停止'}")
                    last_health_check = current_time
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                print("🛑 收到键盘中断信号")
                break
            except Exception as e:
                print(f"❌ 主循环异常: {str(e)}")
                time.sleep(5)
        
    except Exception as e:
        print(f"❌ 启动RabbitMQ消费者失败: {str(e)}")
        sys.exit(1)
    
    finally:
        if consumer:
            consumer.stop_consuming()
        print("👋 RabbitMQ消费者已退出")

if __name__ == '__main__':
    main() 