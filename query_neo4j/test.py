#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 search_urls.py 中新增的 files 参数功能
验证文件类型过滤是否正常工作
"""

import os
import sys
import django
from unittest.mock import Mock

# 添加项目路径到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 设置Django环境
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'your_project.settings')
django.setup()

from query_neo4j.search_urls import search_urls

def test_files_parameter():
    """测试files参数功能"""
    
    print("=== 测试 search_urls 的 files 参数功能 ===\n")
    
    # 测试案例1: 搜索PDF文件
    print("1. 测试搜索PDF文件...")
    request_mock = Mock()
    request_mock.GET = {
        'name': '人工智能',
        'num_pages_to_crawl': '5',
        'userID': '123', 
        'enable_deduplication': 'false',
        'files': 'pdf'  # 指定搜索PDF文件
    }
    
    try:
        result = search_urls(request_mock)
        print(f"✅ PDF搜索结果: {result.get('status')}")
        if result.get('data'):
            data = result['data']
            if isinstance(data, dict) and 'unique_urls' in data:
                urls = data['unique_urls']
            else:
                urls = data
            
            print(f"   找到 {len(urls)} 个PDF文件")
            for i, item in enumerate(urls[:3], 1):  # 只显示前3个
                print(f"   {i}. {item.get('title', 'No title')[:50]}...")
                print(f"      URL: {item.get('url', 'No URL')}")
                print(f"      文件类型: {item.get('file_type', 'Unknown')}")
                print()
        
        search_params = result.get('search_params', {})
        print(f"   搜索参数: 关键词={search_params.get('keyword')}, 文件类型={search_params.get('file_type')}")
        print()
        
    except Exception as e:
        print(f"❌ PDF搜索失败: {str(e)}\n")
    
    # 测试案例2: 搜索DOC文件
    print("2. 测试搜索DOC文件...")
    request_mock.GET['files'] = 'doc'
    request_mock.GET['name'] = '机器学习'
    
    try:
        result = search_urls(request_mock)
        print(f"✅ DOC搜索结果: {result.get('status')}")
        if result.get('data'):
            data = result['data']
            if isinstance(data, dict) and 'unique_urls' in data:
                urls = data['unique_urls']
            else:
                urls = data
            print(f"   找到 {len(urls)} 个DOC文件")
        
        search_params = result.get('search_params', {})
        print(f"   搜索参数: 关键词={search_params.get('keyword')}, 文件类型={search_params.get('file_type')}")
        print()
        
    except Exception as e:
        print(f"❌ DOC搜索失败: {str(e)}\n")
    
    # 测试案例3: 不指定文件类型（常规搜索）
    print("3. 测试常规搜索（不指定files参数）...")
    request_mock.GET.pop('files', None)  # 移除files参数
    request_mock.GET['name'] = '深度学习'
    request_mock.GET['num_pages_to_crawl'] = '3'
    
    try:
        result = search_urls(request_mock)
        print(f"✅ 常规搜索结果: {result.get('status')}")
        if result.get('data'):
            data = result['data']
            if isinstance(data, dict) and 'unique_urls' in data:
                urls = data['unique_urls']
            else:
                urls = data
            print(f"   找到 {len(urls)} 个网页")
            
            # 检查默认文件类型
            if urls:
                first_item = urls[0]
                print(f"   默认文件类型: {first_item.get('file_type', 'Unknown')}")
        
        search_params = result.get('search_params', {})
        print(f"   搜索参数: 关键词={search_params.get('keyword')}, 文件类型={search_params.get('file_type')}")
        print()
        
    except Exception as e:
        print(f"❌ 常规搜索失败: {str(e)}\n")
    
    print("=== 测试完成 ===")

def test_parameter_validation():
    """测试参数验证"""
    print("\n=== 测试参数验证 ===\n")
    
    # 测试案例: files参数为None的情况
    print("1. 测试files参数为None...")
    request_mock = Mock()
    request_mock.GET = {
        'name': '测试',
        'num_pages_to_crawl': '2',
        'userID': '123',
        'files': None
    }
    
    try:
        result = search_urls(request_mock)
        search_params = result.get('search_params', {})
        file_type = search_params.get('file_type')
        print(f"✅ files=None时的file_type: {file_type}")
        assert file_type is None, "files为None时，file_type应该也是None"
        print("   ✓ 参数处理正确")
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
    
    print("\n=== 参数验证完成 ===")

if __name__ == "__main__":
    print("开始测试 search_urls 的 files 参数功能...\n")
    
    # 测试主要功能
    test_files_parameter()
    
    # 测试参数验证
    test_parameter_validation()
    
    print("\n所有测试完成！")