import sys
import time
from django.http import HttpResponse, Http404, StreamingHttpResponse
from django.utils.encoding import escape_uri_path
import zipfile
import os
from py2neo import Graph, Node, Relationship
import pandas as pd
import io
import sys
import networkx as nx
import requests
from bs4 import BeautifulSoup
import json
import re
def get_zhwiki_search_results(keyword):
    # 构建中文维基百科搜索 API 的 URL
    url = f"https://zh.wikipedia.org/w/api.php?action=query&list=search&srsearch={keyword}&format=json&uselang=zh"
    # 发送 API 请求并获取响应
    response = requests.get(url)
    data = response.json()

    # 提取搜索结果
    search_results = []
    for result in data['query']['search']:
        title = result['title']
        excerpt = result['snippet']
        page_id = result['pageid']
        # article_url = f"https://zh.wikipedia.org/?curid={page_id}"
        article_url = f"https://zh.wikipedia.org/wiki/{title}"
        # print(article_url)
        # print(excerpt)
        # 查找所有的searchmatch span标签
        end_index = excerpt.rfind("</span>")

        # 获取最后一个 "</span>" 到字符串结尾的字符
        des_result = excerpt[end_index + len("</span>"):]
        des_result = truncate_at_first_period(des_result)
        des_result = title + "   " + des_result
        search_results.append({'title': title, 'excerpt': des_result, 'url': article_url})
        # print(des_result)
    sorted_results = sorted(search_results, key=lambda x: x['title'].startswith(keyword), reverse=True)

    if not sorted_results:
        print(f"未找到任何与 '{keyword}' 相关的页面。")
    return sorted_results
def remove_brackets_and_quotes(text):
    # 删除中括号及其内容的正则表达式
    text1 = re.sub(r'\[.*?\]', '', text)
    text2 = re.sub(r'[“”"]', '', text1)  # 处理中文和英文引号
    # 返回处理后的文本
    return text2
def truncate_at_first_period(input_string):
    first_period_index = str(input_string).find('。')
    if first_period_index != -1:
        return input_string[:first_period_index + 1]
    else:
        return input_string
def extract_category_content(url, category_class = "lemmaSummary_JdnZx J-summary"):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'cookie': 'BDUSS_BFESS=jZxVTBndUdxbmtrdHdOM2otN3FUSzI1UnF1RlA1OGwySTczaDRkMlFJWlR6NmRsRVFBQUFBJCQAAAAAAQAAAAEAAABtJnIlAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFNCgGVTQoBlf; __bid_n=18c57e5bcb2230433b19a6; BAIDU_WISE_UID=wapp_1715047359453_838; H_WISE_SIDS=60275_60521_60564_60359; PSTM=1723870259; BIDUPSID=337F55F709A408AE9090F1686C307290; BAIDUID=097F50E76706AD9DCF7A1F55D0320192:FG=1; BAIDUID_BFESS=097F50E76706AD9DCF7A1F55D0320192:FG=1; H_PS_PSSID=60566_60621_60630; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22365808958%22%2C%22first_id%22%3A%22191b1a3a01f13cd-0d415ffea4c193-4c657b58-2073600-191b1a3a020130e%22%2C%22props%22%3A%7B%7D%2C%22%24device_id%22%3A%22191b1a3a01f13cd-0d415ffea4c193-4c657b58-2073600-191b1a3a020130e%22%7D; channel=bing; H_WISE_SIDS_BFESS=60275_60521_60564_60359; ZFY=4DHL0f1UFs9MOci23TEoOl6GLiSCyMLj2j:AJAz4p2ek:C; baikeVisitId=2ca09e2f-8157-4ecd-acea-0224ca7a4c7a; zhishiTopicRequestTime=1740384792360; baikedeclare=showed; ab_sr=1.0.1_NzBiYWI2OGUxMzIyZDNlMTE1NGE3MTNiODU5MDU5MmE5ZmE4ZjYyMmFmNDY5ZWRlNTlmNzk2MTYwMmRmNWZkZGM5NzhjNGM4NzE3MTFiM2JhOTQwNzY3ZmRhOTQ4ZGY4MGYxN2Q2MDRmNTE2YzdkNzgyYTRhNzBiNmU2YTJmMjlhMmNkNGI4MTk1NzIyNWU2MTc4ZGY3MmVjZmVmOWYwMGU0NTU3YTFhMzRiODRiYjczYTUzOWJlNjE5MzcwZjI1'
        }

        # 发送 GET 请求获取网页内容,
        print(f'待爬取的url:{url}')
        response = requests.get(url, headers=headers)
        # print(response.text)
        patten = r'https:[^\s]*?Q_70'
        img_src = re.findall(patten,response.text)
        response.raise_for_status()
        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
        soups = BeautifulSoup(response.content, 'html.parser')
        print('---soup成功---')
        # 查找包含指定类别的元素
        category_elements = soup.find_all(class_=category_class)
        # 使用 BeautifulSoup 解析网页内容
        category_content_tag = soups.find('div', class_=category_class)
        if category_content_tag:
            # 提取描述信息的文本部分
            category_content = category_content_tag.get_text(strip=True)
            print("content爬取结果：", category_content)
        else:
            category_content_tag = soup.find('meta', attrs={'name': 'description'})
            if category_content_tag:
                category_content = category_content_tag.get('content')
                print(f'text爬取结果:{category_content}')
            else:
                return 0,0
        # print(category_elements)
        # 提取元素的文本内容
        # category_content = [element.get_text().strip() for element in category_elements]
        category_content = remove_brackets_and_quotes(category_content)
        if img_src == []:
            return category_content,0# 用于判断该URL是否是多义词
        else:
            return category_content, img_src[0]
        # return category_content,img_src[0]# 用于判断该URL是否是多义词

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return 0,0
    except Exception as e:
        return 0,0
# def chuli_baidu(html_url):
#     html_url = html_url.replace("\t", "")
#     html_url = html_url.replace(" ", "")
#     response = requests.get(html_url)  # 发送get请求
#     start_index = response.text.find('window.PAGE_DATA= ') + len('window.PAGE_DATA') + 2
#     end_index = response.text.find('}</script>', start_index) + 1
#     page_data = response.text[start_index:end_index]
#     if page_data == "":
#         return "page data error" #信息抽取结果 搜索
#     print(page_data)
#     json_data = json.loads(page_data)
#     # print(json_data)
#     slice_list = html_url.split("/")
#     length = len(slice_list)
#     i = 0
#     while i < length:
#         if slice_list[i] == "item":
#             break
#         i += 1
#     if i + 1 < length:
#         slice_list = slice_list[:i + 2]
#         # print(slice_list)
#         slice_list.append(str(json_data["lemmaId"]))
#         result = '/'.join(slice_list)
#         return result
#     else:
#         return "net error"
def extract_before_pipe(s,id):
    if 'KO' in s:
        news = s.split(' |')[0]
        file_name = s.split(' |')[0]
        file_name = f'{file_name}.jpg'
        return news,file_name
    if '|' in s:
        news = s.split(' |')[0] + '/' + id
        file_name = s.split(' |')[0] + '_' + id
        file_name = f'{file_name}.jpg'
        return news,file_name
    else:
        file_name = f'{s}.jpg'
        return s,file_name
def baidu_suggest(search_word):
    # 该函数用于获取名词在百度百科中的相近词 zihaochen 20241011
    # 构造请求 URL，注意中文字符需要进行 URL 编码
    base_url = "https://baike.baidu.com/api/searchui/suggest"
    params = {
        'enc': 'utf8',
        'wd': search_word  # 将搜索词作为参数
    }

    # 发起 GET 请求
    response = requests.get(base_url, params=params)
    # 检查请求是否成功
    if response.status_code == 200:
        info_list = []
        # 打印返回的内容（通常是 JSON 格式的数据）
        result = response.json()
        for i in result['list']:
            info = {}
            info['name'] = i['lemmaTitle']
            info['id'] = i['lemmaId']
            info['label'] = i['lemmaDesc'] # 能够继续获取图片
            info_list.append(info)

    else:
        info_list = [{'name': search_word, 'id': 0, 'label': '暂无描述'}]
    return info_list
def main(request):
    source = request.GET['source']  # 1-百度百科，0-维基百科
    target = request.GET['name']
    id = request.GET['id']
    print('---开始执行网页接口---')
    print(f'传入的source,target,id:{source}{target}{id}')
    print(f'传入的id:{id}')
    if source == '1':
        print('---开始执行 知识关联 网页爬取接口---')
        # 获取请求的 headers
        target, file_name = extract_before_pipe(target, id)
        # file_name = f'{target}.jpg'
        if target == '郑磊':
            target = '郑磊/49734658'
        if target == '唐杰':
            target = '唐杰/12019960'

        if source == '1':
            name_info = baidu_suggest(target)  # 获取该名词在百度百科中的相近词
            if len(name_info) > 0:
                target = name_info[0]['name']
                sugg_id = name_info[0]['id']
            else:
                sugg_id = 0
            url = f'https://baike.baidu.com/item/{target}/{sugg_id}'
            begin = time.time()
            judge_content_list, img_src = extract_category_content(url)
            if img_src == 0:
                img_src = 'https://bkimg.cdn.bcebos.com/pic/7c1ed21b0ef41bd5ad6ea3156f9096cb39dbb7fd97b2?x-bce-process=image/resize,m_lfit,w_536,limit_1/quality,Q_70'
                if judge_content_list == 0:
                    result = [img_src, '未查询到有效信息', 0, 0]
                    print(json.dumps([judge_content_list], ensure_ascii=False))
                else:
                    result = [img_src, judge_content_list, 0, 0]
                    print(json.dumps([judge_content_list], ensure_ascii=False))
            if judge_content_list == []:
                print('error: 爬取失败')
                judge_content_list = ['---爬取失败---']

            # src_name = download_image(img_src,folder_path,file_name)

            end = time.time()
            cost = end - begin
            result={}
            result["title"]=target
            result["content_text"]=judge_content_list
            result["url"]=url
            return json.dumps([result], ensure_ascii=False)

    if source == '0':
        results = get_zhwiki_search_results(target)
    return json.dumps(results, ensure_ascii=False)