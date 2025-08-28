import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import re
import json
from fake_useragent import UserAgent


def add_quotes_and_newlines(text, string_list):
    modified_text = text
    for string_to_find in string_list:
        # 在字符串前添加换行符
        modified_text = modified_text.replace(string_to_find, f'\n"{string_to_find}":')
    return modified_text


def remove_citations(text):
    # 使用正则表达式匹配引用符号并替换为空字符串
    # cleaned_text = re.sub(r'\[\d+\]', '', text)
    cleaned_text = re.sub(r'\[\d+(?:-\d+)?\]', '', str(text))
    cleaned_text = re.sub(r'\s+', '', cleaned_text).strip()
    return cleaned_text


def fetch_category_content(url, category_class):
    try:
        # 发送 GET 请求获取网页内容
        response = requests.get(url)
        response.raise_for_status()

        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找包含指定类别的元素
        category_elements = soup.find_all(class_=category_class)

        # 提取元素的文本内容
        category_content = [element.get_text() for element in category_elements]

        return category_content

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def extract_baidu_baike_content(url, target_class):
    try:
        # 发送 GET 请求
        response = requests.get(url)
        # 检查响应状态码
        response.raise_for_status()
        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
        # 根据类别选择器提取目标元素
        target_element = soup.find('div', class_=target_class)

        if target_element:
            content = target_element.get_text().strip()

            return content
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def extract_category_content(url, category_class="list-dot list-dot-paddingleft"):
    try:
        # 发送 GET 请求获取网页内容,
        headers = {'User-Agent': UserAgent().random}
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
        # 查找包含指定类别的元素
        category_elements = soup.find_all(class_=category_class)
        # 提取元素的文本内容
        category_content = [element.get_text().strip() for element in category_elements]

        return category_content  # 用于判断该URL是否是多义词

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def easyDescription(url):
    response = requests.get(url)
    # 使用BeautifulSoup解析网页内容
    soup = BeautifulSoup(response.text, "html.parser")
    # 查找网页描述标签<meta name="description">
    meta_description = soup.find("meta", {"name": "description"})
    # 提取网页描述内容
    if meta_description:
        description = meta_description.get("content")
        return description
    else:
        return None


def extract_links_from_class(url):
    try:
        # 发送 GET 请求获取网页内容，提取多义词的链接
        response = requests.get(url)
        response.raise_for_status()

        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找包含 coaamet 类的元素
        element = soup.find(class_='lemmaItem_EYg5x')

        # 在 coaamet 类元素下查找所有超链接
        links = []
        if element:
            link_elements = element.find_all('a')
            links = [link.get('href') for link in link_elements]
        return links

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_descrip(name):
    # name = request.GET["name"]
    baidu_baike_url = f"https://baike.baidu.com/item/{name}"
    # print(baidu_baike_url)
    #判断是否是多义词界面，例如“图论”，如果是，那就进入第一个链接
    judge_content_list = extract_category_content(baidu_baike_url)
    state = 0
    if judge_content_list != None and judge_content_list != [] :
        for content in judge_content_list:
            if "多义词" in content:
                n = 0
                judge_links = extract_links_from_class(baidu_baike_url)
                while judge_links == [] and n <= 5:
                    judge_links = extract_links_from_class(baidu_baike_url)
                    n += 1
                if judge_links != []:
                    # print("这是一个多义词")
                    new_link = judge_links[0].split('/item/')[-1]
                    state = 1
    if state == 1:
        baidu_baike_url = re.sub(r'/item/[^/]+', f'/item/{new_link}', baidu_baike_url)

    # 类别选择器
    target_class = "lemma-summary J-summary"  # 简介
    extracted_content = extract_baidu_baike_content(baidu_baike_url, target_class)  # 简介

    serach = 0
    # 多搜索几次描述
    while extracted_content == None and serach <= 30:
        extracted_content = extract_baidu_baike_content(baidu_baike_url, target_class)  # 简介
        serach += 1
        # print(f"执行简介的第{serach-1}次搜索")

    # 移除引用符号，例如 [1] [2-5]
    extracted_content = remove_citations(extracted_content)

    #
    # 打印提取的内容
    txt = extracted_content
    result = txt.replace("播报编辑", '')  # 删除百科网页中没用的字符
    # print(result)

    return result

def main(request):

    name=request.GET['name']
    print(name)
    id=request.GET['nodeId']

    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    # 查询节点是否已有 updateText 属性
    check_query = f"MATCH (n) WHERE id(n) = {id} RETURN EXISTS(n.updateText) AS hasUpdateText"
    result = session.run(check_query)
    has_update_text = result.single()["hasUpdateText"]

    # 如果节点没有 updateText 属性，添加该属性
    if not has_update_text:
        text = get_descrip(name)
        add_update_text_query = f"MATCH (n) WHERE id(n) = {id} SET n.updateText = $updateText"
        session.run(add_update_text_query, updateText=text)

        # 关闭数据库连接
        session.close()

        # 返回响应
        return json.dumps("节点更新完毕", ensure_ascii=False)
        # return
    else:
        return json.dumps("节点已经是最新的状态", ensure_ascii=False)
        # return
