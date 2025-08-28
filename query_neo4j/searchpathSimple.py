# coding:utf-8

import zhconv
import argparse
from neo4j import GraphDatabase
import json
import pymysql
import time
import jieba
import jieba.posseg as pseg
def main(request):
    sentence = request.GET["sentence"]
    try:
        res = pseg.cut(sentence)
        words = []
        flags = []
        for word, flag in res:
            words.append(word)
            flags.append(flag)
        result = extract_words(words, flags)
    except IndexError:
        result=['根科目','图论']



    # 抽取实体
    # result=start(sentence)
    # print(result)
    name1 = result[0]
    name2 = result[1]


    driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))
    session = driver.session()


    nodeid_list = []  # 用于去重
    linkid_list = []  # 用于去重
    sum_list = {}  # 总数据
    sum_list["type"]="force"
    sum_list["categories"]=[
        {
            "name": "条目(来源一)",
            "keyword": {},
            "base": "HTMLElement"
        },
        {
            "name": "二层科目",
            "keyword": {},
            "base": "WebGLRenderingContext"
        },
        {
            "name": "其他科目",
            "keyword": {},
            "base": "SVGElement",
            "itemStyle": {
                "normal": {
                    "color": 'rgba(255,186,44,0.95)'
                }
            }
        },
        {
            "name": "根科目",
            "keyword": {},
            "base": "CSSRule",
            "itemStyle": {
                "normal": {
                    "color": 'rgb(236,147,158)'
                }
            }
        },
        {
            "name": "条目(来源二)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(78,189,236)'
                }
            }
        },
        {
            "name": "一层科目",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(52,176,143)'
                }
            }
        },
        {
            "name": "条目(来源三)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#91D8E4'  #5994a0
                }
            }
        },
        {
            "name": "家谱",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#408E91'  # 5994a0
                }
            }
        },
        {
            "name": "自建节点",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#738fa0'  # 5994a0
                }
            }
        },
        {
            "name": "条目(来源四)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#8376a0'  #5994a0
                }
            }
        }
    ]

    nodes = []
    links = []
    table = []
    sameAs = []

    #查询知海
    query_word = '''
        match (n1),(n2)
        where n1.name=\"'''+name1+'''\" and n2.name=\"'''+name2+'''\"
        with n1,n2 limit 1
        match p = shortestPath((n1) - [*] - (n2))
        UNWIND nodes(p) AS node
        UNWIND relationships(p) AS rel
        RETURN node, rel limit 50
        '''
    results_place = session.run(query_word)
    relations=[]
    for record in results_place:
        relations.append(record['rel'])

    relations=set(relations)
    for res in relations:
        node0=res.nodes[0]
        node1=res.nodes[1]

        if node0 is not None:
            if node0.id not in nodeid_list:
                node_set = {}

                nodeid_list.append(node0.id)
                node_set["id"] = str(node0.id)
                node_set["value"] = 1
                node_set["name"] = zhconv.convert(node0._properties["name"],'zh-cn')
                node_set["url"] = zhconv.convert(node0._properties["url"],'zh-cn')
                node_set["timestamp"] = node0._properties["timestamp"]
                node_set["symbolSize"] = 40
                if 'Wikipedia' in list(set(node0.labels)):
                    node_set['category'] = 0
                elif 'CaLe0' in list(set(node0.labels)):
                    node_set['category'] = 3
                elif 'CaLe1' in list(set(node0.labels)):
                    node_set['category'] = 5
                elif 'CaLe2' in list(set(node0.labels)):
                    node_set['category'] = 1
                elif 'ownthink' in list(set(node0.labels)):
                    node_set['category'] = 4
                elif 'selfCreate' in list(set(node0.labels)):
                    node_set['category'] = 8
                elif 'ScholarCSKG' in list(set(node0.labels)):
                    node_set['category'] = 9
                else:
                    node_set['category'] = 2
                node_set['properties']=node0._properties

                if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    nodes.append(node_set)

        if node1 is not None:
            if node1.id not in nodeid_list:
                node_set1 = {}
                nodeid_list.append(node1.id)
                node_set1["id"] = str(node1.id)
                node_set1["value"] = 1
                node_set1["name"] =  zhconv.convert(node1._properties["name"],'zh-cn')
                node_set1["url"] = zhconv.convert(node1._properties["url"],'zh-cn')
                node_set1["timestamp"] = node1._properties["timestamp"]
                node_set1["symbolSize"] = 40
                if 'Wikipedia' in list(set(node1.labels)):
                    node_set1['category'] = 0
                elif 'CaLe0' in list(set(node1.labels)):
                    node_set1['category'] = 3
                elif 'CaLe1' in list(set(node1.labels)):
                    node_set1['category'] = 5
                elif 'CaLe2' in list(set(node1.labels)):
                    node_set1['category'] = 1
                elif 'ownthink' in list(set(node1.labels)):
                    node_set1['category'] = 4
                elif 'selfCreate' in list(set(node1.labels)):
                    node_set1['category'] = 8
                elif 'ScholarCSKG' in list(set(node1.labels)):
                    node_set1['category'] = 9
                else:
                    node_set1['category'] = 2
                node_set1['properties']=node1._properties

                if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    nodes.append(node_set1)


        link_set = {}

        if res is not None:
            if res.id not in linkid_list:

                startNodeId=str(node0.id)
                endNodeId=str(node1.id)

                for node in nodes:
                    if startNodeId==node["id"]:
                        link_set["target"]=startNodeId
                    if endNodeId==node["id"]:
                        link_set["source"]=endNodeId
                    link_set["label"]={"show":True,"formatter":res.type,"fontSize":12}
                if len(link_set) != 0:
                    links.append(link_set)

                start_properties=node0._properties
                end_properties=node1._properties
                start_properties["name"]=zhconv.convert(start_properties["name"],'zh-cn')
                start_properties["url"]=zhconv.convert(start_properties["url"],'zh-cn')
                end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')
                end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')




    # #查询华谱
    sum_list['nodes'] = nodes
    sum_list['links'] = links
    # sum_list['table'] = table
    session.close()


    return json.dumps(sum_list, ensure_ascii=False)
def extract_words(words, tags):
    extracted_words = []
    i = 0
    while i < len(tags):
        if tags[i] == 'n' or tags[i] == 'nr' or tags[i] == 'nz' or tags[i] == 'eng' or tags[i] == 'p':
            j = i + 1
            while j < len(tags) and tags[j] not in ['p', 'c']:
                j += 1
            extracted_words.append(''.join(words[i:j]))
            i = j - 1
        elif tags[i] == 'c' or tags[i] == 'p':
            j = i + 1
            while j < len(tags) and tags[j] not in ['nd', 'uj']:
                j += 1
            extracted_words.append(''.join(words[i+1:j]))
            i = j - 1
        i += 1
    res=[]
    res.append(extracted_words[0])
    res.append(extracted_words[1])
    return res
