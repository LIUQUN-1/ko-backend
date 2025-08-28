# coding:utf-8
import re
import codecs, sys
from collections import Counter
import json
import csv
import pymysql
import sys
import time
from urllib.parse import quote, unquote
import random
import zhconv
import argparse
from neo4j import GraphDatabase
import json
import requests
import pymysql
import time
from py2neo import *
from py2neo.matching import *


def search_category(name):
    url = "http://zhonghuapu.com:8095/entity/alias2entities"

    params = {
        # "entity_name": "合肥工业大学",
        "alias_name": name
    }
    response = requests.post(url, data=params)
    if response.status_code == 200:
        return response.json()
    else:
        return "请求失败"


# def export_content_file(content, driver, session, sum_list):
#     nodeid_list = []  # 用于去重
#     linkid_list = []  # 用于去重
#     nodes = []
#     links = []
#     table = []
#     query_word = """
#                 MATCH (h)-[r]-(t)-[r1]-(l) WHERE  ((h.name={arg_1} and h:wikibaike) or (h.name={arg_1} and h:baidu_directory))
#                 and (t:hypernode) and (l:baidupage or l:wikipage)
#                 RETURN h,r,t,r1,l LIMIT 50
#
#         """
#     # query_word = '''
#     #
#     #             MATCH (h)-[r]-(t) WHERE (t.name={arg_2}) or (id(h) = {arg_1} and t:wikibaike) or (id(h) = {arg_1} and t:baidu_directory)
#     #              or (id(h) = {arg_1} and t:baidupage) or (id(h) = {arg_1} and t:wikipage) RETURN h,r,t LIMIT 100
#     #             '''
#     result = session.run(query_word, parameters={"arg_1": content})
#     for res in result:
#         # print(res["h"])
#         if res["h"] is not None:
#             if res['h'].id not in nodeid_list:
#                 node_set = {}
#
#                 nodeid_list.append(res['h'].id)
#                 node_set["id"] = str(res['h'].id)
#                 node_set["value"] = 1
#                 a = res['t']
#                 node_set["name"] = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#
#                 node_set["symbolSize"] = 30
#                 # node_set["labels"] = str(res['h'].labels)
#                 if 'Guobiao2017NES' in list(set(res['h'].labels)):
#                     node_set['category'] = 1
#                 elif 'KOCategory' in list(set(res['h'].labels)):
#                     node_set['category'] = 2
#                 elif 'wikibaike' in list(set(res['h'].labels)):
#                     node_set['category'] = 3
#                 elif 'baidu_directory' in list(set(res['h'].labels)):
#                     node_set['category'] = 4
#                 elif 'hypernode' in list(set(res['h'].labels)):
#                     node_set['category'] = 5
#                 elif 'baidupage' in list(set(res['h'].labels)):
#                     node_set['category'] = 6
#                 elif 'wikipage' in list(set(res['h'].labels)):
#                     node_set['category'] = 7
#                 else:
#                     node_set['category'] = 0
#                 node_set['properties'] = res['h']._properties
#                 if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set)
#
#         if res["t"] is not None:
#             if res['t'].id not in nodeid_list:
#                 node_set1 = {}
#                 nodeid_list.append(res['t'].id)
#                 node_set1["id"] = str(res['t'].id)
#                 node_set1["value"] = 1
#                 node_set1["name"] = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_set1["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['t'].labels)):
#                     node_set1['category'] = 1
#                 elif 'KOCategory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 2
#                 elif 'wikibaike' in list(set(res['t'].labels)):
#                     node_set1['category'] = 3
#                 elif 'baidu_directory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 4
#                 elif 'hypernode' in list(set(res['t'].labels)):
#                     node_set1['category'] = 5
#                 elif 'baidupage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 6
#                 elif 'wikipage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 7
#                 else:
#                     node_set1['category'] = 0
#                 node_set1['properties'] = res['t']._properties
#                 if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set1)
#         if res["l"] is not None:
#             if res['l'].id not in nodeid_list:
#                 node_set2 = {}
#                 nodeid_list.append(res['l'].id)
#                 node_set2["id"] = str(res['l'].id)
#                 node_set2["value"] = 1
#                 node_set2["name"] = zhconv.convert(res['l']._properties["name"], 'zh-cn')
#                 node_set2["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['l'].labels)):
#                     node_set2['category'] = 1
#                 elif 'KOCategory' in list(set(res['l'].labels)):
#                     node_set2['category'] = 2
#                 elif 'wikibaike' in list(set(res['l'].labels)):
#                     node_set2['category'] = 3
#                 elif 'baidu_directory' in list(set(res['l'].labels)):
#                     node_set2['category'] = 4
#                 elif 'hypernode' in list(set(res['l'].labels)):
#                     node_set2['category'] = 5
#                 elif 'baidupage' in list(set(res['l'].labels)):
#                     node_set2['category'] = 6
#                 elif 'wikipage' in list(set(res['l'].labels)):
#                     node_set2['category'] = 7
#                 else:
#                     node_set2['category'] = 0
#                 node_set2['properties'] = res['l']._properties
#                 if len(node_set2) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set2)
#         link_set = {}
#         table_set = {}
#         if res["r"] is not None:
#             if res['r'].id not in linkid_list:
#
#                 startNodeId = str(res["r"].nodes[0].id)
#                 endNodeId = str(res["r"].nodes[1].id)
#                 for node in nodes:
#                     if startNodeId == node["id"]:
#                         link_set["target"] = startNodeId
#                     if endNodeId == node["id"]:
#                         link_set["source"] = endNodeId
#                     link_set["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
#                 if len(link_set) != 0:
#                     # print(link_set)
#                     links.append(link_set)
#                     print(links)
#
#                 table_set["_fields"] = []
#                 start_properties = res["r"].nodes[0]._properties
#                 end_properties = res["r"].nodes[1]._properties
#                 start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                 end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                 table_set["_fields"].append({'properties': start_properties})
#                 table_set["_fields"].append({'type': res["r"].type})
#                 table_set["_fields"].append({'properties': end_properties})
#                 table.append(table_set)
#         link_set1 = {}
#         table_set1 = {}
#         if res["r1"] is not None:
#             if res['r1'].id not in linkid_list:
#
#                 startNodeId = str(res["r1"].nodes[0].id)
#                 endNodeId = str(res["r1"].nodes[1].id)
#                 for node in nodes:
#                     if startNodeId == node["id"]:
#                         link_set1["target"] = startNodeId
#                     if endNodeId == node["id"]:
#                         link_set1["source"] = endNodeId
#                     link_set1["label"] = {"show": True, "formatter": res["r1"].type, "fontSize": 12}
#                 if len(link_set1) != 0:
#                     links.append(link_set1)
#
#                 table_set1["_fields"] = []
#                 start_properties = res["r1"].nodes[0]._properties
#                 end_properties = res["r1"].nodes[1]._properties
#                 start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                 end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                 table_set1["_fields"].append({'properties': start_properties})
#                 table_set1["_fields"].append({'type': res["r1"].type})
#                 table_set1["_fields"].append({'properties': end_properties})
#                 table.append(table_set1)
#     # print(nodes)
#     # # print(len(links))
#     # print(links)
#     # print(table)
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#
#     return json.dumps(sum_list, ensure_ascii=False)


def main(request):
    name = request.GET["name"]
    indexlevel = request.GET["index"]
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    nodeid_list = []  # 用于去重
    linkid_list = []  # 用于去重
    sum_list = {}  # 总数据
    sum_list["type"] = "force"

    sum_list["categories"] = [
        {
            "name": "其他",
            "keyword": {},
            "base": "HTMLElement"
        },
        {
            "name": "国标目录",
            "keyword": {},
            "base": "SVGElement",
            "itemStyle": {
                "normal": {
                    "color": 'rgba(255,186,44,0.95)'  # rgba(255,186,44,0.95)
                }
            }
        },
        {
            "name": "目录",
            "keyword": {},
            "base": "CSSRule",
            "itemStyle": {
                "normal": {
                    "color": 'rgb(236,147,158)'
                }
            }
        },
        {
            "name": "维基目录",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(100,176,100)'
                }
            }
        },

        {
            "name": "百度目录",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(52,176,143)'
                }
            }
        },
        {
            "name": "超点",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#FF7F00'
                }
            }
        },
        {
            "name": "百度页面",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#EAADEA'
                }
            }
        },
        {
            "name": "维基页面",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(36,100,143)'
                }
            }
        },

    ]
    nodes = []
    links = []
    table = []
    # 查询知海
    all_result=[]
    query_word = '''

            MATCH (h)<-[r]-(t)<-[r1]-(t1) WHERE (h:KOCategory and h.name contains {arg_1}) and (t:KOCategory) and (t1:KOCategory) RETURN h,r,t,r1,t1 LIMIT 100
            '''
    # query_word = '''
    #
    #             MATCH (h)-[r]-(t) WHERE (t.name={arg_2}) or (id(h) = {arg_1} and t:wikibaike) or (id(h) = {arg_1} and t:baidu_directory)
    #              or (id(h) = {arg_1} and t:baidupage) or (id(h) = {arg_1} and t:wikipage) RETURN h,r,t LIMIT 100
    #             '''
    results_place = session.run(query_word, parameters={"arg_1": name})
    all_result.append(results_place)
    for result in all_result:
        print(result)
        for res in result:
            if res["h"] is not None:
                if res['h'].id not in nodeid_list:
                    node_set = {}

                    nodeid_list.append(res['h'].id)
                    node_set["id"] = str(res['h'].id)
                    node_set["value"] = 1
                    a = res['t']
                    node_set["name"] = zhconv.convert(res['h']._properties["name"], 'zh-cn')

                    node_set["symbolSize"] = 30
                    # node_set["labels"] = str(res['h'].labels)
                    if 'Guobiao2017NES' in list(set(res['h'].labels)):
                        node_set['category'] = 1
                    elif 'KOCategory' in list(set(res['h'].labels)):
                        node_set['category'] = 2
                    elif 'wikibaike' in list(set(res['h'].labels)):
                        node_set['category'] = 3
                    elif 'baidu_directory' in list(set(res['h'].labels)):
                        node_set['category'] = 4
                    elif 'hypernode' in list(set(res['h'].labels)):
                        node_set['category'] = 5
                    elif 'baidupage' in list(set(res['h'].labels)):
                        node_set['category'] = 6
                    elif 'wikipage' in list(set(res['h'].labels)):
                        node_set['category'] = 7
                    else:
                        node_set['category'] = 0
                    node_set['properties'] = res['h']._properties
                    if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        nodes.append(node_set)

            if res["t"] is not None:
                if res['t'].id not in nodeid_list:
                    node_set1 = {}
                    nodeid_list.append(res['t'].id)
                    node_set1["id"] = str(res['t'].id)
                    node_set1["value"] = 1
                    node_set1["name"] = zhconv.convert(res['t']._properties["name"], 'zh-cn')
                    node_set1["symbolSize"] = 30
                    if 'Guobiao2017NES' in list(set(res['t'].labels)):
                        node_set1['category'] = 1
                    elif 'KOCategory' in list(set(res['t'].labels)):
                        node_set1['category'] = 2
                    elif 'wikibaike' in list(set(res['t'].labels)):
                        node_set1['category'] = 3
                    elif 'baidu_directory' in list(set(res['t'].labels)):
                        node_set1['category'] = 4
                    elif 'hypernode' in list(set(res['t'].labels)):
                        node_set1['category'] = 5
                    elif 'baidupage' in list(set(res['t'].labels)):
                        node_set1['category'] = 6
                    elif 'wikipage' in list(set(res['t'].labels)):
                        node_set1['category'] = 7
                    else:
                        node_set1['category'] = 0
                    node_set1['properties'] = res['t']._properties
                    if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        nodes.append(node_set1)

            if res["t1"] is not None:
                if res['t1'].id not in nodeid_list:
                    node_set2 = {}
                    nodeid_list.append(res['t1'].id)
                    node_set2["id"] = str(res['t1'].id)
                    node_set2["value"] = 1
                    node_set2["name"] = zhconv.convert(res['t1']._properties["name"], 'zh-cn')
                    node_set2["symbolSize"] = 30
                    if 'Guobiao2017NES' in list(set(res['t1'].labels)):
                        node_set2['category'] = 1
                    elif 'KOCategory' in list(set(res['t1'].labels)):
                        node_set2['category'] = 2
                    elif 'wikibaike' in list(set(res['t1'].labels)):
                        node_set2['category'] = 3
                    elif 'baidu_directory' in list(set(res['t1'].labels)):
                        node_set2['category'] = 4
                    elif 'hypernode' in list(set(res['t1'].labels)):
                        node_set2['category'] = 5
                    elif 'baidupage' in list(set(res['t1'].labels)):
                        node_set2['category'] = 6
                    elif 'wikipage' in list(set(res['t1'].labels)):
                        node_set2['category'] = 7
                    else:
                        node_set2['category'] = 0
                    node_set2['properties'] = res['t1']._properties
                    if len(node_set2) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        nodes.append(node_set2)


            if res["r"] is not None:
                link_set = {}
                table_set = {}
                if res['r'].id not in linkid_list:

                    startNodeId = str(res["r"].nodes[0].id)
                    endNodeId = str(res["r"].nodes[1].id)
                    for node in nodes:
                        if startNodeId == node["id"]:
                            link_set["target"] = startNodeId
                        if endNodeId == node["id"]:
                            link_set["source"] = endNodeId
                        link_set["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
                    if len(link_set) != 0:
                        links.append(link_set)

                    table_set["_fields"] = []
                    start_properties = res["r"].nodes[0]._properties
                    end_properties = res["r"].nodes[1]._properties
                    start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
                    end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')

                    table_set["_fields"].append({'properties': start_properties})
                    table_set["_fields"].append({'type': res["r"].type})
                    table_set["_fields"].append({'properties': end_properties})
                    table.append(table_set)

            if res["r1"] is not None:
                link_set1 = {}
                table_set1 = {}
                if res['r1'].id not in linkid_list:

                    startNodeId = str(res["r1"].nodes[0].id)
                    endNodeId = str(res["r1"].nodes[1].id)
                    for node in nodes:
                        if startNodeId == node["id"]:
                            link_set1["target"] = startNodeId
                        if endNodeId == node["id"]:
                            link_set1["source"] = endNodeId
                        link_set1["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
                    if len(link_set1) != 0:
                        links.append(link_set1)

                    table_set1["_fields"] = []
                    start_properties = res["r1"].nodes[0]._properties
                    end_properties = res["r1"].nodes[1]._properties
                    start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
                    end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')

                    table_set1["_fields"].append({'properties': start_properties})
                    table_set1["_fields"].append({'type': res["r1"].type})
                    table_set1["_fields"].append({'properties': end_properties})
                    table.append(table_set1)

    sum_list['nodes'] = nodes
    sum_list['links'] = links
    sum_list['table'] = table
    session.close()

    # return json.dumps(sum_list, ensure_ascii=False)
    if len(sum_list['nodes']) <= 0:
        return "查询不到"
    else:
        return json.dumps(sum_list, ensure_ascii=False)
# # coding:utf-8
# import re
# import codecs, sys
# from collections import Counter
# import json
# import csv
# import pymysql
# import sys
# import time
# from urllib.parse import quote, unquote
# import random
# import zhconv
# import argparse
# from neo4j import GraphDatabase
# import json
# import requests
# import pymysql
# import time
# from py2neo import *
# from py2neo.matching import *
#
# def search_category(name):
#
#
#     url = "http://zhonghuapu.com:8095/entity/alias2entities"
#
#     params = {
#         # "entity_name": "合肥工业大学",
#         "alias_name": name
#     }
#     response = requests.post(url, data=params)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         return "请求失败"
# def export_content_file(content,driver,session,sum_list):
#     nodeid_list = []  # 用于去重
#     linkid_list = []  # 用于去重
#     nodes = []
#     links = []
#     table = []
#     query_word = """
#                 MATCH (h)-[r]-(t)-[r1]-(l) WHERE  ((h.name={arg_1} and h:wikibaike) or (h.name={arg_1} and h:baidu_directory))
#                 and (t:hypernode) and (l:baidupage or l:wikipage)
#                 RETURN h,r,t,r1,l LIMIT 50
#
#         """
#     # query_word = '''
#     #
#     #             MATCH (h)-[r]-(t) WHERE (t.name={arg_2}) or (id(h) = {arg_1} and t:wikibaike) or (id(h) = {arg_1} and t:baidu_directory)
#     #              or (id(h) = {arg_1} and t:baidupage) or (id(h) = {arg_1} and t:wikipage) RETURN h,r,t LIMIT 100
#     #             '''
#     result = session.run(query_word, parameters={"arg_1": content})
#     for res in result:
#         # print(res["h"])
#         if res["h"] is not None:
#             if res['h'].id not in nodeid_list:
#                 node_set = {}
#
#                 nodeid_list.append(res['h'].id)
#                 node_set["id"] = str(res['h'].id)
#                 node_set["value"] = 1
#                 a = res['t']
#                 node_set["name"] = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#
#                 node_set["symbolSize"] = 30
#                 # node_set["labels"] = str(res['h'].labels)
#                 if 'Guobiao2017NES' in list(set(res['h'].labels)):
#                     node_set['category'] = 1
#                 elif 'SuperCategory' in list(set(res['h'].labels)):
#                     node_set['category'] = 2
#                 elif 'wikibaike' in list(set(res['h'].labels)):
#                     node_set['category'] = 3
#                 elif 'baidu_directory' in list(set(res['h'].labels)):
#                     node_set['category'] = 4
#                 elif 'hypernode' in list(set(res['h'].labels)):
#                     node_set['category'] = 5
#                 elif 'baidupage' in list(set(res['h'].labels)):
#                     node_set['category'] = 6
#                 elif 'wikipage' in list(set(res['h'].labels)):
#                     node_set['category'] = 7
#                 else:
#                     node_set['category'] = 0
#                 node_set['properties'] = res['h']._properties
#                 if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set)
#
#         if res["t"] is not None:
#             if res['t'].id not in nodeid_list:
#                 node_set1 = {}
#                 nodeid_list.append(res['t'].id)
#                 node_set1["id"] = str(res['t'].id)
#                 node_set1["value"] = 1
#                 node_set1["name"] = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_set1["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['t'].labels)):
#                     node_set1['category'] = 1
#                 elif 'SuperCategory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 2
#                 elif 'wikibaike' in list(set(res['t'].labels)):
#                     node_set1['category'] = 3
#                 elif 'baidu_directory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 4
#                 elif 'hypernode' in list(set(res['t'].labels)):
#                     node_set1['category'] = 5
#                 elif 'baidupage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 6
#                 elif 'wikipage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 7
#                 else:
#                     node_set1['category'] = 0
#                 node_set1['properties'] = res['t']._properties
#                 if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set1)
#         if res["l"] is not None:
#             if res['l'].id not in nodeid_list:
#                 node_set2 = {}
#                 nodeid_list.append(res['l'].id)
#                 node_set2["id"] = str(res['l'].id)
#                 node_set2["value"] = 1
#                 node_set2["name"] = zhconv.convert(res['l']._properties["name"], 'zh-cn')
#                 node_set2["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['l'].labels)):
#                     node_set2['category'] = 1
#                 elif 'SuperCategory' in list(set(res['l'].labels)):
#                     node_set2['category'] = 2
#                 elif 'wikibaike' in list(set(res['l'].labels)):
#                     node_set2['category'] = 3
#                 elif 'baidu_directory' in list(set(res['l'].labels)):
#                     node_set2['category'] = 4
#                 elif 'hypernode' in list(set(res['l'].labels)):
#                     node_set2['category'] = 5
#                 elif 'baidupage' in list(set(res['l'].labels)):
#                     node_set2['category'] = 6
#                 elif 'wikipage' in list(set(res['l'].labels)):
#                     node_set2['category'] = 7
#                 else:
#                     node_set2['category'] = 0
#                 node_set2['properties'] = res['l']._properties
#                 if len(node_set2) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set2)
#         link_set = {}
#         table_set = {}
#         if res["r"] is not None:
#             if res['r'].id not in linkid_list:
#
#                 startNodeId = str(res["r"].nodes[0].id)
#                 endNodeId = str(res["r"].nodes[1].id)
#                 for node in nodes:
#                     if startNodeId == node["id"]:
#                         link_set["target"] = startNodeId
#                     if endNodeId == node["id"]:
#                         link_set["source"] = endNodeId
#                     link_set["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
#                 if len(link_set) != 0:
#                     # print(link_set)
#                     links.append(link_set)
#                     print(links)
#
#                 table_set["_fields"] = []
#                 start_properties = res["r"].nodes[0]._properties
#                 end_properties = res["r"].nodes[1]._properties
#                 start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                 end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                 table_set["_fields"].append({'properties': start_properties})
#                 table_set["_fields"].append({'type': res["r"].type})
#                 table_set["_fields"].append({'properties': end_properties})
#                 table.append(table_set)
#         link_set1 = {}
#         table_set1 = {}
#         if res["r1"] is not None:
#             if res['r1'].id not in linkid_list:
#
#                 startNodeId = str(res["r1"].nodes[0].id)
#                 endNodeId = str(res["r1"].nodes[1].id)
#                 for node in nodes:
#                     if startNodeId == node["id"]:
#                         link_set1["target"] = startNodeId
#                     if endNodeId == node["id"]:
#                         link_set1["source"] = endNodeId
#                     link_set1["label"] = {"show": True, "formatter": res["r1"].type, "fontSize": 12}
#                 if len(link_set1) != 0:
#                     links.append(link_set1)
#
#                 table_set1["_fields"] = []
#                 start_properties = res["r1"].nodes[0]._properties
#                 end_properties = res["r1"].nodes[1]._properties
#                 start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                 end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                 table_set1["_fields"].append({'properties': start_properties})
#                 table_set1["_fields"].append({'type': res["r1"].type})
#                 table_set1["_fields"].append({'properties': end_properties})
#                 table.append(table_set1)
#     # print(nodes)
#     # # print(len(links))
#     # print(links)
#     # print(table)
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#
#     return json.dumps(sum_list, ensure_ascii=False)
# def main(request):
#     name = request.GET["name"]
#     indexlevel = request.GET["index"]
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
#     session = driver.session()
#     nodeid_list = []  # 用于去重
#     linkid_list = []  # 用于去重
#     sum_list = {}  # 总数据
#     sum_list["type"] = "force"
#
#     sum_list["categories"] = [
#         {
#             "name": "其他",
#             "keyword": {},
#             "base": "HTMLElement"
#         },
#         {
#             "name": "国标目录",
#             "keyword": {},
#             "base": "SVGElement",
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgba(255,186,44,0.95)'  # rgba(255,186,44,0.95)
#                 }
#             }
#         },
#         {
#             "name": "超级目录",
#             "keyword": {},
#             "base": "CSSRule",
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(236,147,158)'
#                 }
#             }
#         },
#         {
#             "name": "维基目录",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(100,176,100)'
#                 }
#             }
#         },
#
#         {
#             "name": "百度目录",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(52,176,143)'
#                 }
#             }
#         },
#         {
#             "name": "超点",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": '#FF7F00'
#                 }
#             }
#         },
#         {
#             "name": "百度页面",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": '#EAADEA'
#                 }
#             }
#         },
#         {
#             "name": "维基页面",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(36,100,143)'
#                 }
#             }
#         },
#
#     ]
#     nodes = []
#     links = []
#     table = []
#     # 查询知海
#     name_list=search_category(name)
#     if name_list=="请求失败":
#         sum_list['nodes'] = []
#         sum_list['links'] = []
#         sum_list['table'] = []
#         return json.dumps(sum_list, ensure_ascii=False)
#     else:
#         name_list=name_list["data"]
#     if len(name_list)<=0:
#         message=export_content_file(name,driver,session,sum_list)
#         return message
#     id_list=[]
#     for i in name_list:
#         if "graphId" not in i:
#             continue
#         if i["graphId"]==None:
#             continue
#         id_list.append(int(i["graphId"]))
#     all_result=[]
#     print(id_list)
#     for i in id_list:
#         query_word = '''
#
#             MATCH (h)-[r]-(t) WHERE (h:c_hypernode) and (id(h) = {arg_1} and t:wikibaike) or (id(h) = {arg_1} and t:baidu_directory)
#              or (id(h) = {arg_1} and t:baidupage) or (id(h) = {arg_1} and t:wikipage) RETURN h,r,t LIMIT 100
#             '''
#         # query_word = '''
#         #
#         #             MATCH (h)-[r]-(t) WHERE (t.name={arg_2}) or (id(h) = {arg_1} and t:wikibaike) or (id(h) = {arg_1} and t:baidu_directory)
#         #              or (id(h) = {arg_1} and t:baidupage) or (id(h) = {arg_1} and t:wikipage) RETURN h,r,t LIMIT 100
#         #             '''
#         results_place = session.run(query_word, parameters={"arg_1": i})
#         all_result.append(results_place)
#     for result in all_result:
#         print(result)
#         for res in result:
#             if res["h"] is not None:
#                 if res['h'].id not in nodeid_list:
#                     node_set = {}
#
#                     nodeid_list.append(res['h'].id)
#                     node_set["id"] = str(res['h'].id)
#                     node_set["value"] = 1
#                     a = res['t']
#                     node_set["name"] = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#
#                     node_set["symbolSize"] = 30
#                     # node_set["labels"] = str(res['h'].labels)
#                     if 'Guobiao2017NES' in list(set(res['h'].labels)):
#                         node_set['category'] = 1
#                     elif 'SuperCategory' in list(set(res['h'].labels)):
#                         node_set['category'] = 2
#                     elif 'wikibaike' in list(set(res['h'].labels)):
#                         node_set['category'] = 3
#                     elif 'baidu_directory' in list(set(res['h'].labels)):
#                         node_set['category'] = 4
#                     elif 'hypernode' in list(set(res['h'].labels)):
#                         node_set['category'] = 5
#                     elif 'baidupage' in list(set(res['h'].labels)):
#                         node_set['category'] = 6
#                     elif 'wikipage' in list(set(res['h'].labels)):
#                         node_set['category'] = 7
#                     else:
#                         node_set['category'] = 0
#                     node_set['properties'] = res['h']._properties
#                     if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                         nodes.append(node_set)
#
#             if res["t"] is not None:
#                 if res['t'].id not in nodeid_list:
#                     node_set1 = {}
#                     nodeid_list.append(res['t'].id)
#                     node_set1["id"] = str(res['t'].id)
#                     node_set1["value"] = 1
#                     node_set1["name"] = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                     node_set1["symbolSize"] = 30
#                     if 'Guobiao2017NES' in list(set(res['t'].labels)):
#                         node_set1['category'] = 1
#                     elif 'SuperCategory' in list(set(res['t'].labels)):
#                         node_set1['category'] = 2
#                     elif 'wikibaike' in list(set(res['t'].labels)):
#                         node_set1['category'] = 3
#                     elif 'baidu_directory' in list(set(res['t'].labels)):
#                         node_set1['category'] = 4
#                     elif 'hypernode' in list(set(res['t'].labels)):
#                         node_set1['category'] = 5
#                     elif 'baidupage' in list(set(res['t'].labels)):
#                         node_set1['category'] = 6
#                     elif 'wikipage' in list(set(res['t'].labels)):
#                         node_set1['category'] = 7
#                     else:
#                         node_set1['category'] = 0
#                     node_set1['properties'] = res['t']._properties
#                     if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                         nodes.append(node_set1)
#             link_set = {}
#             table_set = {}
#             if res["r"] is not None:
#                 if res['r'].id not in linkid_list:
#
#                     startNodeId = str(res["r"].nodes[0].id)
#                     endNodeId = str(res["r"].nodes[1].id)
#                     for node in nodes:
#                         if startNodeId == node["id"]:
#                             link_set["target"] = startNodeId
#                         if endNodeId == node["id"]:
#                             link_set["source"] = endNodeId
#                         link_set["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
#                     if len(link_set) != 0:
#                         links.append(link_set)
#
#                     table_set["_fields"] = []
#                     start_properties = res["r"].nodes[0]._properties
#                     end_properties = res["r"].nodes[1]._properties
#                     start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                     end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                     table_set["_fields"].append({'properties': start_properties})
#                     table_set["_fields"].append({'type': res["r"].type})
#                     table_set["_fields"].append({'properties': end_properties})
#                     table.append(table_set)
#
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#
#     # return json.dumps(sum_list, ensure_ascii=False)
#     if len(sum_list['nodes'])<=0:
#         return "查询不到"
#     else:
#         return json.dumps(sum_list, ensure_ascii=False)


# # coding:utf-8
# import re
# import codecs, sys
# from collections import Counter
# import json
# import csv
# import pymysql
# import sys
# import time
# from urllib.parse import quote, unquote
# import random
# import zhconv
# import argparse
# from neo4j import GraphDatabase
# import json
# import pymysql
# import time
# from py2neo import *
# from py2neo.matching import *
#
#
# def main(request):
#     name = request.GET["name"]
#     indexlevel = request.GET["index"]
#     driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
#     # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
#     session = driver.session()
#     nodeid_list = []  # 用于去重
#     linkid_list = []  # 用于去重
#     sum_list = {}  # 总数据
#     sum_list["type"] = "force"
#
#     sum_list["categories"] = [
#         {
#             "name": "其他",
#             "keyword": {},
#             "base": "HTMLElement"
#         },
#         {
#             "name": "国标目录",
#             "keyword": {},
#             "base": "SVGElement",
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgba(255,186,44,0.95)'  # rgba(255,186,44,0.95)
#                 }
#             }
#         },
#         {
#             "name": "超级目录",
#             "keyword": {},
#             "base": "CSSRule",
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(236,147,158)'
#                 }
#             }
#         },
#         {
#             "name": "维基目录",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(100,176,100)'
#                 }
#             }
#         },
#
#         {
#             "name": "百度目录",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(52,176,143)'
#                 }
#             }
#         },
#         {
#             "name": "超点",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": '#FF7F00'
#                 }
#             }
#         },
#         {
#             "name": "百度页面",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": '#EAADEA'
#                 }
#             }
#         },
#         {
#             "name": "维基页面",
#             "keyword": {},
#             "itemStyle": {
#                 "normal": {
#                     "color": 'rgb(36,100,143)'
#                 }
#             }
#         },
#
#     ]
#     nodes = []
#     links = []
#     table = []
#     # 查询知海
#
#     query_word = '''
#         MATCH (h:hypernode)<-[r:Superedge]-(t) WHERE (h.name={arg_1} and t:wikibaike) or (h.name={arg_1} and t:baidu_directory)
#          or (h.name={arg_1} and t:baidupage) or (h.name={arg_1} and t:wikipage) RETURN h,r,t LIMIT 100 union all
#
#
#         MATCH (h:hypernode)
#         where h.name ={arg_1}
#         CALL apoc.path.expandConfig(h,{
#         relationshipFilter: "<Superedge|<Contain",
#         labelFilter: ">wikibaike|>baidu_directory",
#
#
#
#
#         maxLevel: {arg_2},
#         limit:300
#         })
#         yield path
#         with relationships(path) as rels
#         unwind rels as rel
#         return startNode(rel) as h ,rel as r, endNode(rel) as t
#
#
#
#
#
#
#         '''
#     results_place = session.run(query_word, parameters={"arg_1": name, "arg_2": indexlevel})
#     for res in results_place:
#         if res["h"] is not None:
#             if res['h'].id not in nodeid_list:
#                 node_set = {}
#
#                 nodeid_list.append(res['h'].id)
#                 node_set["id"] = str(res['h'].id)
#                 node_set["value"] = 1
#                 a = res['t']
#                 node_set["name"] = zhconv.convert(res['h']._properties["name"], 'zh-cn')
#
#                 node_set["symbolSize"] = 30
#                 # node_set["labels"] = str(res['h'].labels)
#                 if 'Guobiao2017NES' in list(set(res['h'].labels)):
#                     node_set['category'] = 1
#                 elif 'SuperCategory' in list(set(res['h'].labels)):
#                     node_set['category'] = 2
#                 elif 'wikibaike' in list(set(res['h'].labels)):
#                     node_set['category'] = 3
#                 elif 'baidu_directory' in list(set(res['h'].labels)):
#                     node_set['category'] = 4
#                 elif 'hypernode' in list(set(res['h'].labels)):
#                     node_set['category'] = 5
#                 elif 'baidupage' in list(set(res['h'].labels)):
#                     node_set['category'] = 6
#                 elif 'wikipage' in list(set(res['h'].labels)):
#                     node_set['category'] = 7
#                 else:
#                     node_set['category'] = 0
#                 node_set['properties'] = res['h']._properties
#                 if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set)
#
#         if res["t"] is not None:
#             if res['t'].id not in nodeid_list:
#                 node_set1 = {}
#                 nodeid_list.append(res['t'].id)
#                 node_set1["id"] = str(res['t'].id)
#                 node_set1["value"] = 1
#                 node_set1["name"] = zhconv.convert(res['t']._properties["name"], 'zh-cn')
#                 node_set1["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['t'].labels)):
#                     node_set1['category'] = 1
#                 elif 'SuperCategory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 2
#                 elif 'wikibaike' in list(set(res['t'].labels)):
#                     node_set1['category'] = 3
#                 elif 'baidu_directory' in list(set(res['t'].labels)):
#                     node_set1['category'] = 4
#                 elif 'hypernode' in list(set(res['t'].labels)):
#                     node_set1['category'] = 5
#                 elif 'baidupage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 6
#                 elif 'wikipage' in list(set(res['t'].labels)):
#                     node_set1['category'] = 7
#                 else:
#                     node_set1['category'] = 0
#                 node_set1['properties'] = res['t']._properties
#                 if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set1)
#         link_set = {}
#         table_set = {}
#         if res["r"] is not None:
#             if res['r'].id not in linkid_list:
#
#                 startNodeId = str(res["r"].nodes[0].id)
#                 endNodeId = str(res["r"].nodes[1].id)
#                 for node in nodes:
#                     if startNodeId == node["id"]:
#                         link_set["target"] = startNodeId
#                     if endNodeId == node["id"]:
#                         link_set["source"] = endNodeId
#                     link_set["label"] = {"show": True, "formatter": res["r"].type, "fontSize": 12}
#                 if len(link_set) != 0:
#                     links.append(link_set)
#
#                 table_set["_fields"] = []
#                 start_properties = res["r"].nodes[0]._properties
#                 end_properties = res["r"].nodes[1]._properties
#                 start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#                 end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
#
#                 table_set["_fields"].append({'properties': start_properties})
#                 table_set["_fields"].append({'type': res["r"].type})
#                 table_set["_fields"].append({'properties': end_properties})
#                 table.append(table_set)
#
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#
#     return json.dumps(sum_list, ensure_ascii=False)

