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
import pymysql
import time
from py2neo import *
from py2neo.matching import *


def addNodes(res, nodeid_list, nodes):
    if res.id not in nodeid_list:
        node_set = {}
        nodeid_list.append(res.id)
        node_set["id"] = str(res.id)
        node_set["value"] = 1
        node_set["name"] = zhconv.convert(res._properties["name"], 'zh-cn')
        node_set["symbolSize"] = 30
        # node_set["labels"] = str(res['h'].labels)
        if 'Guobiao2017NES' in list(set(res.labels)):
            node_set['category'] = 1
        elif 'KOCategory' in list(set(res.labels)) or 'coarse' in list(set(res.labels)):
            node_set['category'] = 2
        elif 'wikibaike' in list(set(res.labels)):
            node_set['category'] = 3
        elif 'baidu_directory' in list(set(res.labels)):
            node_set['category'] = 4
        elif 'hypernode' in list(set(res.labels)):
            node_set['category'] = 5
        elif 'baidupage' in list(set(res.labels)):
            node_set['category'] = 6
        elif 'wikipage' in list(set(res.labels)):
            node_set['category'] = 7
        elif 'Strict' in list(set(res.labels)):
            node_set['category'] = 8
        else:
            node_set['category'] = 0
        node_set['properties'] = res._properties
        if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
            nodes.append(node_set)
    return nodeid_list, nodes


def add_lines(res, linkid_list, nodes, links, table):
    link_set = {}  # 易错点！！！这里要重命名一下！！！
    table_set = {}
    if res.id not in linkid_list:
        linkid_list.append(res.id)
        startNodeId = str(res.nodes[0].id)
        endNodeId = str(res.nodes[1].id)
        for node in nodes:
            if startNodeId == node["id"]:
                link_set["target"] = startNodeId
            if endNodeId == node["id"]:
                link_set["source"] = endNodeId
            link_set["label"] = {"show": True, "formatter": res.type, "fontSize": 12}
        if len(link_set) != 0:
            links.append(link_set)

        table_set["_fields"] = []
        start_properties = res.nodes[0]._properties
        end_properties = res.nodes[1]._properties
        start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')

        # 假设 res['t']._properties["name"] 可能是一个列表
        name_property = end_properties["name"]

        # 确保它是字符串
        if isinstance(name_property, list):
            # 将所有元素转换为字符串并连接
            name_property = ''.join(str(ch) for ch in name_property)
        elif not isinstance(name_property, str):
            # 如果不是字符串，也不是列表，直接转换成字符串
            name_property = str(name_property)

        end_properties["name"] = zhconv.convert(name_property, 'zh-cn')  #

        table_set["_fields"].append({'properties': start_properties})
        table_set["_fields"].append({'type': res.type})
        table_set["_fields"].append({'properties': end_properties})
        table.append(table_set)
    return linkid_list, links, table


def main(request):
    node_id = request.GET["node_ID"]
    userID = request.GET["index"]  # 和另一个共用一个，懒得改了，实际上就是用户ID
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
                    "color": 'rgba(255,186,44,0.95)'
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
        {
            "name": "个人文件",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(36,120,143)'
                }
            }
        },
        {
            "name": "粗粒度目录",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(239,147,161)'
                }
            }
        },
        {
            "name": "文件名",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(37,120,140)'
                }
            }
        },

    ]
    nodes = []
    links = []
    table = []
    # 查询知海
    nodeIDs = str(node_id).split(",")
    if nodeIDs == ['']:
        sum_list['nodes'] = nodes
        sum_list['links'] = links
        sum_list['table'] = table
        return json.dumps(sum_list, ensure_ascii=False)
    kocategory_id = []
    print(nodeIDs)
    for node_id in nodeIDs:
        query_word = '''


             MATCH (h)-[r]-(t) WHERE (id(h)={arg_1} and (t:coarse))
             RETURN h,r,t LIMIT 100


            '''
        print(node_id)
        print(userID)

        results_place = session.run(query_word, parameters={"arg_1": int(node_id)})
        for res in results_place:
            if res["h"] is not None:
                content_id = res["h"].id
                nodeid_list, nodes = addNodes(res["h"], nodeid_list, nodes)
            if res["t"] is not None:
                nodeid_list, nodes = addNodes(res["t"], nodeid_list, nodes)
                kocategory_id.append(res["t"].id)
            if res["r"] is not None:
                linkid_list, links, table = add_lines(res["r"], linkid_list, nodes, links, table)

    # sum=0
    print(kocategory_id)
    for i in kocategory_id:
        # sum+=1
        # if (sum>=5):
        #     break
        query_word = '''
                MATCH (h:coarse)<-[r]-(t) WHERE (id(h)={arg_1} and t:coarse) RETURN h,r,t LIMIT 300 union all


         MATCH (h:coarse)
       where id(h)={arg_1}
         CALL apoc.path.expandConfig(h,{
         relationshipFilter: "<edge",
         labelFilter: ">coarse",
         maxLevel: 7,
         limit:300,
         direction: "incoming"
         })
         yield path
         with relationships(path) as rels
         unwind rels as rel
         WITH rel, startNode(rel) AS t, endNode(rel) AS h
            WHERE h:coarse AND t <> h  // 确保 t 不是 h
         return startNode(rel) as h ,rel as r, endNode(rel) as t
    '''
        results_place = session.run(query_word, parameters={"arg_1": int(i)})
        for res in results_place:
            if res["h"] is not None:
                content_id = res["h"].id
                nodeid_list, nodes = addNodes(res["h"], nodeid_list, nodes)
            if res["t"] is not None:
                nodeid_list, nodes = addNodes(res["t"], nodeid_list, nodes)
            # if res["t1"] is not None:
            #     if res["t1"].id not in nodeid_list:
            #         kocategory_id.append(res["t1"].id)
            #     nodeid_list, nodes = addNodes(res["t1"], nodeid_list, nodes)
            if res["r"] is not None:
                linkid_list, links, table = add_lines(res["r"], linkid_list, nodes, links, table)

            # if res["r1"] is not None:
            #     linkid_list, links, table = add_lines(res["r1"], linkid_list, nodes, links, table)

    sum_list['nodes'] = nodes
    sum_list['links'] = links
    sum_list['table'] = table
    session.close()
    print(sum_list['nodes'])

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
# import pymysql
# import time
# from py2neo import *
# from py2neo.matching import *
# def addNodes(res,nodeid_list,nodes):
#     if res.id not in nodeid_list:
#         node_set = {}
#         nodeid_list.append(res.id)
#         node_set["id"] = str(res.id)
#         node_set["value"] = 1
#         node_set["name"] = zhconv.convert(res._properties["name"], 'zh-cn')
#         node_set["symbolSize"] = 30
#         # node_set["labels"] = str(res['h'].labels)
#         if 'Guobiao2017NES' in list(set(res.labels)):
#             node_set['category'] = 1
#         elif 'KOCategory' in list(set(res.labels)):
#             node_set['category'] = 2
#         elif 'wikibaike' in list(set(res.labels)):
#             node_set['category'] = 3
#         elif 'baidu_directory' in list(set(res.labels)):
#             node_set['category'] = 4
#         elif 'hypernode' in list(set(res.labels)):
#             node_set['category'] = 5
#         elif 'baidupage' in list(set(res.labels)):
#             node_set['category'] = 6
#         elif 'wikipage' in list(set(res.labels)):
#             node_set['category'] = 7
#         else:
#             node_set['category'] = 0
#         node_set['properties'] = res._properties
#         if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#             nodes.append(node_set)
#     return nodeid_list,nodes
# def add_lines(res,linkid_list,nodes,links,table):
#     link_set = {}  # 易错点！！！这里要重命名一下！！！
#     table_set = {}
#     if res.id not in linkid_list:
#         linkid_list.append(res.id)
#         startNodeId = str(res.nodes[0].id)
#         endNodeId = str(res.nodes[1].id)
#         for node in nodes:
#             if startNodeId == node["id"]:
#                 link_set["target"] = startNodeId
#             if endNodeId == node["id"]:
#                 link_set["source"] = endNodeId
#             link_set["label"] = {"show": True, "formatter": res.type, "fontSize": 12}
#         if len(link_set) != 0:
#             links.append(link_set)
#
#         table_set["_fields"] = []
#         start_properties = res.nodes[0]._properties
#         end_properties = res.nodes[1]._properties
#         start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
#
#         # 假设 res['t']._properties["name"] 可能是一个列表
#         name_property = end_properties["name"]
#
#         # 确保它是字符串
#         if isinstance(name_property, list):
#             # 将所有元素转换为字符串并连接
#             name_property = ''.join(str(ch) for ch in name_property)
#         elif not isinstance(name_property, str):
#             # 如果不是字符串，也不是列表，直接转换成字符串
#             name_property = str(name_property)
#
#         end_properties["name"] = zhconv.convert(name_property, 'zh-cn')  #
#
#         table_set["_fields"].append({'properties': start_properties})
#         table_set["_fields"].append({'type': res.type})
#         table_set["_fields"].append({'properties': end_properties})
#         table.append(table_set)
#     return linkid_list,links,table
#
# def main(request):
#     node_id = request.GET["node_ID"]
#     userID = request.GET["index"]  # 和另一个共用一个，懒得改了，实际上就是用户ID
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
#                     "color": 'rgba(255,186,44,0.95)'
#                 }
#             }
#         },
#         {
#             "name": "目录",
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
#     nodeIDs=str(node_id).split(",")
#     if nodeIDs==['']:
#         sum_list['nodes'] = nodes
#         sum_list['links'] = links
#         sum_list['table'] = table
#         return json.dumps(sum_list, ensure_ascii=False)
#     kocategory_id = []
#     for node_id in nodeIDs:
#         query_word = '''
#
#
#              MATCH (h)-[r]-(t:hypernode)-[r1]-(t1) WHERE (id(h)={arg_1} and (t1:KOCategory))
#              RETURN h,r,t,r1,t1 LIMIT 100
#
#
#             '''
#         print(node_id)
#         print(userID)
#
#         results_place = session.run(query_word, parameters={"arg_1": int(node_id), "arg_2": int(userID)})
#         for res in results_place:
#             if res["h"] is not None:
#                 content_id = res["h"].id
#                 nodeid_list, nodes = addNodes(res["h"], nodeid_list, nodes)
#             if res["t"] is not None:
#                 nodeid_list, nodes = addNodes(res["t"], nodeid_list, nodes)
#             if res["t1"] is not None:
#                 if res["t1"].id not in nodeid_list:
#                     kocategory_id.append(res["t1"].id)
#                 nodeid_list, nodes = addNodes(res["t1"], nodeid_list, nodes)
#             if res["r"] is not None:
#                 linkid_list, links, table = add_lines(res["r"], linkid_list, nodes, links, table)
#             if res["r1"] is not None:
#                 linkid_list, links, table = add_lines(res["r1"], linkid_list, nodes, links, table)
#
#     # sum=0
#     print(kocategory_id)
#     for i in kocategory_id:
#         # sum+=1
#         # if (sum>=5):
#         #     break
#         query_word = '''
#                 MATCH (h:KOCategory)<-[r]-(t) WHERE (id(h)={arg_1} and t:KOCategory) RETURN h,r,t LIMIT 300 union all
#
#
#          MATCH (h:KOCategory)
#        where id(h)={arg_1}
#          CALL apoc.path.expandConfig(h,{
#          relationshipFilter: "<edge",
#          labelFilter: ">KOCategory",
#          maxLevel: 7,
#          limit:300,
#          direction: "incoming"
#          })
#          yield path
#          with relationships(path) as rels
#          unwind rels as rel
#          WITH rel, startNode(rel) AS t, endNode(rel) AS h
#             WHERE h:KOCategory AND t <> h  // 确保 t 不是 h
#          return startNode(rel) as h ,rel as r, endNode(rel) as t
#     '''
#         results_place = session.run(query_word, parameters={"arg_1": int(i)})
#         for res in results_place:
#             if res["h"] is not None:
#                 content_id = res["h"].id
#                 nodeid_list, nodes = addNodes(res["h"], nodeid_list, nodes)
#             if res["t"] is not None:
#                 nodeid_list, nodes = addNodes(res["t"], nodeid_list, nodes)
#             # if res["t1"] is not None:
#             #     if res["t1"].id not in nodeid_list:
#             #         kocategory_id.append(res["t1"].id)
#             #     nodeid_list, nodes = addNodes(res["t1"], nodeid_list, nodes)
#             if res["r"] is not None:
#                 linkid_list, links, table = add_lines(res["r"], linkid_list, nodes, links, table)
#             # if res["r1"] is not None:
#             #     linkid_list, links, table = add_lines(res["r1"], linkid_list, nodes, links, table)
#
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#     print(sum_list['nodes'])
#
#     return json.dumps(sum_list, ensure_ascii=False)
