# coding:utf-8
import zhconv
from neo4j import GraphDatabase
import json
import pymysql
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

    def insert_data(self, table_name, data):
        try:
            # 先检查主键是否存在
            primary_key = list(data.keys())[0]  # 假设主键在第一个位置
            primary_key_value = data[primary_key]

            # 生成检查主键是否存在的 SQL 查询
            check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s"
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value,))
                result = cursor.fetchone()

                if result[0] > 0:
                    print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
                    return  # 主键已存在，跳过插入操作

            # 生成插入 SQL 语句
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 执行插入操作
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print("数据插入成功！")
        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            self.connection.rollback()  # 回滚事务

    def insert_relation(self, table_name, data):
        try:
            # 先检查主键是否存在
            primary_key = list(data.keys())[0]  # 假设主键在第一个位置
            last_key = list(data.keys())[-1]
            primary_key_value = data[primary_key]
            last_key_value = data[last_key]

            # 生成检查主键是否存在的 SQL 查询
            check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s AND {last_key} = %s "
            with self.connection.cursor() as cursor:
                cursor.execute(check_query, (primary_key_value, last_key_value,))
                result = cursor.fetchone()

                if result[0] > 0:
                    print(f"主键 {primary_key_value} 已存在，跳过插入操作。")
                    return  # 主键已存在，跳过插入操作

            # 生成插入 SQL 语句
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # 执行插入操作
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, tuple(data.values()))
                self.connection.commit()
                print("数据插入成功！")
        except pymysql.MySQLError as e:
            print(f"插入数据失败：{e}")
            self.connection.rollback()  # 回滚事务

    def query_tables(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"查询失败：{e}")
            raise

    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭！")
def addNodes(res,nodeid_list,nodes):
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
        elif 'KOCategory' in list(set(res.labels)):
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
        else:
            node_set['category'] = 0
        node_set['properties'] = res._properties
        if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
            nodes.append(node_set)
    return nodeid_list,nodes
def add_lines(res,linkid_list,nodes,links,table):
    link_set = {}  # 易错点！！！这里要重命名一下！！！
    table_set = {}
    if res.id not in linkid_list:
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
    return linkid_list,links,table

def search_content_to_file(content,db):
    """
    根据给定的 content 查询 entity_to_file 表中的 file_id，
    并返回 file 表中所有匹配的行的信息。
    """
    query = """
    SELECT ef.sim, f.*
    FROM file AS f
    JOIN entity_to_file AS ef ON f.id = ef.file_id
    WHERE ef.entity = %s;
    """
    try:
        with db.connection.cursor() as cursor:
            cursor.execute(query, (content,))
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询失败：{e}")
        raise
def check_node_exists(name,node_id,userID,session):
    result = session.run(
        """
        MATCH (n)
        WHERE id(n) = $node_id and n.name=$name and n.user_id=$userID
        RETURN CASE WHEN COUNT(n) > 0 THEN 1 ELSE 0 END AS exists
        """,
        node_id=int(node_id),
        name=name,
        userID=int(userID)
    )
    return result.single()[0]
def main(request):
    name = request.GET["name"]
    userID = request.GET["index"]  # 和另一个共用一个，懒得改了，实际上就是用户ID
    driver = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    db = MySQLDatabase(
        host="114.213.234.179",
        user="koroot",  # 替换为您的用户名
        password="DMiC-4092",  # 替换为您的密码
        database="db_hp"  # 替换为您的数据库名
    )

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

    ]
    nodes = []
    links = []
    table = []
    # 查询知海
    query_word='''
    MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (h.name contains {arg_1} and (h:wikipage or h:baidupage or h:File or (h:Strict and h.user_id={arg_2})))
    RETURN h,r,t,r1,t1 LIMIT 300
    '''
    # query_word = '''
    #
    #
    #      MATCH (t1:KOCategory)-[r1]-(h:hypernode)-[r]-(t) WHERE  (h.name CONTAINS {arg_1} and t:baidupage) or (h.name CONTAINS {arg_1} and t:wikipage)
    #      or (h.name CONTAINS {arg_1} and t:Strict and t.user_id={arg_2}) or (h.name CONTAINS {arg_1} and t:File)
    #      RETURN h,r,t,r1,t1 LIMIT 30
    #
    #
    #     '''
    print(name)
    print(userID)
    kocategory_id=[]
    results_place = session.run(query_word, parameters={"arg_1": name, "arg_2": int(userID)})
    for res in results_place:
        if res["h"] is not None:
            content_id=res["h"].id
            nodeid_list,nodes=addNodes(res["h"],nodeid_list,nodes)
        if res["t"] is not None:
            nodeid_list,nodes=addNodes(res["t"],nodeid_list,nodes)
        if res["t1"] is not None:
            if res["t1"].id not in nodeid_list:
                kocategory_id.append(res["t1"].id)
            nodeid_list, nodes = addNodes(res["t1"], nodeid_list, nodes)
        if res["r"] is not None:
            linkid_list,links,table=add_lines(res["r"],linkid_list,nodes,links,table)
        if res["r1"] is not None:
            linkid_list, links,table = add_lines(res["r1"], linkid_list, nodes, links, table)





    #下面这部分是处理mysql二级目录文件的部分
    db.connect()
    result = search_content_to_file(name, db)

    all_exatra_file_id = []  # 存放从mysql里面找到的二级文件
    if len(result) <= 0:
        pass
    else:
        for i in result:
            result = check_node_exists(i[2], i[1], userID, session)
            if ("1" in str(result)):
                all_exatra_file_id.append(i[1])
    for i1 in all_exatra_file_id:
        query_word = '''
            MATCH (h)<-[r]-(t:hypernode)<-[r1]-(t1:KOCategory) WHERE (id(h)={arg_1} and (h:wikipage or h:baidupage or h:File or (h:Strict and h.user_id={arg_2})))
            RETURN h,r,t,r1,t1 LIMIT 300
            '''
        results_place = session.run(query_word, parameters={"arg_1": int(i1), "arg_2": int(userID)})
        for res in results_place:
            if res["h"] is not None:
                content_id = res["h"].id
                nodeid_list, nodes = addNodes(res["h"], nodeid_list, nodes)
            if res["t"] is not None:
                nodeid_list, nodes = addNodes(res["t"], nodeid_list, nodes)
            if res["t1"] is not None:
                if res["t1"].id not in nodeid_list:
                    kocategory_id.append(res["t1"].id)
                nodeid_list, nodes = addNodes(res["t1"], nodeid_list, nodes)
            if res["r"] is not None:
                linkid_list, links, table = add_lines(res["r"], linkid_list, nodes, links, table)
            if res["r1"] is not None:
                linkid_list, links, table = add_lines(res["r1"], linkid_list, nodes, links, table)



    print(kocategory_id)
    sum=0
    for i in kocategory_id:
        sum+=1
        # if (sum>=5):
        #     break
        query_word = '''
                MATCH (h:KOCategory)<-[r]-(t) WHERE (id(h)={arg_1} and t:KOCategory) RETURN h,r,t LIMIT 100 union all


         MATCH (h:KOCategory)
       where id(h)={arg_1}
         CALL apoc.path.expandConfig(h,{
         relationshipFilter: "<edge",
         labelFilter: ">KOCategory",
         maxLevel: 5,
         limit:300,
         direction: "incoming"
         })
         yield path
         with relationships(path) as rels
         unwind rels as rel
         WITH rel, startNode(rel) AS t, endNode(rel) AS h
            WHERE h:KOCategory AND t <> h  // 确保 t 不是 h
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
    # print(sum_list['nodes'])

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
#
#
# def main(request):
#     name = request.GET["name"]
#     userID = request.GET["index"] #和另一个共用一个，懒得改了，实际上就是用户ID
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
#     query_word = '''
#
#
#          MATCH (t1:KOCategory)-[r1]-(h:hypernode)-[r]-(t) WHERE  (h.name CONTAINS {arg_1} and t:baidupage) or (h.name CONTAINS {arg_1} and t:wikipage)
#          or (h.name CONTAINS {arg_1} and t:Strict and t.user_id={arg_2})
#          RETURN h,r,t,r1,t1 LIMIT 30
#
#
#         '''
#     print(name)
#     print(userID)
#     results_place = session.run(query_word, parameters={"arg_1": name,"arg_2":int(userID)})
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = res['t']._properties["name"]
#                 if len(name_property)>16:
#                     name_property = name_property[:16]+"..."
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 node_set1["name"] = zhconv.convert(name_property, 'zh-cn')
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
#         if res["t1"] is not None:
#             if res['t1'].id not in nodeid_list:
#                 node_set2 = {}
#                 nodeid_list.append(res['t1'].id)
#                 node_set2["id"] = str(res['t1'].id)
#                 node_set2["value"] = 1
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = res['t1']._properties["name"]
#                 if len(name_property) > 16:
#                     name_property = name_property[:16] + "..."
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 node_set2["name"] = zhconv.convert(name_property, 'zh-cn')
#                 node_set2["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 1
#                 elif 'KOCategory' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 2
#                 elif 'wikibaike' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 3
#                 elif 'baidu_directory' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 4
#                 elif 'hypernode' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 5
#                 elif 'baidupage' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 6
#                 elif 'wikipage' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 7
#                 else:
#                     node_set2['category'] = 0
#                 node_set2['properties'] = res['t1']._properties
#                 if len(node_set2) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set2)
#         link_set = {}#易错点！！！这里要重命名一下！！！
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = end_properties["name"]
#
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 end_properties["name"] = zhconv.convert(name_property, 'zh-cn')#
#
#                 table_set["_fields"].append({'properties': start_properties})
#                 table_set["_fields"].append({'type': res["r"].type})
#                 table_set["_fields"].append({'properties': end_properties})
#                 table.append(table_set)
#
#         link_set1 = {}  # 易错点！！！这里要重命名一下！！！
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = end_properties["name"]
#
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 end_properties["name"] = zhconv.convert(name_property, 'zh-cn')  #
#
#                 table_set1["_fields"].append({'properties': start_properties})
#                 table_set1["_fields"].append({'type': res["r1"].type})
#                 table_set1["_fields"].append({'properties': end_properties})
#                 table.append(table_set1)
#
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#     print(sum_list['nodes'])
#
#     return json.dumps(sum_list, ensure_ascii=False)


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
#     userID = request.GET["index"] #和另一个共用一个，懒得改了，实际上就是用户ID
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
#     query_word = '''
#
#
#          MATCH (t1:KOCategory)-[r1]-(h:hypernode)-[r]-(t) WHERE  (h.name CONTAINS {arg_1} and t:baidupage) or (h.name CONTAINS {arg_1} and t:wikipage)
#          or (h.name CONTAINS {arg_1} and t:Strict and t.user_id={arg_2})
#          RETURN h,r,t,r1,t1 LIMIT 30
#
#
#         '''
#     print(name)
#     print(userID)
#     results_place = session.run(query_word, parameters={"arg_1": name,"arg_2":int(userID)})
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = res['t']._properties["name"]
#                 if len(name_property)>16:
#                     name_property = name_property[:16]+"..."
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 node_set1["name"] = zhconv.convert(name_property, 'zh-cn')
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
#         if res["t1"] is not None:
#             if res['t1'].id not in nodeid_list:
#                 node_set2 = {}
#                 nodeid_list.append(res['t1'].id)
#                 node_set2["id"] = str(res['t1'].id)
#                 node_set2["value"] = 1
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = res['t1']._properties["name"]
#                 if len(name_property) > 16:
#                     name_property = name_property[:16] + "..."
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 node_set2["name"] = zhconv.convert(name_property, 'zh-cn')
#                 node_set2["symbolSize"] = 30
#                 if 'Guobiao2017NES' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 1
#                 elif 'KOCategory' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 2
#                 elif 'wikibaike' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 3
#                 elif 'baidu_directory' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 4
#                 elif 'hypernode' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 5
#                 elif 'baidupage' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 6
#                 elif 'wikipage' in list(set(res['t1'].labels)):
#                     node_set2['category'] = 7
#                 else:
#                     node_set2['category'] = 0
#                 node_set2['properties'] = res['t1']._properties
#                 if len(node_set2) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
#                     nodes.append(node_set2)
#         link_set = {}#易错点！！！这里要重命名一下！！！
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = end_properties["name"]
#
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 end_properties["name"] = zhconv.convert(name_property, 'zh-cn')#
#
#                 table_set["_fields"].append({'properties': start_properties})
#                 table_set["_fields"].append({'type': res["r"].type})
#                 table_set["_fields"].append({'properties': end_properties})
#                 table.append(table_set)
#
#         link_set1 = {}  # 易错点！！！这里要重命名一下！！！
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
#
#                 # 假设 res['t']._properties["name"] 可能是一个列表
#                 name_property = end_properties["name"]
#
#                 # 确保它是字符串
#                 if isinstance(name_property, list):
#                     # 将所有元素转换为字符串并连接
#                     name_property = ''.join(str(ch) for ch in name_property)
#                 elif not isinstance(name_property, str):
#                     # 如果不是字符串，也不是列表，直接转换成字符串
#                     name_property = str(name_property)
#
#                 end_properties["name"] = zhconv.convert(name_property, 'zh-cn')  #
#
#                 table_set1["_fields"].append({'properties': start_properties})
#                 table_set1["_fields"].append({'type': res["r1"].type})
#                 table_set1["_fields"].append({'properties': end_properties})
#                 table.append(table_set1)
#
#     sum_list['nodes'] = nodes
#     sum_list['links'] = links
#     sum_list['table'] = table
#     session.close()
#     print(sum_list['nodes'])
#
#     return json.dumps(sum_list, ensure_ascii=False)
