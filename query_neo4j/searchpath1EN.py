# coding:utf-8
import re
import codecs, sys
from collections import Counter
import json
import csv
import pymysql
import sys
import time
import time
from func_timeout import func_set_timeout
import func_timeout
import pymysql
from urllib.parse import quote, unquote
import random
import zhconv
import argparse
from neo4j import GraphDatabase
import json
import pymysql
import time
db = pymysql.connect(host='www.zhonghuapu.com',
                     user='koroot',
                     password='DMiC-4092',
                     database='db_hp',
                     charset='utf8')

cursor = db.cursor()
cursor.execute("SELECT VERSION()")
def main(request):

    nodeid_list = []  # 用于去重
    linkid_list = []  # 用于去重
    sum_list = {}  # 总数据
    sum_list["type"]="force"
    sum_list["categories"]=[
        {
            "name": "Entry(Source 1)",
            "keyword": {},
            "base": "HTMLElement"
        },
        {
            "name": "Second-level Subjects",
            "keyword": {},
            "base": "WebGLRenderingContext"
        },
        {
            "name": "Other Subjects",
            "keyword": {},
            "base": "SVGElement",
            "itemStyle": {
                "normal": {
                    "color": 'rgba(255,186,44,0.95)'
                }
            }
        },
        {
            "name": "Root subjects",
            "keyword": {},
            "base": "CSSRule",
            "itemStyle": {
                "normal": {
                    "color": 'rgb(236,147,158)'
                }
            }
        },
        {
            "name": "Entry(Source 2)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(78,189,236)'
                }
            }
        },
        {
            "name": "First-level Subjects",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(52,176,143)'
                }
            }
        },
        {
            "name": "Entry(Source 3)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#91D8E4'  #5994a0
                }
            }
        },
        {
            "name": "Genealogy",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#408E91'  # 5994a0
                }
            }
        },
        {
            "name": "Private Node",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#738fa0'  # 5994a0
                }
            }
        },
        {
            "name": "Entry(Source 4)",
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
    flag=0

    driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
# driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))

    session1 = driver.session()
    session2 = driver.session()
    # 华谱连接
    driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    session_hp = driver_hp.session()

    name1 = request.GET["name1"]#起始节点
    name2 = request.GET["name2"]#结束节点
    name3 = request.GET["name3"]#中间节点

    showFlag=request.GET["showFlag"]
    hipFlag=request.GET["hipFlag"]
    startLens=request.GET["startLens"]
    endLens=request.GET["endLens"]

    resourceStart=request.GET["resourceStart"]
    resourceEnd=request.GET["resourceEnd"]
    resourceMiddle=request.GET["resourceMiddle"]
    if resourceStart=="知海" and resourceEnd=="知海":
        #不限制中间点和路径长度
        if showFlag=='false' and hipFlag=='false':
            session = driver.session()

            backquery_word = '''
            match (n1),(n2)
            where id(n1)='''+name1+''' and id(n2)='''+name2+''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50
    
            '''

            backresults_place = session.run(backquery_word)
            backres=manage(backresults_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
            session.close()
            return json.dumps(backres, ensure_ascii=False)
        #限制中间点 不限制路径长度
        elif showFlag=='true' and hipFlag=='false':

            try:
                #查找路径含有中间节点
                res=search(session2,name1,name2,name3,sum_list,nodeid_list,nodes,linkid_list,links,flag)

                return json.dumps(res, ensure_ascii=False)
            except (func_timeout.exceptions.FunctionTimedOut,ValueError) as e:
                flag=1
                backquery_word1 = '''
            match (n1),(n2)
            where id(n1)='''+name1+''' and id(n2)='''+name2+''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50
                '''
                backresults_place = session1.run(backquery_word1, parameters={"arg_1": name1,"arg_2": name2})
                backres=manage(backresults_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
                session1.close()
                backres['flag']=flag
                return json.dumps(backres, ensure_ascii=False)
        #不限制中间点 限制路径长度
        elif showFlag=='false' and hipFlag=='true':
            try:
                #查找路径含有中间节点
                res=searchhip(session2,name1,name2,startLens,endLens,sum_list,nodeid_list,nodes,linkid_list,links,flag)
                return json.dumps(res, ensure_ascii=False)
            except (func_timeout.exceptions.FunctionTimedOut,ValueError) as e:
                flag=1
                backquery_word1 = '''
            match (n1),(n2)
            where id(n1)='''+name1+''' and id(n2)='''+name2+''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50
    
                '''
                backresults_place = session1.run(backquery_word1, parameters={"arg_1": name1,"arg_2": name2})
                backres=manage(backresults_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
                session1.close()
                backres['flag']=flag
                return json.dumps(backres, ensure_ascii=False)
        #限制中间点 限制路径长度
        else:
            try:
                #查找路径含有中间节点
                res=searchhipshow(session2,name1,name2,name3,startLens,endLens,sum_list,nodeid_list,nodes,linkid_list,links,flag)
                return json.dumps(res, ensure_ascii=False)
            except (func_timeout.exceptions.FunctionTimedOut,ValueError) as e:
                flag=1
                backquery_word1 = '''
            match (n1),(n2)
            where id(n1)='''+name1+''' and id(n2)='''+name2+''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50
    
                '''
                backresults_place = session1.run(backquery_word1, parameters={"arg_1": name1,"arg_2": name2})
                backres=manage(backresults_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
                session1.close()
                backres['flag']=flag
                return json.dumps(backres, ensure_ascii=False)
    elif resourceStart=="华谱" and resourceEnd=="华谱":


        backquery_word = '''
            match (n1),(n2)
            where id(n1)='''+name1+''' and id(n2)='''+name2+''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50
    
            '''

        backresults_place = session_hp.run(backquery_word)
        backres=manageHp(backresults_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
        session_hp.close()

        return json.dumps(backres, ensure_ascii=False)
        # print(123)
        # return json.dumps([], ensure_ascii=False)
    elif resourceStart == "华谱" and resourceEnd == "知海":
        # 先从知海数据库(中文维基)中查到对应节点的id,将新id和另一节点的知海id传入语句
        searchName=request.GET["nameStart"]
        searchName=searchName.split("(")[0]

        queryword = '''match(h:Wikipedia) where h.name="''' + searchName + '''\" return id(h) as id  limit 1'''
        results_id = session1.run(queryword)
        if results_id.peek() is not None:
            new_id=str(results_id.single()[0])

            session = driver.session()

            backquery_word = '''
            match (n1),(n2)
            where id(n1)=''' + new_id + ''' and id(n2)=''' + name2 + ''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50

            '''
            backresults_place = session.run(backquery_word)
            backres = manage(backresults_place, sum_list, nodeid_list, nodes, linkid_list, links, flag)
            session.close()
            # 将华普节点加入
            HpNode=searchHpById(name1,session_hp)

            backres['nodes'].append(HpNode)

            HpLink={}
            HpLink["target"]=HpNode["id"]
            HpLink["source"]=new_id
            HpLink["label"]= {"show":True,"formatter":"SameAs","fontSize":12}
            backres['links'].append(HpLink)


            return json.dumps(backres, ensure_ascii=False)
        else:#知海中没有对应的名字
            return json.dumps(sum_list, ensure_ascii=False)
    else:#知海->华谱
        # 先从知海数据库(中文维基)中查到对应节点的id,将新id和另一节点的知海id传入语句
        searchName=request.GET["nameEnd"]
        searchName=searchName.split("(")[0]

        queryword = '''match(h:Wikipedia) where h.name="''' + searchName + '''\" return id(h) as id  limit 1'''
        results_id = session1.run(queryword)
        if results_id.peek() is not None:
            new_id=str(results_id.single()[0])

            session = driver.session()
            backquery_word = '''
            match (n1),(n2)
            where id(n1)=''' + name1 + ''' and id(n2)=''' + new_id + ''' 
            with n1,n2 limit 1
            match p = shortestPath((n1) - [*] - (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN node, rel limit 50

            '''
            backresults_place = session.run(backquery_word)
            backres = manage(backresults_place, sum_list, nodeid_list, nodes, linkid_list, links, flag)
            session.close()
            # 将华普节点加入
            HpNode=searchHpById(name2,session_hp)
            backres['nodes'].append(HpNode)

            HpLink={}
            HpLink["target"]=HpNode["id"]
            HpLink["source"]=new_id
            HpLink["label"]= {"show":True,"formatter":"SameAs","fontSize":12}
            backres['links'].append(HpLink)
            return json.dumps(backres, ensure_ascii=False)
        else:#知海中没有对应的名字
            return json.dumps(sum_list, ensure_ascii=False)



@func_set_timeout(15)
def search(session2,name1,name2,name3,sum_list,nodeid_list,nodes,linkid_list,links,flag):

    #查找路径含有中间节点
    query_word_middle='''
    MATCH (start), (end), (via)
    where id(start)='''+name1+''' and id(end)='''+name2+''' and id(via)='''+name3+'''
    with start,end,via limit 1
    MATCH path = shortestPath((start)-[*]-(end))
    where via IN nodes(path) AND via <> start AND via <> end
    UNWIND nodes(path) AS node
    UNWIND relationships(path) AS rel
    RETURN node, rel

    '''

    results_place = session2.run(query_word_middle)

    res=manage(results_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
    session2.close()
    return res




@func_set_timeout(15)
def searchhip(session2,name1,name2,startLens,endLens,sum_list,nodeid_list,nodes,linkid_list,links,flag):
    #查找路径限制长度
#
    query_word_middle='''
MATCH (n1)
MATCH (n2)
where id(n1)=''' +name1+ ''' and id(n2)=''' +name2+ '''
with n1,n2 limit 2
CALL apoc.path.expandConfig(n1, {minLevel: '''+startLens+''',maxLevel: '''+endLens+''',limit: 1,bfs: false,uniqueness: 'NODE_GLOBAL',terminatorNodes: [n2]})
YIELD path
UNWIND nodes(path) AS node
UNWIND relationships(path) AS rel
RETURN node, rel
'''

    results_place = session2.run(query_word_middle)
    res=manage(results_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
    session2.close()
    return res


@func_set_timeout(15)
def searchhipshow(session2,name1,name2,name3,startLens,endLens,sum_list,nodeid_list,nodes,linkid_list,links,flag):

    #查找路径含有中间节点
    # query_word_middle='''
    #     MATCH (start:Category), (end:item), (via:Category )
    #     MATCH path = ((start)-[*''' +startLens+".."+endLens+ '''] -(end))
    #     where start.name=\"''' +name1+ '''\" and end.name=\"''' +name2+ '''\" and via.name=\"''' +name3+ '''\" and via IN nodes(path) AND via <> start AND via <> end
    #     UNWIND nodes(path) AS node
    #     UNWIND relationships(path) AS rel
    #     RETURN node, rel
    #     union
    #     MATCH (start:item), (end:item), (via:Category )
    #     MATCH path = ((start)-[*''' +startLens+".."+endLens+ '''] -(end))
    #     where start.name=\"''' +name1+ '''\" and end.name=\"''' +name2+ '''\" and via.name=\"''' +name3+ '''\" and via IN nodes(path) AND via <> start AND via <> end
    #     UNWIND nodes(path) AS node
    #     UNWIND relationships(path) AS rel
    #     RETURN node, rel
    # '''
    query_word_middle='''
        MATCH (start), (end), (via )
        MATCH path = ((start)-[*''' +startLens+".."+endLens+ '''] -(end))
        where id(start)='''+name1+''' and id(end)='''+name2+''' and id(via)='''+name3+''' and via IN nodes(path) AND via <> start AND via <> end
        UNWIND nodes(path) AS node
        UNWIND relationships(path) AS rel
        RETURN node, rel
    '''

    results_place = session2.run(query_word_middle)
    res=manage(results_place,sum_list,nodeid_list,nodes,linkid_list,links,flag)
    session2.close()
    return res

def manage(results_place,sum_list,nodeid_list,nodes,linkid_list,links,flag):

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





    sum_list['nodes'] = nodes
    sum_list['links'] = links
    sum_list['flag'] = flag

    return sum_list
def manageHp(results_place,sum_list,nodeid_list,nodes,linkid_list,links,flag):

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

                node_set["url"] = "www.zhonghuapu.com"
                node_set["timestamp"] = "20200506"
                node_set["symbolSize"] = 40
                node_set['category'] = 6
                node_set['properties']=node0._properties
                node_set["genealogy"] ="华谱"

                try:
                    cursor = db.cursor()
                    sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + node0._properties["myID"]
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    for i in result:
                        node_set["genealogy"]=i[0]
                        #时间戳转换
                        timeArray = time.localtime(i[1])
                        formatTime = time.strftime("%Y%m%d", timeArray)
                        node_set["timestamp"]=int(formatTime)
                except Exception as e:
                    print("查询失败")

                if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    nodes.append(node_set)

        if node1 is not None:
            if node1.id not in nodeid_list:
                node_set1 = {}
                nodeid_list.append(node1.id)
                node_set1["id"] = str(node1.id)
                node_set1["value"] = 1
                node_set1["name"] =  zhconv.convert(node1._properties["name"],'zh-cn')
                node_set1["url"] = "www.zhonghuapu.com"
                node_set1["timestamp"] = "20200506"
                node_set1["symbolSize"] = 40
                node_set1['category'] = 6
                node_set1["genealogy"] ="华谱"
                node_set1['properties']=node1._properties
                try:
                    cursor = db.cursor()
                    sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + node1._properties["myID"]
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    for i in result:
                        node_set1["genealogy"]=i[0]
                        #时间戳转换
                        timeArray = time.localtime(i[1])
                        formatTime = time.strftime("%Y%m%d", timeArray)
                        node_set1["timestamp"]=int(formatTime)
                except Exception as e:
                    print("查询失败")
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
    sum_list['nodes'] = nodes
    sum_list['links'] = links
    sum_list['flag'] = flag

    return sum_list
def searchHpById(id,session_hp1):
    # 华谱库搜素


    query_word_hp = '''
          MATCH (h:People) where id(h)= '''+id+''' return  h
    '''

    results_hp = session_hp1.run(query_word_hp)
    for res_hp in results_hp:
        if len(res_hp["h"]._properties["myID"])!=0:
            nodeset = {}
            try:
                cursor = db.cursor()
                sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + res_hp["h"]._properties["myID"]
                cursor.execute(sql)
                result = cursor.fetchall()
                for i in result:
                    nodeset["id"] = str(res_hp["h"].id)
                    nodeset["value"] = 1
                    nodeset["name"] = zhconv.convert(res_hp['h']._properties["name"], 'zh-cn')
                    nodeset["url"] = "华谱系统（www.zhonghuapu.com）"
                    nodeset["symbolSize"] = 40
                    nodeset['category'] = 6
                    nodeset["genealogy"] =i[0]
                    nodeset['properties']=res_hp['h']._properties
                    #时间戳转换
                    timeArray = time.localtime(i[1])
                    formatTime = time.strftime("%Y%m%d", timeArray)
                    nodeset["timestamp"]=int(formatTime)
            except Exception as e:
                print("查询失败")
    session_hp1.close()
    return nodeset