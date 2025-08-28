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
from datetime import datetime
from py2neo import *
from py2neo.matching import *
translation_dict = {
    "name": "名称",
    "url": "链接",
    "timestamp": "时间",
    "source":"来源",
    "desc":"描述",
    "des":"描述",
    "gptDes":"大模型描述"
}
corresponsed={"BaiduBaike":"百度百科","Category":"科目","HAO":"HAO营销","Wikipedia":"维基百科","ownthink":"思知","openkg":"openkg","selfCreate":"自建节点","YongleCanon":"永乐大典","hyperNode":"超点","HAO":"HAO营销","ScholarCSKG":"ScholarCSKG",
              "MAKG":"MAKG","Journals":"Journals","ConceptNet":"ConceptNet","CONFInstances":"CONFInstances",
              "CONF Series":"CONF Series","Author":"Author","Affiliations":"Affiliations","Thingo":"思高"}

def main(request):
    hyperData=[]
    name = request.GET["name"]
    hyperID=getID(name,hyperData)

    if request.GET["userId"]!="null":
        userId = request.GET["userId"]
        print(f'useID:{userId}')
    else:
        userId= "6000027"
    print(f'name:{name}')
    userId= "6000027"
    userId = int(userId)

    db = pymysql.connect(host='www.zhonghuapu.com',
                         user='koroot',
                         password='DMiC-4092',
                         database='db_hp',
                         charset='utf8')

    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    # driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    driver_hp = GraphDatabase.driver("bolt://114.213.234.179:7687", auth=("neo4j", "hfut701DMiChp"))
    # driver_hp = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session_hp = driver_hp.session()

    nodeid_list = []  # 用于去重
    linkid_list = []  # 用于去重
    sum_list = {}  # 总数据
    sum_list["type"]="force"
    response={"0":"维基百科","1":"二层科目","2":"其他科目","3":"根科目","4":"思知","5":"一层科目","6":"华谱系统人物","7":"华谱系统家谱","8":"自建节点","9":"scholarCSKG","10":"超点","11":"HAO营销","11":"百度百科"}
    corresponsed={"BaiduBaike":"百度百科","Category":"科目","HAO":"HAO营销","Wikipedia":"维基百科","ownthink":"思知","openkg":"openkg","selfCreate":"自建节点","YongleCanon":"永乐大典","hyperNode":"超点","HAO":"HAO营销","ScholarCSKG":"ScholarCSKG","MAKG":"MAKG","Journals":"Journals",
                  "ConceptNet":"ConceptNet","CONFInstances":"CONFInstances","CONF Series":"CONF Series",
                  "Author":"Author","Affiliations":"Affiliations","Thingo":"思高"}
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
        },
        {
            "name": "超点",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#FF7F00'  #5994a0
                }
            }
        },
        {
            "name": "HAO",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#32CD99'  #5994a0
                }
            }
        },
        {
            "name": "百度百科",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#EAADEA'  #5994a0
                }
            }
        },
        {
            "name": "大模型",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#5994a0'  # 5994a0
                }
            }
        },
        {
            "name": "思高",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#88abda'  # 5994a0
                }
            }
        }
    ]

    nodes = []
    links = []
    table = []
    sameAs = []
    new_name=name
    #查询知海
    query_word = '''
        MATCH (h:Wikipedia) where h.name ={arg_1}  OPTIONAL match (h)-[r:Contain]-(t:Wikipedia) return h, r, t limit 50 union
        
        MATCH (h:ownthink) where h.name ={arg_1} OPTIONAL match (h)-[r:Contain]-(t:ownthink) return h, r, t limit 50 union
        
        MATCH (h:Thingo) where h.name ={arg_1} OPTIONAL match (h)-[r:Contain]-(t:Thingo) return h, r, t limit 50 union
        
        MATCH (h:selfCreate) where h.name ={arg_1} and h.public=1 OPTIONAL match (h)-[r:Contain]-(t:item) where t.public=1 return h, r, t limit 20 union
        
        MATCH (a:UserId)-[]-(b:selfCreate) where a.user_id = {arg_2} WITH collect(b) AS connectedNodes
        
        MATCH (h:selfCreate) where h.name ={arg_1} and h.public=0 and h IN connectedNodes OPTIONAL match (h)-[r:Contain]-(t:item) where t.public=0 and t IN connectedNodes return h, r, t limit 20 union
        
        MATCH (h:ScholarCSKG) where h.name ={arg_1} OPTIONAL match (h)-[r:Contain]-(t:item) return h, r, t limit 50 union
        MATCH (h:BaiduBaike) where h.name ={arg_1} OPTIONAL match (h)-[r:Contain]-(t:item) return h, r, t limit 20 union
        MATCH (h:HAO) where h.name ={arg_1} OPTIONAL match (h)-[r:Contain]-(t:HAO) return h, r, t limit 10 union
        MATCH (h:hyperNode) where h.name ={arg_1} OPTIONAL match (h)-[r:Same]-(t:item) return h, r, t limit 15 union
        MATCH (h:item)-[r:SameAs]-(t:item) where h.name ={arg_1}  return h, r, t limit 20 union
        MATCH (h:item)-[r:transE2C]-(t:item) where h.name ={arg_1}  return h, r, t limit 20 union
        MATCH (h:item)-[r:belong]-(t:Category) where h.name ={arg_1} return h, r, t limit 20 union
        MATCH (h:Wikipedia)
        where h.name ={arg_1}
        CALL apoc.path.expandConfig(h,{
        relationshipFilter: "Sub|belong",
         labelFilter: ">Category",
         minLevel: 1,
         maxLevel: 10,
         limit:100
        })
        yield path
        with relationships(path) as rels
        unwind rels as rel
        return startNode(rel) as h ,rel as r, endNode(rel) as t 
        union
        MATCH (h:Thingo)
        where h.name ={arg_1}
        CALL apoc.path.expandConfig(h,{
        relationshipFilter: "Contain",
        minLevel: 1,
        maxLevel: 5,
        limit:50
        })
        yield path
        with relationships(path) as rels
        unwind rels as rel
        return startNode(rel) as h ,rel as r, endNode(rel) as t

        '''
    results_place = session.run(query_word, parameters={"arg_1": name,"arg_2": userId})

    # # # 如果没有查到节点
    # if results_place.peek() is None:
    #     graph = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    #     # graph = Graph("bolt://114.213.232.140:7687", auth=('neo4j', 'DMiChao'))
    #     matcher = NodeMatcher(graph)
    #     # 不区分大小写差
    #     newnodes = matcher.match("EnWikipedia").where("_.name=~'(?i)" + name + "'").first()
    #     # print(nodes)
    #     new_name=newnodes['name']
    #
    #
    #     query_word2 = '''
    #                MATCH (h:Wikipedia) where h.name={arg_2}     OPTIONAL match (h)-[r:Contain]-(t:Wikipedia) return h, r, t limit 50 union
    #                MATCH (h:ownthink) where h.name ={arg_2}     OPTIONAL match (h)-[r:Contain]-(t:ownthink) return h, r, t limit 50 union
    #                MATCH (h:selfCreate) where h.name ={arg_2}     OPTIONAL match (h)-[r:Contain]-(t:item) return h, r, t limit 20 union
    #                MATCH (h:ScholarCSKG) where h.name ={arg_2}     OPTIONAL match (h)-[r:Contain]-(t:item) return h, r, t limit 50 union
    #                MATCH (h:item)-[r:SameAs]-(t:item) where h.name ={arg_2}    return h, r, t limit 20 union
    #                MATCH (h:item)-[r:transE2C]-(t:item) where h.name ={arg_2}     return h, r, t limit 20 union
    #                MATCH (h:item)-[r:belong]-(t:Category) where h.name ={arg_2}    return h, r, t limit 20 union
    #                MATCH (h:item)
    #                where h.name ={arg_2}
    #                CALL apoc.path.expandConfig(h,{
    #                relationshipFilter: "Sub|belong",
    #                 labelFilter: ">Category",
    #                 minLevel: 1,
    #                 maxLevel: 10,
    #                 limit:100
    #                })
    #                yield path
    #                with relationships(path) as rels
    #                unwind rels as rel
    #                return startNode(rel) as h ,rel as r, endNode(rel) as t limit 200
    #                '''
    #     results_place = session.run(query_word2, parameters={"arg_2": new_name})



    for res in results_place:
        if res["h"] is not None:
            if res['h'].id not in nodeid_list:
                node_set = {}

                nodeid_list.append(res['h'].id)
                node_set["id"] = str(res['h'].id)
                node_set["value"] = 1
                node_set["name"] = zhconv.convert(res['h']._properties["name"],'zh-cn')
                try:
                    node_set["url"] = zhconv.convert(res['h']._properties["url"],'zh-cn')
                    node_set["timestamp"] = res['h']._properties["timestamp"]
                except KeyError as e:
                    node_set["url"]="https://ko.zhonghuapu.com"
                    node_set["timestamp"] = "20240101"
                node_set["symbolSize"] = 30
                if 'Wikipedia' in list(set(res['h'].labels)):
                    node_set['category'] = 0
                elif 'CaLe0' in list(set(res['h'].labels)):
                    node_set['category'] = 3
                elif 'CaLe1' in list(set(res['h'].labels)):
                    node_set['category'] = 5
                elif 'CaLe2' in list(set(res['h'].labels)):
                    node_set['category'] = 1
                elif 'ownthink' in list(set(res['h'].labels)):
                    node_set['category'] = 4
                elif 'selfCreate' in list(set(res['h'].labels)):
                    node_set['category'] = 8
                elif 'ScholarCSKG' in list(set(res['h'].labels)):
                    node_set['category'] = 9
                elif 'hyperNode' in list(set(res['h'].labels)):
                    node_set['category'] = 10
                    node_set["symbolSize"] = 40
                elif 'HAO' in list(set(res['h'].labels)):
                    node_set['category'] = 11
                elif 'BaiduBaike' in list(set(res['h'].labels)):
                    node_set['category'] = 12
                elif 'Thingo' in list(set(res['h'].labels)):
                    node_set['category'] = 14
                else:
                    node_set['category'] = 2

                # node_set['properties']=res['h']._properties
                node_set['properties']=change_keys_to_chinese(res['h']._properties, translation_dict)
                if node_set["name"]==new_name:

                    sameAs.append(node_set)
                if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    nodes.append(node_set)

        if res["t"] is not None:
            if res['t'].id not in nodeid_list:
                node_set1 = {}
                nodeid_list.append(res['t'].id)
                node_set1["id"] = str(res['t'].id)
                node_set1["value"] = 1
                node_set1["name"] =  zhconv.convert(res['t']._properties["name"],'zh-cn')
                try:
                    node_set1["url"] = zhconv.convert(res['t']._properties["url"],'zh-cn')
                except KeyError as e:
                    node_set1["url"]="https://ko.zhonghuapu.com"
                node_set1["timestamp"] = res['t']._properties["timestamp"]
                node_set1["symbolSize"] = 30

                if 'Wikipedia' in list(set(res['t'].labels)):
                    node_set1['category'] = 0
                elif 'CaLe0' in list(set(res['t'].labels)):
                    node_set1['category'] = 3
                elif 'CaLe1' in list(set(res['t'].labels)):
                    node_set1['category'] = 5
                elif 'CaLe2' in list(set(res['t'].labels)):
                    node_set1['category'] = 1
                elif 'ownthink' in list(set(res['t'].labels)):
                    node_set1['category'] = 4
                elif 'selfCreate' in list(set(res['t'].labels)):
                    node_set1['category'] = 8
                elif 'ScholarCSKG' in list(set(res['t'].labels)):
                    node_set1['category'] = 9
                elif 'hyperNode' in list(set(res['t'].labels)):
                    node_set1['category'] = 10
                    node_set["symbolSize"] = 40
                elif 'HAO' in list(set(res['t'].labels)):
                    node_set1['category'] = 11
                elif 'BaiduBaike' in list(set(res['t'].labels)):
                    node_set1['category'] = 12
                elif 'Thingo' in list(set(res['t'].labels)):

                    node_set1['category'] = 14
                else:
                    node_set1['category'] = 2
                # node_set1['properties']=res['t']._properties
                node_set1['properties']=change_keys_to_chinese(res['t']._properties, translation_dict)
                if node_set1["name"] == new_name:
                    sameAs.append(node_set1)
                if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    nodes.append(node_set1)


        link_set = {}
        table_set = {}
        if res["r"] is not None:
            if res['r'].id not in linkid_list:

                startNodeId=str(res["r"].nodes[0].id)
                endNodeId=str(res["r"].nodes[1].id)

                for node in nodes:
                    if startNodeId==node["id"]:
                        link_set["target"]=startNodeId
                    if endNodeId==node["id"]:
                        link_set["source"]=endNodeId
                # 找到关系的两端节点的来源
                relDes=""
                startNodelabels=list(set(res["r"].nodes[0].labels))
                startResource=""
                for i in startNodelabels:
                    if i in corresponsed.keys():
                        startResource=corresponsed[i]
                endNodelabels=list(set(res["r"].nodes[1].labels))
                endResource=""
                for i in endNodelabels:
                    if i in corresponsed.keys():
                        endResource=corresponsed[i]

                if endResource==startResource:
                    try:
                        relDes="关系为同一来源："+startResource+"的内部关联，详情请前往："+res["r"].nodes[0]._properties["url"]+"和"+res["r"].nodes[1]._properties["url"]
                    except KeyError as e:
                        relDes="关系为同一来源："+startResource+"的内部关联"
                else:
                    try:
                        relDes="关系为不同来源："+startResource+"--->"+endResource+"之间的关联，详情请前往："+res["r"].nodes[0]._properties["url"]+"和"+res["r"].nodes[1]._properties["url"]
                    except KeyError as e:
                        relDes="关系为不同来源："+startResource+"--->"+endResource+"之间的关联"


                link_set["label"]={"show":True,"formatter":res["r"].type,"fontSize":12,"desc": {"关系":relDes}}
                if len(link_set) != 0:
                    links.append(link_set)

                table_set["_fields"]=[]
                start_properties=res["r"].nodes[0]._properties
                end_properties=res["r"].nodes[1]._properties
                start_properties["name"]=zhconv.convert(start_properties["name"],'zh-cn')
                end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')
                # try:
                #     start_properties["url"]=zhconv.convert(start_properties["url"],'zh-cn')
                #     end_properties["url"]=zhconv.convert(end_properties["url"],'zh-cn')
                # except KeyError as e:
                #     start_properties["url"]="https://ko.zhonghuapu.com"
                #     end_properties["url"]="https://ko.zhonghuapu.com"
                table_set["_fields"].append({'properties':start_properties})
                table_set["_fields"].append({'type':res["r"].type})
                table_set["_fields"].append({'properties':end_properties})
                table.append(table_set)




    #查询华谱
    query_word_hp = '''
          MATCH (h:People) where h.name ={arg_1} and exists(h.myID)  and ( not exists(h.delFlag) or h.delFlag=0) return  h
    '''
    results_hp = session_hp.run(query_word_hp, parameters={"arg_1": name})
    jiapu=[]
    aabs = 0
    for res_hp in results_hp:
        if res_hp['h'].id not in nodeid_list and len(res_hp["h"]._properties["myID"])!=0  and res_hp["h"]._properties["myID"]!='demo':
            nodeset = {}
            nodeset2 = {}
            link_set2 = {}
            link_set3 = {}
            try:
                cursor = db.cursor()
                sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + res_hp["h"]._properties["myID"]
                cursor.execute(sql)
                result = cursor.fetchall()
                for i in result:
                    if i[5] == userId or i[4] != 1:
                        if i[0] not in jiapu:
                            jiapu.append(i[0])

                            nodeid_list.append(res_hp['h'].id)
                            nodeset["id"] = str(res_hp["h"].id)
                            nodeset["value"] = 1
                            nodeset["name"] = zhconv.convert(res_hp['h']._properties["name"], 'zh-cn')
                            nodeset["url"] = "华谱系统（www.zhonghuapu.com）"
                            nodeset["symbolSize"] = 30
                            nodeset['category'] = 6
                            nodeset["genealogy"] =i[0]
                            nodeset['properties']=res_hp['h']._properties

                            #时间戳转换
                            timeArray = time.localtime(i[1])
                            formatTime = time.strftime("%Y%m%d", timeArray)
                            nodeset["timestamp"]=int(formatTime)

                            # 家谱节点
                            nodeset2["id"] = str(res_hp["h"]._properties["myID"])
                            nodeset2["value"] = 1
                            nodeset2["name"] = i[0]
                            nodeset2["url"] = "华谱系统（www.zhonghuapu.com）"
                            nodeset2["symbolSize"] = 30
                            nodeset2['category'] = 7
                            nodeset2["GeneNumber"] = i[2]
                            nodeset2['description'] = i[3]
                            # nodeset2['description'] = "此家谱家族体系庞大"
                            nodeset2["timestamp"]=int(formatTime)
                            # 家谱边
                            link_set2['source'] = nodeset['id']
                            link_set2['label'] = {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                            link_set2['target'] = nodeset2['id']

                            link_set3['source'] = str(hyperID)
                            link_set3['label'] = {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系为不同来源：华谱--->超点之间的关联"}}
                            link_set3['target'] = nodeset['id']

                            # print(links)
                            # links.append(link_set1)
            except Exception as e:
                #continue
                print("查询失败")

            if link_set2 not in links and len(link_set2)!=0:
                links.append(link_set2)
            if link_set3 not in links and len(link_set3)!=0:

                links.append(link_set3)

            if len(nodeset) != 0 :  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                sameAs.append(nodeset)
                nodes.append(nodeset)
            if len(nodeset2) != 0:
                nodes.append(nodeset2)

    #
    # for i in range(len(sameAs)):
    #     for j in range(len(sameAs)):
    #         if i!=j:#去除自环
    #             link_set={}
    #             link_set['source']=sameAs[i]['id']
    #             link_set['label']= {'show': True, 'formatter': 'SameAs', 'fontSize': 12}
    #             link_set['target'] = sameAs[j]['id']
    #             if link_set not in links:
    #                 links.append(link_set)
    # 查询家谱
    cursor = db.cursor()
    sql1 = "select id ,name,num,createtime,description,public,creater from tb_mydata where num!='0' and name='%s'"%name
    cursor.execute(sql1)
    result = cursor.fetchall()
    for i in result:

        if i[6] == userId or i[5] != 1:
            #形成家谱节点
            node_set={}
            node_set["id"] = str(i[0])
            node_set["value"] = 1
            node_set["name"] = zhconv.convert(i[1], 'zh-cn')
            node_set["url"] = "华谱系统（www.zhonghuapu.com）"
            node_set["symbolSize"] = 30
            node_set['category'] = 7
            node_set['GeneNumber'] = str(i[2])
            node_set['description'] = str(i[4])
            # 时间戳转换
            timeArray = time.localtime(i[3])
            formatTime = time.strftime("%Y%m%d", timeArray)
            node_set["timestamp"] = int(formatTime)
            if len(node_set) != 0 :
                nodes.append(node_set)

            #遍历每份家谱找到相关联的人并建边
            query_word_hp_jiapu = '''
                  MATCH (h:People) where h.myID ={arg_1} and ( not exists(h.delFlag) or h.delFlag=0)  return  h limit 30
            '''
            results_hp_jiapu = session_hp.run(query_word_hp_jiapu, parameters={"arg_1": str(i[0])})
            # 画出人物节点，并和家谱节点建边
            for res in results_hp_jiapu:
                if res['h'].id not in nodeid_list:
                    node_set1={}
                    nodeid_list.append(res['h'].id)
                    node_set1["id"] = str(res["h"].id)
                    node_set1["value"] = 1
                    node_set1["name"] = zhconv.convert(res['h']._properties["name"],'zh-cn')
                    node_set1["url"] = "华谱系统（www.zhonghuapu.com）"
                    node_set1["symbolSize"] = 30
                    node_set1['category'] = 6
                    node_set1["timestamp"] = node_set["timestamp"]
                    node_set1["genealogy"] = node_set["name"]
                    node_set1["properties"] = res['h']._properties
                    nodes.append(node_set1)
                    #和家谱节点建边
                    link_set1={}
                    link_set1['source']=node_set1["id"] #家谱id
                    link_set1['label']= {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                    link_set1['target'] = node_set["id"]#人物id


                    if link_set1 not in links:
                        links.append(link_set1)

                    table_set1 = {}
                    table_set1["_fields"]=[]
                    table_set1["_fields"].append({'properties':node_set})
                    table_set1["_fields"].append({'type':'GenePerson'})
                    table_set1["_fields"].append({'properties':node_set1})
                    table.append(table_set1)
    sum_list['nodes'] = nodes
    sum_list['links'] = links
    sum_list['table'] = table
    sum_list['hyperTable']=hyperData

    session.close()
    # session_hp.close()
    print('----------serchfuse3接口调用结束----------')
    return json.dumps(sum_list, ensure_ascii=False)




def getID(name,hyperData):
    
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session3 = driver.session()
    query_word1='''
                match(n:hyperNode) where n.name={arg_1}
                return count(n) as num ,id(n) as id limit 1
            '''
    results_node1 = session3.run(query_word1, parameters={"arg_1": name})

    query_word='''
                    match(n:item) where n.name={arg_1}
                    return n
                '''
    results_node = session3.run(query_word, parameters={"arg_1": name})

    query_word2='''
                    match(n:item) where n.name={arg_1}
                    return n
                '''
    results_node2 = session3.run(query_word2, parameters={"arg_1": name})


    results_nodeTmp = session3.run(query_word, parameters={"arg_1": name})
    reSource=[]
    reUrl=[]
    reDes=[]
    hyperSource=""
    hyperUrl=""
    hyperDesc=""
    for res in results_node:
        #处理label

        for i in res["n"].labels:
            if i in corresponsed.keys():
                reSource.append(corresponsed[i])
                # hyperDataChild["source"]=corresponsed[i]
        #处理url
        try:
            if "haodaka" not in res["n"]._properties["url"]:
                reUrl.append(res["n"]._properties["url"])
                # hyperDataChild["url"]=res["n"]._properties["url"]
        except KeyError as e:
            print()
            # hyperDataChild["url"]="https://ko.zhonghuapu.com"
        #处理des\url
        try:
            reDes.append(res["n"]._properties["des"])
            # hyperDataChild["des"]=res["n"]._properties["des"]
        except KeyError as e:
            print()
            # hyperDataChild["des"]="无"
    for res in results_node2:
        hyperDataChild={}
        for i in res["n"].labels:
            if i in corresponsed.keys():
                hyperDataChild["source"]=corresponsed[i]
        try:
            if "haodaka" not in res["n"]._properties["url"]:
                hyperDataChild["url"]=res["n"]._properties["url"]
        except KeyError as e:
            hyperDataChild["url"]="https://ko.zhonghuapu.com"
        try:
            hyperDataChild["des"]=res["n"]._properties["des"]
            hyperDataChild["timestamp"]=str(res["n"]._properties["timestamp"])
        except KeyError as e:
            hyperDataChild["des"]="无"
            hyperDataChild["timestamp"]="20240101"
        hyperDataChild["name"]=name

        hyperDataChild["id"]=res["n"].id


        hyperData.append(hyperDataChild)
    for i in hyperData:
        if i.get("source") == "超点":
            hyperData.remove(i)

    #获取来源
    for i in list(set(reSource)):
        hyperSource+=i+"；"
    hyperSource=hyperSource[:-1]
    #获取链接
    for i in list(set(reUrl)):
        hyperUrl+=i+"；"
    hyperUrl=hyperUrl[:-1]
    #获取描述
    for i in list(set(reDes)): 
        hyperDesc+=i+"||"
    hyperDesc=hyperDesc[:-1]


    res_len=len(list(results_nodeTmp))

    #库中没有这个点
    if res_len == 0:
        return -1,hyperData
    # 库中有这个点
    else:

        #库中没有超点，建立超点
        if results_node1.peek() is None:
            if res_len>1:
                current_date = datetime.now().date()
                # 将日期格式化为整数
                formatted_date = int(current_date.strftime('%Y%m%d'))
                query_word3="CREATE (n:hyperNode:item) set n.url=\""+hyperUrl+"\", n.desc=\""+hyperDesc+"\",n.source=\""+hyperSource+"\",n.name=\""+name+"\",n.timestamp="+str(formatted_date)+"  with n MATCH (a:item ), (b:hyperNode) where a.name=n.name and id(a)<>id(n) and  id(b)=id(n)  MERGE (a)-[r:Same]->(b) return id(b) as  id"
                results_node3 = session3.run(query_word3, parameters={"arg_1": name})
                id2=0
                for i in results_node3:
                    id2=i["id"]
                return id2,hyperData
            else:#"只有一个点 返回这个点id"
                query_word2='''
                    match(n:item) where n.name={arg_1} return id(n) as id limit 1
                    '''
                results_node2 = session3.run(query_word2, parameters={"arg_1": name})
                id1=0
                for i in results_node2:
                    id1=i["id"]
                return id1,hyperData
        #库中有超点，返回超点id
        else:
            id=0
            for i in results_node1:
                id=i["id"]
            return id,hyperData



    session3.close()
    return -1,hyperData

def change_keys_to_chinese(dictionary, translation):
    new_dict = {}
    for key, value in dictionary.items():
        if key in translation:
            new_key = translation[key]
        else:
            new_key = key
        new_dict[new_key] = value
    return new_dict