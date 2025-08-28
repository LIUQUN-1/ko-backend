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
import time
corresponsed={"BaiduBaike":"百度百科","Category":"科目","HAO":"HAO营销","Wikipedia":"维基百科","ownthink":"思知","openkg":"openkg","selfCreate":"自建节点","YongleCanon":"永乐大典","hyperNode":"超点","HAO":"HAO营销","ScholarCSKG":"ScholarCSKG","MAKG":"MAKG","Journals":"Journals","ConceptNet":"ConceptNet","CONFInstances":"CONFInstances","CONF Series":"CONF Series","Author":"Author","Affiliations":"Affiliations"}

def main(request):

    db = pymysql.connect(host='www.zhonghuapu.com',
                         user='koroot',
                         password='DMiC-4092',
                         database='db_hp',
                         charset='utf8')

    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    #


    id=request.GET["id"]
    print(request.GET["ids"])
    ids=request.GET["ids"].split(',')
    # category_flag=6
    category_flag = request.GET["category"]
    category_flag = int(category_flag)
    nodeName=request.GET["name"]
    new_ids=[]
    for i in ids:
        new_ids.append(int(i))


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
        }
    ]

    nodes = []
    links = []
    table = []

    # driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    session_hp = driver_hp.session()
    # 点击华谱人物节点
    if category_flag == 6:
        newTable=[]
        # print("查家谱")
        query_word = '''
        match (h1)-[r1]-(h2)
        where id(h1)={arg_1} and exists(h2.myID)  and (not exists(h2.delFlag) or h2.delFlag=0)
        return h1 as h,r1 as r,h2 as t limit 20
        union
        match (h1)-[r1]-(h2)-[r2]-(h3)
        where id(h1)={arg_1} and id(h3) in {arg_2} and id(h1) <> id(h3) and exists(h2.myID)  and (not exists(h2.delFlag) or h2.delFlag=0)
        return h2 as h,r2 as r, h3 as t limit 20
            '''
        result_hp = session_hp.run(query_word, parameters={"arg_1": int(id),"arg_2": new_ids})

        for res_hp in result_hp:
            if res_hp["h"] is not None:
                if res_hp['h'].id not in nodeid_list and len(res_hp["h"]._properties["myID"])!=0:
                    nodeset = {}
                    try:
                        cursor = db.cursor()
                        sql = "select name ,createtime from tb_mydata where id=" + res_hp["h"]._properties["myID"]
                        cursor.execute(sql)
                        result = cursor.fetchall()
                        for i in result:
                            nodeid_list.append(res_hp['h'].id)
                            nodeset["id"] = str(res_hp["h"].id)
                            nodeset["value"] = 1
                            nodeset["name"] = zhconv.convert(res_hp['h']._properties["name"], 'zh-cn')
                            nodeset["url"] = "华谱系统（www.zhonghuapu.com）"
                            nodeset["symbolSize"] = 30
                            nodeset['category'] = 6
                            nodeset["genealogy"] = i[0]
                            nodeset["properties"]=res_hp['h']._properties
                            # 时间戳转换
                            import time
                            timeArray = time.localtime(i[1])
                            formatTime = time.strftime("%Y%m%d", timeArray)
                            nodeset["timestamp"] = int(formatTime)
                            #关联人物与家谱建边
                            if res_hp["h"]._properties["myID"] in ids:
                                linkset1={}
                                linkset1['source']=nodeset["id"] #家谱id
                                linkset1['label']= {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                                linkset1['target'] = str(res_hp["h"]._properties["myID"])#人物id
                                if linkset1 not in links:
                                    links.append(linkset1)
                    except Exception as e:
                        print("查询失败")
                    if len(nodeset) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        nodes.append(nodeset)

            if res_hp["t"] is not None:
                if res_hp['t'].id not in nodeid_list and len(res_hp["t"]._properties["myID"])!=0:
                    nodeset = {}
                    try:
                        cursor = db.cursor()
                        sql = "select name ,createtime from tb_mydata where id=" + res_hp["t"]._properties["myID"]
                        cursor.execute(sql)
                        result = cursor.fetchall()
                        for i in result:
                            nodeid_list.append(res_hp['t'].id)
                            nodeset["id"] = str(res_hp["t"].id)
                            nodeset["value"] = 1
                            nodeset["name"] = zhconv.convert(res_hp['t']._properties["name"], 'zh-cn')
                            nodeset["url"] = "华谱系统（www.zhonghuapu.com）"
                            nodeset["symbolSize"] = 30
                            nodeset['category'] = 6
                            nodeset["genealogy"] = i[0]
                            nodeset["properties"]=res_hp['t']._properties
                            # 时间戳转换
                            import time
                            timeArray = time.localtime(i[1])
                            formatTime = time.strftime("%Y%m%d", timeArray)
                            nodeset["timestamp"] = int(formatTime)

                            #关联人物与家谱建边
                            if res_hp["t"]._properties["myID"] in ids:

                                linkset1={}
                                linkset1['source']=nodeset["id"] #家谱id
                                linkset1['label']= {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                                linkset1['target'] = str(res_hp["t"]._properties["myID"])#人物id
                                if linkset1 not in links:
                                    links.append(linkset1)
                    except Exception as e:
                        print("查询失败")
                    if len(nodeset) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        nodes.append(nodeset)

            if res_hp["r"] is not None:
                if res_hp['r'].id not in linkid_list:
                    tableSingle={}
                    link_set = {}
                    table_set = {}
                    startNodeId = str(res_hp["r"].nodes[0].id)
                    endNodeId = str(res_hp["r"].nodes[1].id)
                    for node in nodes:
                        if startNodeId == node["id"]:
                            link_set["target"] = startNodeId
                        if endNodeId == node["id"]:
                            link_set["source"] = endNodeId
                        link_set["label"] = {"show": True, "formatter": "person2person", "fontSize": 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}

                    if len(link_set) != 0:
                        links.append(link_set)

                    table_set["_fields"] = []
                    start_properties = res_hp["r"].nodes[0]._properties
                    end_properties = res_hp["r"].nodes[1]._properties
                    try:
                        start_properties["name"] = zhconv.convert(start_properties["name"], 'zh-cn')
                        start_properties["url"] = zhconv.convert("www.zhonghuapu.com", 'zh-cn')

                        end_properties["name"] = zhconv.convert(end_properties["name"], 'zh-cn')
                        end_properties["url"] = zhconv.convert("www.zhonghuapu.com", 'zh-cn')
                    except Exception as e:
                        start_properties["name"] = zhconv.convert("null name", 'zh-cn')
                        start_properties["url"] = zhconv.convert("www.zhonghuapu.com", 'zh-cn')

                        end_properties["name"] = zhconv.convert("null name", 'zh-cn')
                        end_properties["url"] = zhconv.convert("www.zhonghuapu.com", 'zh-cn')

                    table_set["_fields"].append({'properties': start_properties})

                    table_set["_fields"].append({'type': res_hp["r"].type})

                    table_set["_fields"].append({'properties': end_properties})
                    table.append(table_set)
                    try:
                        tableSingle["name"]=start_properties["name"]
                        tableSingle["type"]=res_hp["r"].type
                        tableSingle["name1"]=end_properties["name"]
                        newTable.append(tableSingle)
                    except Exception as e:
                        print()
        sum_list['nodes'] = nodes
        sum_list['links'] = removeTwoway(links)
        sum_list['table'] = table
        sum_list['newTable'] = newTable
        session.close()
        session_hp.close()
        return json.dumps(sum_list, ensure_ascii=False)
    # 点击家谱
    elif category_flag == 7:
        newTable=[]
        # 遍历每份家谱找到相关联的人并建边
        query_word_hp_jiapu = '''
              MATCH (h:People) where h.myID ={arg_1} and ( not exists(h.delFlag) or h.delFlag=0)  return  h limit 20
        '''
        results_hp_jiapu = session_hp.run(query_word_hp_jiapu, parameters={"arg_1": str(id)})
        # 画出人物节点，并和家谱节点建边
        for res in results_hp_jiapu:
            tableSingle={}
            if res['h'].id not in nodeid_list:
                #取家谱信息
                cursor = db.cursor()
                sql = "select name,createtime  from tb_mydata where id=" + str(id)
                cursor.execute(sql)
                result = cursor.fetchall()
                jiapuName=""
                time=0
                for i in result:
                    jiapuName=i[0]
                    #时间戳转换
                    import time
                    timeArray = time.localtime(i[1])
                    formatTime = time.strftime("%Y%m%d", timeArray)
                    time=int(formatTime)
                node_set1={}
                nodeid_list.append(res['h'].id)
                node_set1["id"] = str(res["h"].id)
                node_set1["value"] = 1
                node_set1["name"] = zhconv.convert(res['h']._properties["name"],'zh-cn')
                node_set1["url"] = "华谱系统（www.zhonghuapu.com）"
                node_set1["symbolSize"] = 30
                node_set1['category'] = 6
                node_set1['timestamp']=time
                node_set1['genealogy']=jiapuName
                node_set1['properties']=res['h']._properties

                nodes.append(node_set1)
                #和家谱节点建边
                link_set1={}
                link_set1['source']=node_set1["id"] #人物id
                link_set1['label']= {'show': True, 'formatter': 'GenePerson', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                link_set1['target'] = str(id)#家谱id
                if link_set1 not in links:
                    links.append(link_set1)

                table_set1 = {}
                table_set1["_fields"]=[]
                table_set1["_fields"].append({'properties':{"name":jiapuName,"url":"www.zhonghuapu.com"}})
                table_set1["_fields"].append({'type':'GenePerson'})
                table_set1["_fields"].append({'properties':node_set1})
                table.append(table_set1)
                try:
                    tableSingle["name"]=jiapuName
                    tableSingle["type"]='家谱内人物'
                    tableSingle["name1"]=res['h']._properties["name"]
                    newTable.append(tableSingle)
                except Exception as e:
                    print()
        sum_list['nodes'] = nodes
        sum_list['links'] = removeTwoway(links)
        sum_list['table'] = table
        sum_list['newTable'] = newTable
        session.close()
        session_hp.close()
        return json.dumps(sum_list, ensure_ascii=False)
    # 点击非华谱节点
    else:
        newTable=[]
        query_word = '''
        match (h1)-[r1]-(h2)
        where id(h1)={arg_1}
        return h1 as h,r1 as r,h2 as t limit 30
        union
        match (h1)-[r1]-(h2)-[r2]-(h3)
        where id(h1)={arg_1} and id(h3) in {arg_2} and id(h1) <> id(h3)
        return h2 as h,r2 as r, h3 as t limit 30
            '''
        results_place = session.run(query_word, parameters={"arg_1": int(id),"arg_2": new_ids})
        for res in results_place:
            tableSingle={}
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
                    except KeyError:
                        print()

                    node_set["symbolSize"]=30
                    if 'Wikipedia' in list(set(res['h'].labels)):
                        node_set['category']=0
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
                    elif 'HAO' in list(set(res['h'].labels)):
                        node_set['category'] = 11
                    elif 'BaiduBaike' in list(set(res['h'].labels)):
                        node_set['category'] = 12
                    else:
                        node_set['category'] = 2

                    node_set['properties']=res['h']._properties
                    if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        node_set["index"]=len(nodes)
                        nodes.append(node_set)

            if res["t"] is not None:
                if res['t'].id not in nodeid_list:

                    node_set1 = {}
                    nodeid_list.append(res['t'].id)
                    node_set1["id"] = str(res['t'].id)
                    node_set1["value"] = 1
                    node_set1["name"] =  zhconv.convert(res['t']._properties["name"],'zh-cn')

                    try:
                        node_set1["timestamp"] = res['t']._properties["timestamp"]
                        node_set1["url"] = zhconv.convert(res['t']._properties["url"],'zh-cn')
                    except KeyError:
                        print()
                    node_set1["symbolSize"]=30
                    if 'Wikipedia' in list(set(res['t'].labels)):
                        node_set1['category']=0
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
                    elif 'HAO' in list(set(res['t'].labels)):
                        node_set1['category'] = 11
                    elif 'BaiduBaike' in list(set(res['t'].labels)):
                        node_set1['category'] = 12
                    else:
                        node_set1['category'] = 2
                    node_set1['properties']=res['t']._properties
                    if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                        node_set1["index"] = len(nodes)
                        nodes.append(node_set1)

            if res["r"] is not None:
                if res['r'].id not in linkid_list:

                    link_set = {}
                    table_set = {}
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

                    try:
                        tableSingle["name"]=res["r"].nodes[0]._properties["name"]
                        tableSingle["type"]=res["r"].type
                        tableSingle["name1"]=res["r"].nodes[1]._properties["name"]
                        newTable.append(tableSingle)
                    except KeyError as e:
                        print()

                    if len(link_set) != 0:
                        links.append(link_set)

                    # table_set["_fields"]=[]
                    # start_properties=res["r"].nodes[0]._properties
                    # end_properties=res["r"].nodes[1]._properties
                    # start_properties["name"]=zhconv.convert(start_properties["name"],'zh-cn')
                    # start_properties["url"]=zhconv.convert(start_properties["url"],'zh-cn')
                    # end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')
                    # end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')
                    #
                    # table_set["_fields"].append({'properties':start_properties})
                    #
                    # table_set["_fields"].append({'type':res["r"].type})
                    # table_set["_fields"].append({'properties':end_properties})
                    # table.append(table_set)


        sum_list['nodes'] = nodes
        sum_list['links'] = links
        sum_list['newTable'] = newTable
        # sum_list['table'] = table

        #查询华谱
        query_word_hp = '''
              MATCH (h:People) where h.name ={arg_1} and exists(h.myID)  and ( not exists(h.delFlag) or h.delFlag=0) return  h
        '''
        results_hp = session_hp.run(query_word_hp, parameters={"arg_1": nodeName})
        jiapu=[]
        for res_hp in results_hp:
            tableSingle={}
            if res_hp['h'].id not in nodeid_list and len(res_hp["h"]._properties["myID"])!=0:
                nodeset = {}
                # nodeset2 = {}
                link_set2 = {}
                try:
                    cursor = db.cursor()
                    sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + res_hp["h"]._properties["myID"]
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    for i in result:
                        # if i[5] == userId or i[4] != 1:
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
                            import time
                            timeArray = time.localtime(i[1])
                            formatTime = time.strftime("%Y%m%d", timeArray)
                            nodeset["timestamp"]=int(formatTime)
                            #
                            # # 家谱节点
                            # nodeset2["id"] = str(res_hp["h"]._properties["myID"])
                            # nodeset2["value"] = 1
                            # nodeset2["name"] = i[0]
                            # nodeset2["url"] = "华谱系统（www.zhonghuapu.com）"
                            # nodeset2["symbolSize"] = 30
                            # nodeset2['category'] = 7
                            # nodeset2["GeneNumber"] = i[2]
                            # nodeset2['description'] = i[3]
                            # # nodeset2['description'] = "此家谱家族体系庞大"
                            # nodeset2["timestamp"]=int(formatTime)
                            # 家谱边
                            link_set2['source'] = id
                            link_set2['label'] = {'show': True, 'formatter': 'Same', 'fontSize': 12,"desc": {"关系":"关系源自华谱系统的内部关联,详情请前往：www.zhonghuapu.com"}}
                            link_set2['target'] = nodeset['id']
                            # try:
                            #     tableSingle["name"]=nodeset['id']
                            #     tableSingle["type"]='Same'
                            #     tableSingle["name1"]=res["r"].nodes[1]._properties["name"]
                            #     newTable.append(tableSingle)
                            # except KeyError as e:
                            #     print()
                            #
                except Exception as e:
                    print("查询失败")

                if link_set2 not in links and len(link_set2)!=0:
                    links.append(link_set2)
                if len(nodeset) != 0 :  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空
                    # sameAs.append(nodeset)
                    nodes.append(nodeset)
                # if len(nodeset2) != 0:
                #     nodes.append(nodeset2)

        sum_list['nodes'] = nodes
        sum_list['links'] = removeTwoway(links)

        session.close()
        session_hp.close()
        return json.dumps(sum_list, ensure_ascii=False)


def removeTwoway(links):
    data=links
    seen = set()

    # 使用列表推导来过滤重复的字典
    unique_data = []
    for d in data:
        # 确保source和target是有序的，以便比较
        source_target_pair = tuple(sorted((d['source'], d['target'])))
        if source_target_pair not in seen:
            seen.add(source_target_pair)
            unique_data.append(d)
    # 打印结果
    return unique_data


