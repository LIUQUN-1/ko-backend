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

from urllib.parse import quote, unquote
import random
import zhconv
import argparse
from neo4j import GraphDatabase
import json
import pymysql
import time

def main(request):

    corresponsed={"hyperNode":"超点","BaiduBaike":"百度百科","Category":"科目","HAO":"HAO营销","Wikipedia":"维基百科","ownthink":"思知","openkg":"openkg","selfCreate":"自建节点","YongleCanon":"永乐大典","ScholarCSKG":"ScholarCSKG","MAKG":"MAKG","Journals":"Journals","ConceptNet":"ConceptNet","CONFInstances":"CONFInstances","CONF Series":"CONF Series","Author":"Author","Affiliations":"Affiliations"}
    name=request.GET["name"]
    # driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))
    # 知海连接
    driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    # 华谱连接
    driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    session_hp = driver_hp.session()
    db = pymysql.connect(host='www.zhonghuapu.com',
                         user='koroot',
                         password='DMiC-4092',
                         database='db_hp',
                         charset='utf8')

    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")

    queryword='''match(h:item) where h.name="'''+name+'''\" return h union match(h:Category) where h.name="'''+name+'''\" return h'''

    results_place=session.run(queryword)
    res1=[]
    if len(name)!=0:
        for res in results_place:
            temp={}
            name=res['h']._properties["name"]
            labels=list(set(res['h'].labels))
            label=""
            for i in labels:
                if i in corresponsed.keys():
                    label=corresponsed[i]
            id=str(res['h'].id)
            value="名称："+name+" | 内部id："+id+" | 来源："+label
            value=name+"("+label+")"
            temp["value"]=value
            temp["id"]=str(res['h'].id)
            temp["resource"]="知海"
            res1.append(temp)
    else:
        res1=[]
    # 华谱库搜素
    query_word_hp = '''
          MATCH (h:People) where h.name ={arg_1} and exists(h.myID)  and ( not exists(h.delFlag) or h.delFlag=0) return  h
    '''
    results_hp = session_hp.run(query_word_hp, parameters={"arg_1": name})
    for res_hp in results_hp:
        if len(res_hp["h"]._properties["myID"])!=0:
            nodeset = {}
            nodeset2 = {}
            link_set2 = {}
            try:
                cursor = db.cursor()
                sql = "select name ,createtime,num,description,public,creater from tb_mydata where id= " + res_hp["h"]._properties["myID"]
                cursor.execute(sql)
                result = cursor.fetchall()
                for i in result:
                    name=res_hp['h']._properties["name"]
                    id=str(res_hp["h"].id)
                    label= i[0]
                    temp = {}
                    value = name + "(" + label + ")"
                    temp["value"] = value
                    temp["id"] = id
                    temp["resource"]="华谱"
                    res1.append(temp)
            except Exception as e:
                print("查询失败")
    session.close()
    session_hp.close()

    return json.dumps(res1, ensure_ascii=False)
