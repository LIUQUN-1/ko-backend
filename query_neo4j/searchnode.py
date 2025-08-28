import re
import codecs, sys
from collections import Counter
import json
import sys
import time
from urllib.parse import quote, unquote
import random
from neo4j import GraphDatabase

import argparse

import json


def main(request):
    name=request.GET["name"]

    print(name)
    # driver = GraphDatabase.driver("bolt://114.213.233.177:8687", auth=("neo4j", "DMiChao"))
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    query_word='''
        MATCH (h:Wikipedia) where h.name ={arg_1}  OPTIONAL match (h)-[r:Contain]-(t:Wikipedia) return h, r, t limit 50 union 
        MATCH (h:Wikipedia) where h.name = {arg_1}  OPTIONAL match (h)-[r:belong]-(t:Category) return h, r, t limit 100 union  
        MATCH (h:Wikipedia)-[:belong]-(t:Category) 
        where h.name = {arg_1}  
        OPTIONAL match p=allShortestPaths((t)-[:Sub|belong*..10]-())  
        with relationships(p) as rels  
        unwind rels as rel  
        return startNode(rel) as h ,rel as r, endNode(rel) as t limit 200
        '''
    results_place = session.run(query_word, parameters={"arg_1": name})
    # session.close()
    nodeid_list=[]#用于去重
    linkid_list=[]#用于去重
    sum_list={}#总数据
    nodes=[]
    relationships =[]
    for res in results_place:
        node_set={}
        if  res["h"] is not None:
            if res['h'].id not in nodeid_list :
                nodeid_list.append(res['h'].id)
                node_set["id"]=str(res['h'].id)
                node_set["labels"]=list(set(res['h'].labels))
                node_set["properties"]=res['h']._properties

        if  res["t"] is not None:
            if res['t'].id not in nodeid_list:
                nodeid_list.append(res['t'].id)
                node_set["id"]=str(res['t'].id)
                node_set["labels"]=list(set(res['t'].labels))
                node_set["properties"]=res['t']._properties

        if len(node_set)!=0:#可能会出现头尾节点都在nodeid_list里面导致nodeset为空
            nodes.append(node_set)

        link_set={}
        if res["r"] is not None:
            if res['r'].id not in linkid_list:
                link_set["id"]=str(res['r'].id)
                link_set["startNodeId"]=str(res["r"].nodes[0].id)
                link_set["endNodeId"]=str(res["r"].nodes[1].id)
                link_set["type"] = res["r"].type
                link_set["properties"] = res["r"]._properties
        if len(link_set)!=0:

            relationships.append(link_set)
    sum_list['nodes']=nodes
    sum_list['relationships']=relationships
    session.close()
    return json.dumps(sum_list, ensure_ascii=False)