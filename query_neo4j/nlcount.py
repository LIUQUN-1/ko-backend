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


    #KO数据库
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    # driver = GraphDatabase.driver("bolt://www.zhonghuapu.com:8687", auth=("neo4j", "DMiChao"))

    session = driver.session()


    query_word='''
            MATCH (n) RETURN count(n)
        '''
    query_word1='''
            MATCH ()-[r]->() RETURN count(r)
        '''

    results_node = session.run(query_word)
    node_count=str(results_node.single().value())
    results_link = session.run(query_word1)
    link_count=str(results_link.single().value())

    #neo4j数据库
    driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    session_hp = driver_hp.session()
    query_word_hp = '''
        MATCH (n:People) RETURN count(n)
    '''

    results_node_hp = session_hp.run(query_word_hp)
    node_coun_hp=str(results_node_hp.single().value())

    sum_count=str(int(node_coun_hp)+int(node_count))

    sum_list={}
    # sum_list["nodeCount"]=node_count
    sum_list["nodeCount"]=sum_count
    sum_list["linkCount"]=link_count
    sum_list["firstCount"]="128"
    sum_list["secondCount"]="1139"
    session.close()
    session_hp.close()
    return json.dumps(sum_list, ensure_ascii=False)