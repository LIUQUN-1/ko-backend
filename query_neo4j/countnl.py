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
import pymysql
import json

def main(request):
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
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

    # driver_17687 = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    driver_17687 = GraphDatabase.driver("bolt://114.213.232.140:47687", auth=("neo4j", "123456")) # 分布式小库
    session_17687 = driver_17687.session()
    query_word_17687='''
            MATCH (n) RETURN count(n)
        '''
    query_word1_17687='''
            MATCH ()-[r]->() RETURN count(r)
        '''
    results_node_17687 = session_17687.run(query_word_17687)
    node_count_17687=str(results_node_17687.single().value())

    results_link_17687 = session_17687.run(query_word1_17687)
    link_count_17687=str(results_link_17687.single().value())


    # 文件数据库
    driver_dc = GraphDatabase.driver("bolt://114.213.232.140:37687", auth=("neo4j", "123456"))
    session_dc = driver_dc.session()
    query_word_dc = '''
        MATCH (n) RETURN count(n)
    '''
    query_word1_dc='''
            MATCH ()-[r]->() RETURN count(r)
        '''
    results_node_dc = session_dc.run(query_word_dc)
    node_coun_dc = str(results_node_dc.single().value())

    results_link_dc = session_dc.run(query_word1_dc)
    link_count_dc=str(results_link_dc.single().value())

    #neo4j数据库
    # driver_hp = GraphDatabase.driver("bolt://www.zhonghuapu.com:7687", auth=("neo4j", "hfut701DMiChp"))
    driver_hp = GraphDatabase.driver("bolt://114.213.234.179:7687", auth=("neo4j", "hfut701DMiChp"))
    session_hp = driver_hp.session()
    query_word_hp = '''
        MATCH (n:People) RETURN count(n)
    '''
    results_node_hp = session_hp.run(query_word_hp)
    node_coun_hp=str(results_node_hp.single().value())
    # 
    # sum_count=str(int(node_coun_hp)+int(node_count) + int(node_count_17687))
    # link_count = str(int(link_count)+int(link_count_17687))
    #
    sum_count=str(int(node_count) + int(node_count_17687) + int(node_coun_dc) + int(node_coun_hp))
    link_count = str(int(link_count)+int(link_count_17687) + int(link_count_dc))
    sum_list={}
    # sum_list["nodeCount"]=node_count
    sum_list["nodeCount"]=sum_count
    sum_list["linkCount"]=link_count
    session.close()
    session_hp.close()
    db = pymysql.connect(host='www.zhonghuapu.com',
                         user='koroot',
                         password='DMiC-4092',
                         database='db_hp',
                         charset='utf8')

    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    try:
        cursor = db.cursor()
        sql="UPDATE ko_num SET node_count={}, link_count={}, first_count={}, second_count={}".format(sum_count,link_count,'128','1139')
        cursor.execute(sql)
        result = cursor.fetchall()
        db.commit()
    except Exception as e:
        print("查询失败")
    db.close()
    return json.dumps(sum_list, ensure_ascii=False)