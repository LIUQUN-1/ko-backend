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

    #KO数据库
    id=request.GET['id']
    name=request.GET['name']
    driver = GraphDatabase.driver("bolt://114.213.232.140:7687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    query_word="MATCH (n1:item), (n2:hyperNode) WHERE id(n1) = "+id+" AND n2.name = \'"+name+"\' and NOT (n1)-[:Same]->(n2)  MERGE (n1)-[:Same]->(n2) return n2"
    # query_word="MATCH (a:item)-[r:Same]-(b:hyperNode) WHERE id(a) = "+id+" DELETE r return b"
    results_node = session.run(query_word)
    hyperid=0
    for res in results_node:
        hyperid=res["n2"].id
    return json.dumps(hyperid, ensure_ascii=False)