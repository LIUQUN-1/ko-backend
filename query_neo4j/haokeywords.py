# import re
# import codecs, sys
# from collections import Counter
# import json
# import sys
# import time
# from urllib.parse import quote, unquote
# import random
# from neo4j import GraphDatabase
# from datetime import datetime
# import argparse
#
# import json
# def search(name):
#     driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
#     session3 = driver.session()
#     query_word1='''
#                 match(n:hyperNode) where n.name={arg_1}
#                 return count(n) as num ,id(n) as id limit 1
#             '''
#     results_node1 = session3.run(query_word1, parameters={"arg_1": name})
#
#     query_word='''
#                     match(n:item) where n.name={arg_1}
#                     return count(n)
#                 '''
#     results_node = session3.run(query_word, parameters={"arg_1": name})
#     node_count=results_node.single().value()
#
#     #库中没有这个点
#     if node_count == 0:
#         return -1
#     # 库中有这个点
#     else:
#         #库中没有超点，建立超点
#         if results_node1.peek() is None:
#             if node_count>1:
#                 print("建立超点 返回超点id")
#                 current_date = datetime.now().date()
#                 # 将日期格式化为整数
#                 formatted_date = int(current_date.strftime('%Y%m%d'))
#                 query_word3="CREATE (n:hyperNode:item) set n.name=\""+name+"\",n.timestamp="+str(formatted_date)+"  with n MATCH (a:item ), (b:hyperNode) where a.name=n.name and id(a)<>id(n) and  id(b)=id(n)  MERGE (a)-[r:Same]->(b) return id(b) as  id"
#                 results_node3 = session3.run(query_word3, parameters={"arg_1": name})
#                 id2=0
#                 for i in results_node3:
#                     id2=i["id"]
#                 return id2
#             else:#"只有一个点 返回这个点id"
#                 query_word2='''
#                     match(n:item) where n.name={arg_1} return id(n) as id limit 1
#                     '''
#                 results_node2 = session3.run(query_word2, parameters={"arg_1": name})
#                 id1=0
#                 for i in results_node2:
#                     id1=i["id"]
#                 return id1
#         #库中有超点，返回超点id
#         else:
#             id=0
#             for i in results_node1:
#                 id=i["id"]
#             return id
#     return -1
#
# if __name__ == '__main__':
#
#     # driver = GraphDatabase.driver("bolt://114.213.233.177:7687", auth=("neo4j", "DMiChao"))
#     # session = driver.session()
#     # query_word1='''
#     #         match(n:item) where n.name={arg_1}
#     #         return count(n) as num ,id(n) as id limit 1
#     #     '''
#     # results_node1 = session.run(query_word1, parameters={"arg_1": "吴信东"})
#     #
#     # for i in results_node1:
#     #     print(type(i["id"]))
#     num=search("吴信东")
#     print(num)
