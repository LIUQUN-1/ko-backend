# @Author : Xie Zexian
# @Description TODO
# @Time : 2023/11/16 21:30
from neo4j import GraphDatabase
import requests
import json


def get_hyper_node(session, node_name):
    cql = "MATCH (n:hyperNode) WHERE n.name={node_name} return id(n) AS id, n.name AS name, n.des AS des"
    result = session.run(cql, parameters={"node_name": node_name})
    return result.data()


def main(request):
    start_node = request.GET['startNode']
    end_node = request.GET['endNode']
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    node_info = {}
    node_info['startNode'] = get_hyper_node(session, start_node)
    node_info['endNode'] = get_hyper_node(session, end_node)
    return json.dumps(node_info)
