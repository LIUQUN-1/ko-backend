from neo4j import GraphDatabase
import json
import time
uri = "bolt://114.213.232.140:17687"
username = "neo4j"
password = "DMiChao"

def create_node_if_not_exists(node_name,session):
    query = f"MATCH (node) WHERE node.name = '{node_name}' RETURN ID(node) AS node_id, node"
    result = session.run(query)
    node_list = []
    node_content= []
    url = "https://ko.zhonghuapu.com"
    current_time = time.localtime()
    current_time = time.strftime("%Y%m%d",current_time)
    print(result.peek())
    if result.peek() is not None:
        # 如果查询结果非空，表示找到了节点
        for record in result:
            node_id = record['node_id']
            node = record['node']
            properties = dict(node.items())
            node_list.append(node_id)
            node_content.append(properties)
        return 0,[node_list,node_content]
    else:
        query_create = (
            "CREATE (n:item:gpt {name: $name,url: $url, timestamp: $timestamp}) "
            "RETURN n"
        )
        result_create = session.run(query_create, name=node_name,url=url,timestamp=current_time)
        created_node = result_create.single()["n"]
        return 1,created_node.id

def create_relationship(id1, id2,existing_node,session):

    query = (
        f"MATCH (n1)-[:Same]-(n4) WHERE id(n1) = {id1} "
        f"MATCH (n2)-[:Same]-(n5) WHERE id(n2) = {id2} "
        f"MATCH (n3) WHERE id(n3) = {existing_node} "
        "MERGE (n4)-[:Contain]->(n3)"
        "MERGE (n5)-[:Contain]->(n3)"
    )
    session.run(query)

# request = [4,5,["汕头大学"]]
def main(request):
    uri = "bolt://114.213.232.140:17687"
    username = "neo4j"
    password = "DMiChao"

    # id1 = request[0]
    # id2 = request[1]
    # node_names = request[2]


    id1 = request.POST['id1']
    id2 = request.POST['id2']
    node_names=[]
    node_names.append(request.POST['name'])
    print(node_names)
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            for node_name in node_names:
                len_node = 0
                state, existing_node = create_node_if_not_exists(node_name,session)
                if state == 0:
                    # print(f"Node '{node_name}' already exists with ID {existing_node}")
                    node_l = existing_node[0]
                    node_c = existing_node[1]
                    for id, content in zip(node_l, node_c):
                        if len(str(content)) > len_node:
                            len_node = len(str(content))
                            link_node_id = id
                    create_relationship(id1, id2, link_node_id,session)
                    return json.dumps("success", ensure_ascii=False)
                else:
                    print(f"Node '{node_name}' created with ID {existing_node}")
                    create_relationship(id1, id2, existing_node,session)
                    return json.dumps("success", ensure_ascii=False)
