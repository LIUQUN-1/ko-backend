from py2neo import Node,Relationship,Graph,Path,Subgraph
from py2neo import NodeMatcher,RelationshipMatcher
from neo4j import GraphDatabase
import os
import json
import neo4j

def create_entity_edge(tx, item, label):
    query_check = f"MATCH (head:{label} {{url: $head}}), (tail:{label} {{url: $tail}}) RETURN count(head) + count(tail) AS count"
    result_check = tx.run(query_check, head=item["head"], tail=item["tail"])
    count = result_check.single()["count"]

    if count != 2:
        print("Error: Nodes not found")
        return 1
    query = f"MERGE (head:{label}  {{url: $head}})\
    MERGE (tail:{label}  {{url: $tail}})\
    MERGE (head)-[r:relation {{rel_type: $name}}]-(tail)\
    SET r.description = $description, r.label_set = $label_set\
    "
    tx.run(query, head=item["head"], tail=item["tail"], name=item["edge_name"], description=item["edge_des"], label_set=item["edge_label_set"])
    return 0

def create_entity_node(tx, item, label):
    query = f"MERGE (n:{label} {{name: $name}}) \
    ON CREATE SET n.url = $url, n.description = $description, n.timestamp = $timestamp \
    RETURN n \
    "
    tx.run(query, name=item["name"], url=item["url"], description=item["des"], timestamp=item["timestamp"])

def get_entity_node(tx, name, label):
    query = f"MATCH (n:{label} {{name: $name}}) RETURN n"
    result = tx.run(query, name=name)
    return result.data()
def main(request):
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    file_obj = request.FILES.get('file', None)
    params = request.POST.get('param').split(',')
    head_path = os.path.join('F:/', 'upload/')
    file_path = os.path.join(head_path,  file_obj.name)
    try:
        with open(file_path, 'wb') as f:
            for chunk in file_obj.chunks():
                f.write(chunk)
            f.close()
        with open(file_path, "r", encoding='utf-8') as file:
            data = json.load(file)
            file.close()
        os.remove(file_path)
    except (IOError, json.JSONDecodeError) as e:
        error_message = {
            "error": "文件失败打开失败",
            "message": str(e)
        }
        return json.dumps(error_message)
    try:
        for param in params:
            with driver.session() as session:
                for item in data:
                    if item.get('name',-1) != -1:
                        session.write_transaction(create_entity_node, item, param)
                        node = session.read_transaction(get_entity_node, item["name"], param)
                    elif item.get('head', -1)!= -1:
                        error = session.write_transaction(create_entity_edge, item, param)
                        if error == 1:
                            error_message = {
                                "error": "请先添加节点",
                            }
                            return json.dumps(error_message)
    except neo4j.exceptions.ServiceUnavailable as e:
        error_message = {
            "error": "文件格式错误",
            "message": str(e)
        }
        print(str(e))
        return json.dumps(error_message)
    driver.close()
    message = {
        "message": "上传成功",
    }
    return json.dumps(message)