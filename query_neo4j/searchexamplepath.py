import json

from neo4j import GraphDatabase
from datetime import datetime

corresponsed = {"BaiduBaike": "百度百科", "Category": "科目", "HAO": "HAO营销", "Wikipedia": "维基百科", "ownthink": "思知", "openkg": "openkg",
                "selfCreate": "自建节点", "YongleCanon": "永乐大典", "hyperNode": "超点", "HAO": "HAO营销", "ScholarCSKG": "ScholarCSKG", "MAKG": "MAKG",
                "Journals": "Journals", "ConceptNet": "ConceptNet", "CONFInstances": "CONFInstances", "CONF Series": "CONF Series", "Author": "Author",
                "Affiliations": "Affiliations"}


def getPath(name1, name2):
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    id1 = str(getID(name1, session))
    id2 = str(getID(name2, session))
    # 创建节点
    query_word2 = '''
    match (n1),(n2)
    where id(n1)=''' + id1 + ''' and id(n2)=''' + id2 + ''' 
    with n1,n2 limit 1
    match p = shortestPath((n1) - [*] - (n2))
    UNWIND nodes(p) AS node
    UNWIND relationships(p) AS rel
    RETURN node,rel limit 50
    '''

    res = session.run(query_word2)
    relations = []
    for record in res:
        relations.append(record['rel'])
    relations = set(relations)
    ans = {}
    path = []
    for res in relations:
        node0 = res.nodes[0]
        node1 = res.nodes[1]
        if node0._properties["name"] != node1._properties["name"]:
            aRel = {}
            aRel["subj"] = {"name": node0._properties["name"], "info": ""}
            aRel["r"] = {"desc": "", "type": res.type}
            aRel["obj"] = {"name": node1._properties["name"], "info": ""}
            path.append(aRel)
    ans["path"] = path
    ans["pair"] = []
    ans["pair"].append(name1)
    ans["pair"].append(name2)
    session.close()
    return json.dumps(ans, ensure_ascii=False)


def getID(name, session):
    query_word1 = '''
                match(n:hyperNode) where n.name={arg_1}
                return count(n) as num ,id(n) as id limit 1
            '''
    results_node1 = session.run(query_word1, parameters={"arg_1": name})

    query_word = '''
                    match(n:item) where n.name={arg_1}
                    return n
                '''
    results_node = session.run(query_word, parameters={"arg_1": name})

    results_nodeTmp = session.run(query_word, parameters={"arg_1": name})
    reSource = []
    reUrl = []
    reDes = []
    hyperSource = ""
    hyperUrl = ""
    hyperDesc = ""
    for res in results_node:
        # 处理label

        for i in res["n"].labels:
            if i in corresponsed.keys():
                reSource.append(corresponsed[i])
                # hyperDataChild["source"]=corresponsed[i]
        # 处理url
        try:
            if "haodaka" not in res["n"]._properties["url"]:
                reUrl.append(res["n"]._properties["url"])
                # hyperDataChild["url"]=res["n"]._properties["url"]
        except KeyError as e:
            print()
            # hyperDataChild["url"]="https://ko.zhonghuapu.com"
        # 处理des\url
        try:
            reDes.append(res["n"]._properties["des"])
            # hyperDataChild["des"]=res["n"]._properties["des"]
        except KeyError as e:
            print()
            # hyperDataChild["des"]="无"

    # 获取来源
    for i in list(set(reSource)):
        hyperSource += i + "；"
    hyperSource = hyperSource[:-1]
    # 获取链接
    for i in list(set(reUrl)):
        hyperUrl += i + "；"
    hyperUrl = hyperUrl[:-1]
    # 获取描述
    for i in list(set(reDes)):
        hyperDesc += i + "||"
    hyperDesc = hyperDesc[:-1]

    res_len = len(list(results_nodeTmp))

    # 库中没有这个点
    if res_len == 0:
        return -1
    # 库中有这个点
    else:

        # 库中没有超点，建立超点
        if results_node1.peek() is None:
            if res_len > 1:
                current_date = datetime.now().date()
                # 将日期格式化为整数
                formatted_date = int(current_date.strftime('%Y%m%d'))
                query_word3 = "CREATE (n:hyperNode:item) set n.url=\"" + hyperUrl + "\", n.desc=\"" + hyperDesc + "\",n.source=\"" + hyperSource + "\",n.name=\"" + name + "\",n.timestamp=" + str(
                    formatted_date) + "  with n MATCH (a:item ), (b:hyperNode) where a.name=n.name and id(a)<>id(n) and  id(b)=id(n)  MERGE (a)-[r:Same]->(b) return id(b) as  id"
                results_node3 = session.run(query_word3, parameters={"arg_1": name})
                id2 = 0
                for i in results_node3:
                    id2 = i["id"]
                return id2
            else:  # "只有一个点 返回这个点id"
                query_word2 = '''
                    match(n:item) where n.name={arg_1} return id(n) as id limit 1
                    '''
                results_node2 = session.run(query_word2, parameters={"arg_1": name})
                id1 = 0
                for i in results_node2:
                    id1 = i["id"]
                return id1
        # 库中有超点，返回超点id
        else:
            id = 0
            for i in results_node1:
                id = i["id"]
            return id

    session3.close()
    return -1

def main(request):
    start_node = request.GET["startNode"]
    end_node = request.GET["endNode"]

    return getPath(start_node, end_node)
