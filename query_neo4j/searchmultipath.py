# coding:utf-8
from datetime import datetime
from neo4j import GraphDatabase
import json
import pymysql
import copy
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque

translation_dict = {
    "name": "名称",
    "url": "链接",
    "timestamp": "时间",
    "source": "来源",
    "desc": "描述",
    "des": "描述"
}
db = pymysql.connect(host='www.zhonghuapu.com',
                     user='koroot',
                     password='DMiC-4092',
                     database='db_hp',
                     charset='utf8')
cursor = db.cursor()
cursor.execute("SELECT VERSION()")
corresponsed = {"BaiduBaike": "百度百科", "Category": "科目", "HAO": "HAO营销", "Wikipedia": "维基百科",
                "ownthink": "思知",
                "openkg": "openkg", "selfCreate": "自建节点", "YongleCanon": "永乐大典", "hyperNode": "超点",
                "HAO": "HAO营销",
                "ScholarCSKG": "ScholarCSKG", "MAKG": "MAKG", "Journals": "Journals", "ConceptNet": "ConceptNet",
                "CONFInstances": "CONFInstances", "CONF Series": "CONF Series", "Author": "Author",
                "Affiliations": "Affiliations", "Thingo": "思高",
                "People": "华谱家谱"}
relResponsed = {'employment': '雇佣', 'colleague': '同事', 'hometion': '同乡', 'hometown': '地域',
                'eduback': '教育背景', 'alumni': '校友',
                'teachmate': '师生', 'collaborator': '合作', 'domain': '领域', 'awards': '荣誉'}


def main(request):
    nodeid_list = []  # 用于去重
    linkid_list = []  # 用于去重
    sum_list = {}  # 总数据

    sum_list["type"] = "force"
    sum_list["categories"] = [
        {
            "name": "条目(来源一)",
            "keyword": {},
            "base": "HTMLElement"
        },
        {
            "name": "二层科目",
            "keyword": {},
            "base": "WebGLRenderingContext"
        },
        {
            "name": "其他科目",
            "keyword": {},
            "base": "SVGElement",
            "itemStyle": {
                "normal": {
                    "color": 'rgba(255,186,44,0.95)'
                }
            }
        },
        {
            "name": "根科目",
            "keyword": {},
            "base": "CSSRule",
            "itemStyle": {
                "normal": {
                    "color": 'rgb(236,147,158)'
                }
            }
        },
        {
            "name": "条目(来源二)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(78,189,236)'
                }
            }
        },
        {
            "name": "一层科目",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": 'rgb(52,176,143)'
                }
            }
        },
        {
            "name": "条目(来源三)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#91D8E4'  # 5994a0
                }
            }
        },
        {
            "name": "家谱",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#408E91'  # 5994a0
                }
            }
        },
        {
            "name": "自建节点",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#738fa0'  # 5994a0
                }
            }
        },
        {
            "name": "条目(来源四)",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#8376a0'  # 5994a0
                }
            }
        },
        {
            "name": "超点",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#FF7F00'  # 5994a0
                }
            }
        },
        {
            "name": "HAO",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#32CD99'  # 5994a0
                }
            }
        },
        {
            "name": "百度百科",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#EAADEA'  # 5994a0
                }
            }
        },
        {
            "name": "大模型",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#5994a0'  # 5994a0
                }
            }
        },
        {
            "name": "思高",
            "keyword": {},
            "itemStyle": {
                "normal": {
                    "color": '#88abda'  # 5994a0
                }
            }
        }
    ]

    # Get current time
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print('---------------', flush=True)
    print("当前时间：", formatted_time, flush=True)
    print('---------------', flush=True)

    # Retrieve parameters from request
    StartNode = request.GET["StartNode"]
    EndNode = request.GET["EndNode"]
    isMulti = request.GET["isMulti"]
    id1 = request.GET["id1"]
    id2 = request.GET["id2"]

    userId = request.GET.get("userId", 1)
    # Determine which database to use
    database = determine_database(StartNode, EndNode, id1, id2)
    print("database:",database, flush=True)
    if database == 'huapu':
        data = searchHP(StartNode, EndNode, isMulti, id1, id2, userId, sum_list, database)
        return data
    else:
        # Proceed with 'ko' database query
        # driver1 = "bolt://114.213.232.140:47687"
        # driver2 = "bolt://114.213.232.140:47688"
        # driver3 = "bolt://114.213.232.140:47689"
        # dirver_choice = random.choice([driver1, driver2, driver3])
        # dirver_choice = driver2
        # driver = GraphDatabase.driver(dirver_choice, auth=("neo4j", "123456"))

        driver4 = "bolt://114.213.232.140:17687"
        dirver_choice = driver4
        driver = GraphDatabase.driver(driver4, auth=("neo4j", "DMiChao"))
        print(f"本次使用的分布式图数据库是:{dirver_choice}", flush=True)

        session = driver.session()

        id1 = parse_id(id1)
        id2 = parse_id(id2)

        if id1 is not None:
            name1 = str(id1)
        else:
            name1 = str(getID(StartNode, session))
        if id2 is not None:
            name2 = str(id2)
        else:
            name2 = str(getID(EndNode, session))
        print("name1:",name1)
        print("name2:",name2)
        if name1 is None or name2 is None:
            # Handle cases where one or both nodes are not found
            backres = {'nodes': [], 'links': []}
            if name1 is None and name2 is None:
                pass  # Both nodes not found
            elif name1 is None:
                backres['nodes'].append(getNode(session, name2))
            elif name2 is None:
                backres['nodes'].append(getNode(session, name1))
            session.close()
            print("backres:",backres,flush=True)
            return json.dumps(backres, ensure_ascii=False)

        # Build query
        if isMulti == '0':
            backquery_word = f'''
                match (n1),(n2)
                where id(n1)={name1} and id(n2)={name2} 
                with n1,n2 limit 1
                match p = shortestPath((n1) - [*1..4] - (n2))
                WHERE NONE(node IN nodes(p) WHERE 'gpt' IN labels(node))
                UNWIND nodes(p) AS node
                UNWIND relationships(p) AS rel
                RETURN p,node, rel 
                '''
        else:
            backquery_word = f'''
                MATCH (n1),(n2)
                WHERE id(n1)={name1} and id(n2)={name2} 
                WITH n1,n2 LIMIT 1
                MATCH p = allshortestpaths((n1) - [*1..4] - (n2))
                WHERE NONE(node IN nodes(p) WHERE 'gpt' IN labels(node))
                UNWIND nodes(p) AS node
                UNWIND relationships(p) AS rel
                RETURN p,node, rel
                '''

        backresults_place = session.run(backquery_word)

        # Initialize parameters
        nodes = []
        links = []
        flag = 0

        # Call manage function to process query results
        backres = manage(backresults_place, sum_list, nodeid_list, nodes, linkid_list, links, flag)
        print("backres:",backres,flush=True)
        if len(backres['nodes']) == 0:
            # Handle cases where no path data is found
            if name1 == '-1' and name2 == '-1':
                pass
            elif name1 == '-1':
                backres['nodes'].append(getNode(session, name2))
            elif name1 != '-1' and name2 == '-1':
                backres['nodes'].append(getNode(session, name1))
            else:
                backres['nodes'].append(getNode(session, name1))
                backres['nodes'].append(getNode(session, name2))

        session.close()

        data = backres
        data["links"] = removeTwoway(data["links"])

        try:
            # Determine head and tail node IDs
            headNode_id = data["pathIds"][0][0]
            tailNode_id = data["pathIds"][0][-1]

            flag1 = False  # Whether the head node has redundant nodes
            flag2 = False  # Whether the tail node has redundant nodes
            for node in data["nodes"]:
                if int(node['id']) == headNode_id and node['category'] == 10:
                    flag1 = True
                if int(node['id']) == tailNode_id and node['category'] == 10:
                    flag2 = True

            data = remove_dupicate_paths(data, flag1, flag2)
            # Remove nodes not in pathIds
            path_node_ids = set(str(node_id) for path in data['pathIds'] for node_id in path)
            filtered_nodes = [node for node in data['nodes'] if node['id'] in path_node_ids]
            data['nodes'] = filtered_nodes

            data = remove_mid_dupicate_paths(data)

            # Find user's enhanced nodes
            enhancedNode, enhancedNodeBelong = searchEnhancedNode(name1, name2, userId)
            data['enhancedNode'] = enhancedNode
            data['enhancedNodeBelong'] = enhancedNodeBelong
            data = managePath(data)
            print("data1:",data,flush=True)
            return json.dumps(data, ensure_ascii=False)
        except IndexError:
            # Handle empty list exception
            enhancedNode, enhancedNodeBelong = searchEnhancedNode(name1, name2, userId)
            data['enhancedNode'] = enhancedNode
            data['enhancedNodeBelong'] = enhancedNodeBelong
            data = managePath(data)
            print("data2:",data,flush=True)
            return json.dumps(data, ensure_ascii=False)


# 判断两个人物是不是都是华谱的人物
def judge_hp_people(StartNode, EndNode, id1, id2):
    if id1 == -1 and id2 == -1:
        return False
    else:
        name1 = int(id1)
        name2 = int(id2)
    # 连接到华谱数据库
    driverHP = GraphDatabase.driver("bolt://114.213.234.179:7687", auth=("neo4j", "hfut701DMiChp"))
    sessionHP = driverHP.session()

    # 编写查询，检查两个指定 ID 的节点是否匹配给定的人名
    query = f'''
        MATCH (n1:People), (n2:People)
        WHERE id(n1) = {name1} AND id(n2) = {name2}
        RETURN n1.name = "{StartNode}" AND n2.name = "{EndNode}" AS result
    '''
    print(f"判断数据是否在华普中 查询语句:{query}", flush=True)

    try:
        # 执行查询并获取结果
        result = sessionHP.run(query).single()
        sessionHP.close()
        driverHP.close()

        # 根据查询结果返回
        return result["result"] if result else False
    except Exception as e:
        # 处理可能的错误
        print(f"Error occurred: {e}")
        sessionHP.close()
        driverHP.close()
        return False


def searchHP(StartNode, EndNode, isMulti, id1, id2, userId, sum_list, database):
    id1 = parse_id(id1)
    id2 = parse_id(id2)

    # Connect to Huapu database
    driverHP = GraphDatabase.driver("bolt://114.213.234.179:7687", auth=("neo4j", "hfut701DMiChp"))
    sessionHP = driverHP.session()

    # if id1 is not None:
    #     name1 = str(id1)
    # else:
    #     name1 = str(getIDHP(StartNode, sessionHP))
    # if id2 is not None:
    #     name2 = str(id2)
    # else:
    #     name2 = str(getIDHP(EndNode, sessionHP))

    name1 = str(getIDHP(StartNode, sessionHP))
    name2 = str(getIDHP(EndNode, sessionHP))

    if name1 is None or name2 is None:
        # Return empty data or appropriate response
        sessionHP.close()
        return json.dumps({"nodes": [], "links": []}, ensure_ascii=False)

    # Build query
    if isMulti == '0':
        backquery_word = f'''
            MATCH (n1),(n2)
            WHERE id(n1)={name1} AND id(n2)={name2}
            WITH n1, n2 LIMIT 1
            MATCH p = shortestPath((n1) -[*1..5]- (n2))
            WHERE NONE(node IN nodes(p) WHERE 'gpt' IN labels(node))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN p, node, rel
        '''
    else:
        backquery_word = f'''
            MATCH (n1),(n2)
            WHERE id(n1)={name1} AND id(n2)={name2}
            WITH n1, n2 LIMIT 1
            MATCH p = allshortestpaths((n1) -[*]- (n2))
            UNWIND nodes(p) AS node
            UNWIND relationships(p) AS rel
            RETURN p, node, rel
        '''
        # backquery_word = f'''
        #     MATCH (n1),(n2)
        #     WHERE id(n1)={name1} AND id(n2)={name2}
        #     WITH n1, n2 LIMIT 1
        #     MATCH p = allshortestpaths((n1) -[*1..5]- (n2))
        #     UNWIND nodes(p) AS node
        #     UNWIND relationships(p) AS rel
        #     RETURN p, node, rel
        # '''
    print(f"在华普中查询对应关系:{backquery_word}", flush=True)

    backresults_place = sessionHP.run(backquery_word)
    # 依用户访问节点的权限重整初次查询结果图
    filtered_results = get_filtered_path(sessionHP, backresults_place, userId, name1, name2)
    # Initialize parameters
    nodeid_list = []
    linkid_list = []
    nodes = []
    links = []
    flag = 0

    # Call manage function to process query results
    # backres = manage(backresults_place, sum_list, nodeid_list, nodes, linkid_list, links, flag)
    backres = manage(filtered_results, sum_list, nodeid_list, nodes, linkid_list, links, flag)
    # if len(backres['nodes']) == 0:
    #     # Handle cases where no path data is found
    #     if name1 == '-1' and name2 == '-1':
    #         pass
    #     elif name1 == '-1':
    #         backres['nodes'].append(getNode(sessionHP, name2))
    #     elif name1 != '-1' and name2 == '-1':
    #         backres['nodes'].append(getNode(sessionHP, name1))
    #     else:
    #         backres['nodes'].append(getNode(sessionHP, name1))
    #         backres['nodes'].append(getNode(sessionHP, name2))
    if len(backres['nodes']) == 0:
        # Handle cases where no path data is found
        if name1 == '-1' and name2 == '-1':
            pass
        elif name1 == '-1':
            backres['nodes'].append(getNodeHP(sessionHP, name2))
        elif name1 != '-1' and name2 == '-1':
            backres['nodes'].append(getNodeHP(sessionHP, name1))
        else:
            backres['nodes'].append(getNodeHP(sessionHP, name1))
            backres['nodes'].append(getNodeHP(sessionHP, name2))


    sessionHP.close()

    data = backres
    data["links"] = removeTwoway(data["links"])
    tmp = data["links"]
    print(f"查询到的两个人的关系:{tmp}", flush=True)

    try:
        # Determine head and tail node IDs
        headNode_id = data["pathIds"][0][0]
        tailNode_id = data["pathIds"][0][-1]

        flag1 = False  # Whether the head node has redundant nodes
        flag2 = False

        for node in data["nodes"]:
            if int(node['id']) == headNode_id and node['category'] == 10:
                flag1 = True
            if int(node['id']) == tailNode_id and node['category'] == 10:
                flag2 = True

        # Remove duplicate paths
        data = remove_dupicate_paths(data, flag1, flag2)
        path_node_ids = set(str(node_id) for path in data['pathIds'] for node_id in path)
        filtered_nodes = [node for node in data['nodes'] if node['id'] in path_node_ids]
        data['nodes'] = filtered_nodes

        # Find user's enhanced nodes (Assuming you have logic for this)
        enhancedNode = {'1': [], '2': [], '3': []}
        enhancedNodeBelong = {'1': [], '2': [], '3': []}
        data['enhancedNode'] = enhancedNode
        data['enhancedNodeBelong'] = enhancedNodeBelong

        # Manage paths if needed
        # data = managePath(data)
        print("data:",data)
        return json.dumps(data, ensure_ascii=False)

    except IndexError:
        # Handle empty list exception
        # enhancedNode, enhancedNodeBelong = searchEnhancedNode(name1, name2, userId)
        # data['enhancedNode'] = enhancedNode
        # data['enhancedNodeBelong'] = enhancedNodeBelong
        # data = managePath(data)
        return json.dumps(data, ensure_ascii=False)


# 处理路径中不应该存在的关系
def managePath(data):
    valid_nodes = []
    for i in data["nodes"]:
        if i["id"] not in valid_nodes:
            valid_nodes.append(i["id"])
    links = data['links']
    filtered_links = [link for link in links if link['target'] in valid_nodes and link['source'] in valid_nodes]
    data['links'] = filtered_links
    return data


# 找用户的增强节点
def searchEnhancedNode(headId, tailId, userId):
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    query = '''
        MATCH (h)-[r]-(t:gpt)
        WHERE id(h) = ''' + headId + ''' OR id(h) = ''' + tailId + '''
        RETURN h,r,t
    '''
    result = session.run(query)
    enhancedNode = {'1': [], '2': [], '3': []}
    enhancedNodeBelong = {'1': [], '2': [], '3': []}
    tripleRes = []
    for res in result:
        # 所有人可见的点
        if 'OA' in res['t']._properties.keys() and res['t']._properties['OA'] == 1:
            tripleRes.append([res['t']._properties['name'], res['r']._properties, res['h'].id])
            temp = []
            temp.append(res['t']._properties['name'])
            temp.append(res['r']._properties['des'])
            temp.append(res['h'].id)
            if str(res['h'].id) == headId and temp not in enhancedNode["1"]:
                enhancedNode["1"].append(temp)
            if str(res['h'].id) == tailId and temp not in enhancedNode["2"]:
                enhancedNode["2"].append(temp)

            # 判断该节点是OA的还是toUser的
            if 'toUser' in res['t']._properties.keys():
                if userId in res['t']._properties['toUser']:
                    enhancedNodeBelong["3"].append(res['t']._properties['name'])
                else:
                    enhancedNodeBelong["1"].append(res['t']._properties['name'])
            else:
                enhancedNodeBelong["1"].append(res['t']._properties['name'])

        # 限定用户的点
        if 'toUser' in res['t']._properties.keys():
            if userId in res['t']._properties['toUser']:
                tripleRes.append([res['t']._properties['name'], res['r']._properties, res['h'].id])
                temp = []
                temp.append(res['t']._properties['name'])
                temp.append(res['r']._properties['des'])
                temp.append(res['h'].id)
                if str(res['h'].id) == headId and temp not in enhancedNode["1"]:
                    enhancedNode["1"].append(temp)
                if str(res['h'].id) == tailId and temp not in enhancedNode["2"]:
                    enhancedNode["2"].append(temp)

                # 判断该节点是OA的还是toUser的
                if 'OA' in res['t']._properties.keys():
                    if res['t']._properties['OA'] == 1:
                        enhancedNodeBelong["3"].append(res['t']._properties['name'])
                    else:
                        enhancedNodeBelong["2"].append(res['t']._properties['name'])
                else:
                    enhancedNodeBelong["2"].append(res['t']._properties['name'])

    # 去重
    if len(enhancedNodeBelong["1"]) != 0:
        enhancedNodeBelong["1"] = list(set(enhancedNodeBelong["1"]))
    if len(enhancedNodeBelong["2"]) != 0:
        enhancedNodeBelong["2"] = list(set(enhancedNodeBelong["2"]))
    if len(enhancedNodeBelong["3"]) != 0:
        enhancedNodeBelong["3"] = list(set(enhancedNodeBelong["3"]))

    # 提取'1'和'2'对应的value数组中的第一个值：name，存储在集合中
    first_values_1 = {item[0] for item in enhancedNode['1']}
    first_values_2 = {item[0] for item in enhancedNode['2']}

    # 检查是否有重合的元素，即在第一个值集合中相等的元素
    intersections = first_values_1.intersection(first_values_2)
    # 遍历'1'对应的value数组
    for intersection in intersections:
        new = []
        for element in enhancedNode['1']:
            # 如果元素的第一个值在交集中，则将其移动到'3'中，并从'1'中删除
            if element[0] in intersection:
                enhancedNode['1'].remove(element)
                new = element
        # 遍历'2'对应的value数组
        for element in enhancedNode['2']:
            # 如果元素的第一个值在交集中，则将其移动到'3'中，并从'2'中删除
            if element[0] in intersection:
                enhancedNode['2'].remove(element)
                new.extend(element)
        enhancedNode['3'].append(new)
    session.close()

    enhancedNodeBelong = getEnhancedNodeRelation(enhancedNode, enhancedNodeBelong, tripleRes)

    return enhancedNode, enhancedNodeBelong


# 重新获取oa或者toUser节点的关联关系
def getEnhancedNodeRelation(enhanceNode1, enhanceNodeBelong, tripleRes):
    temp = {"1": [], "2": [], "3": []}
    res = {"1": [], "2": [], "3": []}
    for key, value in enhanceNode1.items():
        for array in value:
            if array:  # 检查数组是否为空
                first_element = array[0]
                temp[key].append(first_element)
    print(temp)
    for k, v in enhanceNodeBelong.items():
        for i in v:  # 遍历enhanceNodeBelong的每个数组的每个元素
            if i in temp["1"]:  # 只和起始超点连
                for j in enhanceNode1["1"]:
                    if i in j:
                        source = ""
                        reason = ""
                        for value in tripleRes:
                            if j[0] == value[0] and j[1] == value[1]['des'] and j[2] == value[2]:
                                if "source" in value[1].keys():
                                    source = value[1]['source']
                                if "reason" in value[1].keys():
                                    reason = value[1]['reason']
                        res[k].append([j[1], j[0], "", source, "", reason, ""])
            if i in temp["2"]:  # 只和结束超点连
                for j in enhanceNode1["2"]:
                    if i in j:
                        source = ""
                        reason = ""
                        for value in tripleRes:
                            if j[0] == value[0] and j[1] == value[1]['des'] and j[2] == value[2]:
                                if "source" in value[1].keys():
                                    source = value[1]['source']
                                if "reason" in value[1].keys():
                                    reason = value[1]['reason']
                        res[k].append(["", j[0], j[1], "", source, "", reason])
            if i in temp["3"]:  # 两个都连
                for j in enhanceNode1["3"]:
                    if i in j:
                        source1 = ""
                        source2 = ""
                        reason = ""
                        reason1 = ""
                        for value in tripleRes:
                            if j[0] == value[0] and j[1] == value[1]['des'] and j[2] == value[2]:
                                if "source" in value[1].keys():
                                    source1 = value[1]['source']
                                if "reason" in value[1].keys():
                                    reason = value[1]['reason']
                            if j[3] == value[0] and j[4] == value[1]['des'] and j[5] == value[2]:
                                if "source" in value[1].keys():
                                    source2 = value[1]['source']
                                if "reason" in value[1].keys():
                                    reason1 = value[1]['reason']
                        res[k].append([j[1], j[0], j[4], source1, source2, reason, reason1])

    return res


# 检查节点是否是超点
def isSuperNode(data, id):
    for node in data["nodes"]:
        if int(node['id']) == id and node['category'] == 10:
            return True
    return False


# 根据超点id找到待删的冗余节点id以及索引
def delete_nodeid_list(data, id, pathId):
    deleteNodeidList = []
    deletepathIdsindexList = []
    superNodename = ''
    for node in data["nodes"]:
        if int(node['id']) == id:
            superNodename = node["name"]
    for node in data["nodes"]:
        if node["name"] == superNodename and int(node["id"]) != id:
            deleteNodeidList.append(int(node["id"]))
    for deleteNodeid in deleteNodeidList:
        for index, nodeId in enumerate(pathId):
            if nodeId == deleteNodeid:
                deletepathIdsindexList.append(index)
                break
    return deleteNodeidList, deletepathIdsindexList


# 去重中间路径的超点的冗余节点
def remove_mid_dupicate_paths(data):
    datatemp = copy.deepcopy(data)
    for index1, pathId in enumerate(data["pathIds"]):
        SuperNodesid = []  # 存放已经处理过的SuperNode
        pathIdtemp = pathId
        boolSearch = True
        while (boolSearch and len(pathIdtemp) > 2):
            for index, nodeId in enumerate(pathIdtemp):
                if index != 0 and index != (len(pathIdtemp) - 1):
                    boolSearch = False
                    if isSuperNode(datatemp, nodeId) and nodeId not in SuperNodesid:
                        boolSearch = True
                        SuperNodesid.append(nodeId)
                        delete_Nodeid_List, deletepathIdsindexList = delete_nodeid_list(datatemp, nodeId, pathIdtemp)
                        # 更新数据中的nodes属性
                        data["nodes"] = [node for node in data["nodes"] if int(node["id"]) not in delete_Nodeid_List]
                        # 更新数据中的pathIds属性
                        # 更新数据中的pathNodes属性
                        data["pathIds"][index1] = [value for index, value in enumerate(data["pathIds"][index1]) if
                                                   index not in deletepathIdsindexList]
                        pathIdtemp = data["pathIds"][index1]
                        data["pathNodes"][index1] = [value for index, value in enumerate(data["pathNodes"][index1]) if
                                                     index not in deletepathIdsindexList]
                        # 更新数据中的links属性
                        for delete_nodeid in delete_Nodeid_List:
                            links = data["links"]
                            new_links = []
                            for link in links:
                                if (int(link["target"]) == delete_nodeid and int(link["source"]) == nodeId) or (
                                        int(link["source"]) == delete_nodeid and int(link["target"]) == nodeId):
                                    continue
                                elif int(link["target"]) == delete_nodeid:
                                    link["target"] = str(nodeId)
                                    new_links.append(link)
                                elif int(link["source"]) == delete_nodeid:
                                    link["source"] = str(nodeId)
                                    new_links.append(link)
                                else:
                                    new_links.append(link)

                            data["links"] = new_links
                        break
    return data


# 去重头尾节点的超点的冗余节点
def remove_dupicate_paths(data, flag1, flag2):
    head_node_id = data["pathIds"][0][0]
    tail_node_id = data["pathIds"][0][-1]
    head_node_name = data["pathNodes"][0][0]
    tail_node_name = data["pathNodes"][0][-1]
    head_and_tail_id = [head_node_id, tail_node_id]

    remove_head_node_id = []  # 头超点周围的冗余节点id
    remove_tail_node_id = []  # 尾超点周围的冗余节点id

    for node in data["nodes"]:
        if flag1:
            if node["name"] == head_node_name and int(node["id"]) != head_node_id:
                remove_head_node_id.append(int(node["id"]))
        if flag2:
            if node["name"] == tail_node_name and int(node["id"]) != tail_node_id:
                remove_tail_node_id.append(int(node["id"]))

    # 待删节点id
    list_id = remove_head_node_id + remove_tail_node_id

    # 更新数据中的nodes属性
    data["nodes"] = [node for node in data["nodes"] if int(node["id"]) not in list_id]

    # 更新数据中的pathIds属性
    # 更新数据中的pathNodes属性
    if flag1:
        data["pathIds"] = [[item[0], *item[2:]] for item in data["pathIds"]]
        data["pathNodes"] = [[item[0], *item[2:]] for item in data["pathNodes"]]
    if flag2:
        data["pathIds"] = [[*item[:-2], item[-1]] for item in data["pathIds"]]
        data["pathNodes"] = [[*item[:-2], item[-1]] for item in data["pathNodes"]]
    # 更新数据中的links属性
    # 改边
    for link in data["links"]:
        if flag1:
            if int(link["target"]) in remove_head_node_id:
                link["target"] = str(head_node_id)
            if int(link["source"]) in remove_head_node_id:
                link["source"] = str(head_node_id)
        if flag2:
            if int(link["target"]) in remove_tail_node_id:
                link["target"] = str(tail_node_id)
            if int(link["source"]) in remove_tail_node_id:
                link["source"] = str(tail_node_id)
    # 去除改边引起的自旋头尾节点和重复路径
    unique_links = []
    seen = set()

    for link in data["links"]:
        target = link["target"]
        source = link["source"]
        link_tuple = (target, source)
        if target != source or int(target) not in head_and_tail_id:
            if link_tuple not in seen:
                seen.add(link_tuple)
                unique_links.append(link)

    data["links"] = unique_links

    return data


def manage(results_place, sum_list, nodeid_list, nodes, linkid_list, links, flag):
    pathTriple = []
    pathNodes = []
    pathIds = []
    relations = []
    paths = []
    pathRelations = []
    for record in results_place:
        paths.append(record['p'])

        relations.append(record['rel'])
    paths = set(paths)
    for path in paths:
        nodename = []
        ids = []
        for i in path.nodes:
            nodename.append(i._properties["name"])
            ids.append(i.id)
        if nodename not in pathNodes:
            pathNodes.append(nodename)
            pathIds.append(ids)
        for j in path.relationships:
            pathRelations.append(j)

    pathRelations = set(pathRelations)

    for res in pathRelations:
        tripleName = []
        node0 = res.nodes[0]
        node1 = res.nodes[1]
        typeRes = res.type

        if "label_set" in res._properties:
            typeRes = res._properties['label_set']
        typeRes = typeRes.split(',')[0]
        # 华谱里面的关系表示为Kinship
        if res.type == "Kinship":
            typeRes = res._properties["name"]

        if typeRes != "Same":
            tripleName.append(node0._properties["name"])
            try:
                tripleName.append(relResponsed[typeRes])
            except KeyError as e:
                tripleName.append("其他")
            tripleName.append(node1._properties["name"])
            if tripleName not in pathTriple:
                pathTriple.append(tripleName)
        if node0 is not None:
            if node0.id not in nodeid_list:
                node_set = {}
                nodeid_list.append(node0.id)
                node_set["id"] = str(node0.id)
                node_set["value"] = 1
                node_set["name"] = node0._properties["name"]
                try:

                    node_set["url"] = node0._properties["url"]
                    node_set["timestamp"] = node0._properties["timestamp"]
                except KeyError:
                    print()

                node_set["symbolSize"] = 40

                tmp = list(set(node0.labels))
                print(f"查询到的关系中的节点类型:{tmp}", flush=True)

                if 'Wikipedia' in list(set(node0.labels)):
                    node_set['category'] = 0
                elif 'CaLe0' in list(set(node0.labels)):
                    node_set['category'] = 3
                elif 'CaLe1' in list(set(node0.labels)):
                    node_set['category'] = 5
                elif 'CaLe2' in list(set(node0.labels)):
                    node_set['category'] = 1
                elif 'ownthink' in list(set(node0.labels)):
                    node_set['category'] = 4
                elif 'Super' in list(set(node0.labels)):
                    node_set['category'] = 10
                    node_set['url'] = "www.zhonghuapu.com"
                elif 'People' in list(set(node0.labels)):
                    node_set['category'] = 7
                    node_set['url'] = "www.zhonghuapu.com"
                    # node_set['GeneNumber'] = 7
                    # node_set['timestamp'] = 7
                    # node_set['description'] = 7
                elif 'selfCreate' in list(set(node0.labels)):
                    node_set['category'] = 8
                elif 'ScholarCSKG' in list(set(node0.labels)):
                    node_set['category'] = 9
                elif 'hyperNode' in list(set(node0.labels)):
                    node_set['category'] = 10
                elif 'HAO' in list(set(node0.labels)):
                    node_set['category'] = 11
                elif 'BaiduBaike' in list(set(node0.labels)):
                    node_set['category'] = 12
                elif 'gpt' in list(set(node0.labels)):
                    node_set['category'] = 13
                elif 'Thingo' in list(set(node0.labels)):
                    node_set['category'] = 14
                else:
                    node_set['category'] = 2
                # node_set['properties']=node0._properties
                node_set['properties'] = change_keys_to_chinese(node0._properties, translation_dict)
                if len(node_set) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空

                    nodes.append(node_set)

        if node1 is not None:
            if node1.id not in nodeid_list:
                node_set1 = {}
                nodeid_list.append(node1.id)
                node_set1["id"] = str(node1.id)
                node_set1["value"] = 1
                node_set1["name"] = node1._properties["name"]
                try:

                    node_set1["url"] = node1._properties["url"]
                    node_set1["timestamp"] = node1._properties["timestamp"]
                except KeyError:
                    print()

                node_set1["symbolSize"] = 40

                if 'Wikipedia' in list(set(node1.labels)):
                    node_set1['category'] = 0
                elif 'CaLe0' in list(set(node1.labels)):
                    node_set1['category'] = 3
                elif 'CaLe1' in list(set(node1.labels)):
                    node_set1['category'] = 5
                elif 'CaLe2' in list(set(node1.labels)):
                    node_set1['category'] = 1
                elif 'ownthink' in list(set(node1.labels)):
                    node_set1['category'] = 4
                elif 'Super' in list(set(node0.labels)):
                    node_set1['category'] = 10
                    node_set['url'] = "www.zhonghuapu.com"
                elif 'People' in list(set(node0.labels)):
                    node_set1['category'] = 7
                    node_set1['url'] = "www.zhonghuapu.com"
                    # node_set1['GeneNumber'] = 7
                    # node_set1['timestamp'] = 7
                    # node_set1['description'] = 7
                elif 'selfCreate' in list(set(node1.labels)):
                    node_set1['category'] = 8
                elif 'ScholarCSKG' in list(set(node1.labels)):
                    node_set1['category'] = 9
                elif 'hyperNode' in list(set(node1.labels)):
                    node_set1['category'] = 10
                elif 'HAO' in list(set(node1.labels)):
                    node_set1['category'] = 11
                elif 'BaiduBaike' in list(set(node1.labels)):
                    node_set1['category'] = 12
                elif 'gpt' in list(set(node1.labels)):
                    node_set1['category'] = 13
                elif 'Thingo' in list(set(node1.labels)):
                    node_set1['category'] = 14
                else:
                    node_set1['category'] = 2
                # node_set1['properties']=node1._properties
                node_set1['properties'] = change_keys_to_chinese(node1._properties, translation_dict)
                if len(node_set1) != 0:  # 可能会出现头尾节点都在nodeid_list里面导致nodeset为空

                    nodes.append(node_set1)

        link_set = {}

        if res is not None:
            if res.id not in linkid_list:

                startNodeId = str(node0.id)
                endNodeId = str(node1.id)
                name0 = str(node0._properties["name"])
                name1 = str(node1._properties["name"])

                for node in nodes:
                    if startNodeId == node["id"]:
                        link_set["target"] = startNodeId
                    if endNodeId == node["id"]:
                        link_set["source"] = endNodeId

                # 找到关系的两端节点的来源
                relDes = ""

                startNodelabels = list(set(node0.labels))
                startResource = ""
                for i in startNodelabels:
                    if i in corresponsed.keys():
                        startResource = corresponsed[i]
                endNodelabels = list(set(node1.labels))
                endResource = ""
                for i in endNodelabels:
                    if i in corresponsed.keys():
                        endResource = corresponsed[i]

                if "des" in res._properties:
                    relDes = res._properties["des"]

                    link_set["label"] = {"show": True, "formatter": typeRes, "fontSize": 12, "desc": {"关系": relDes},
                                         "offset": [0, 10]}
                    link_set["lineStyle"] = {"curveness": 0}
                else:

                    if endResource == startResource:
                        try:
                            relDes = "关系为同一来源：" + startResource + "的内部关联，详情请前往：" + node0._properties[
                                "url"] + "和" + \
                                     node1._properties["url"]
                        except KeyError as e:
                            relDes = "关系为同一来源：" + startResource + "的内部关联"
                    else:
                        try:
                            relDes = "关系为不同来源：" + startResource + "--->" + endResource + "之间的关联，详情请前往：" + \
                                     node0._properties["url"] + "和" + node1._properties["url"]
                        except KeyError as e:
                            relDes = "关系为不同来源：" + startResource + "--->" + endResource + "之间的关联"

                    link_set["label"] = {"show": True, "formatter": typeRes, "fontSize": 12, "desc": {"关系": relDes},
                                         "offset": [0, 10]}
                    link_set["lineStyle"] = {"curveness": 0}
                    link_set["name"] = startNodeId + name0 + "-->" + endNodeId + name1
                    # # 节点来源
                    # link_set["url"] = startNodeId + name0 + "-->" + endNodeId + name1
                    # # 家谱人数
                    # link_set["GeneNumber"] = startNodeId + name0 + "-->" + endNodeId + name1
                    # # 时间戳
                    # link_set["timestamp"] = startNodeId + name0 + "-->" + endNodeId + name1
                    # # 家谱描述
                    # link_set["description"] = startNodeId + name0 + "-->" + endNodeId + name1
                if len(link_set) != 0:
                    links.append(link_set)

                # start_properties=node0._properties
                # end_properties=node1._properties
                # start_properties["name"]=zhconv.convert(start_properties["name"],'zh-cn')
                # start_properties["url"]=zhconv.convert(start_properties["url"],'zh-cn')
                # end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')
                # end_properties["name"]=zhconv.convert(end_properties["name"],'zh-cn')

    sum_list['nodes'] = nodes

    sum_list['links'] = links
    sum_list['flag'] = flag
    sum_list['pathNodes'] = pathNodes
    sum_list['pathIds'] = pathIds
    sum_list['pathTriple'] = pathTriple
    return sum_list


# 返回id 若不存在节点 则返回-1，存在节点：只有一个返回节点id 有多个返回朝超点id
# def getID(name):
#     # print("======")
#     # print(name)
#     driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
#     session3 = driver.session()
#     query_word1 = '''
#                 match(n:hyperNode) where n.name={arg_1}
#                 return count(n) as num ,id(n) as id limit 1
#             '''
#     results_node1 = session3.run(query_word1, parameters={"arg_1": name})
#
#     query_word = '''
#                     match(n:item) where n.name={arg_1}
#                     return count(n)
#                 '''
#     results_node = session3.run(query_word, parameters={"arg_1": name})
#     node_count = results_node.single().value()
#     # print(node_count)
#     # 库中没有这个点
#     if node_count == 0:
#         return -1
#     # 库中有这个点
#     else:
#         # 库中没有超点，建立超点
#         if results_node1.peek() is None:
#             if node_count > 1:
#                 current_date = datetime.now().date()
#                 # 将日期格式化为整数
#                 formatted_date = int(current_date.strftime('%Y%m%d'))
#                 query_word3 = "CREATE (n:hyperNode:item) set n.url=\"" + "https://www.ko.zhonghuapu.com" + "\", n.name=\"" + name + "\",n.timestamp=" + str(
#                     formatted_date) + "  with n MATCH (a:item ), (b:hyperNode) where a.name=n.name and id(a)<>id(n) and  id(b)=id(n)  MERGE (a)-[r:Same]->(b) return id(b) as  id"
#                 results_node3 = session3.run(query_word3, parameters={"arg_1": name})
#                 id2 = 0
#                 for i in results_node3:
#                     id2 = i["id"]
#                 return id2
#             else:  # "只有一个点 返回这个点id"
#                 query_word2 = '''
#                     match(n:item) where n.name={arg_1} return id(n) as id limit 1
#                     '''
#                 results_node2 = session3.run(query_word2, parameters={"arg_1": name})
#                 id1 = 0
#                 for i in results_node2:
#                     id1 = i["id"]
#                 return id1
#         # 库中有超点，返回超点id
#         else:
#             id = 0
#             for i in results_node1:
#                 id = i["id"]
#             return id
#     return -1
def removeTwoway(links):
    data = links
    seen = set()

    # 使用列表推导来过滤重复的字典
    unique_data = []
    for d in data:
        # 确保source和target是有序的，以便比较
        source_target_pair = tuple(sorted((d['source'], d['target'])))
        if source_target_pair not in seen:
            seen.add(source_target_pair)
            unique_data.append(d)
    # 打印结果
    return unique_data


def getID(name, session):
    if name == '吴信东':
        return 5690
    if name == '郑磊':
        return 2335

    query_word1 = '''
                match(n:hyperNode) where n.name={arg_1}
                WITH n, size(keys(n)) AS numProperties
                ORDER BY numProperties DESC
                return count(n) as num ,id(n) as id limit 1
            '''
    query_word1 = '''
                    match(n:hyperNode) where n.name={arg_1}
                    WITH n, size(keys(n)) AS numProperties
                    ORDER BY numProperties DESC
                    return n
                '''
    results_node1 = session.run(query_word1, parameters={"arg_1": name})
    for res in results_node1:
        print(f"results_node1内容: {res}", flush=True)

    query_word = '''
                    match(n:item) where n.name={arg_1}
                    return n
                '''
    results_node = session.run(query_word, parameters={"arg_1": name})
    for res in results_node:
        print(f"results_node内容: {res}", flush=True)

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
    hyperDesc = hyperDesc[:-1].replace('"', '')

    res_len = len(list(results_nodeTmp))
    for res in list(results_nodeTmp):
        print(f"list(results_nodeTmp)内容: {res}", flush=True)
    # 如果需要遍历res进行处理后再打印
    # 如果需要遍历res进行处理后再打印


    # 库中没有这个点
    if res_len == 0:
        return -1
    # 库中有这个点
    else:
        # 库中没有超点，建立超点
        if results_node1.peek() is None:
            # if res_len > 1:
            #     current_date = datetime.now().date()
            #     # 将日期格式化为整数
            #     formatted_date = int(current_date.strftime('%Y%m%d'))
            #     query_word3 = "CREATE (n:hyperNode:item) set n.url=\"" + hyperUrl + "\", n.des=\"" + hyperDesc + "\",n.source=\"" + hyperSource + "\",n.name=\"" + name + "\",n.timestamp=" + str(
            #         formatted_date) + "  with n MATCH (a:item ), (b:hyperNode) where a.name=n.name and id(a)<>id(n) and  id(b)=id(n)  MERGE (a)-[r:Same]->(b) return id(b) as  id"
            #     print(f"创建超点语句:{query_word3}", flush=True)
            #     results_node3 = session.run(query_word3, parameters={"arg_1": name})
            #     id2 = 0
            #     for i in results_node3:
            #         id2 = i["id"]
            #     return id2
            # else:  # "只有一个点 返回这个点id"
            #     query_word2 = '''
            #         match(n:item) where n.name={arg_1} return id(n) as id limit 1
            #         '''
            #     results_node2 = session.run(query_word2, parameters={"arg_1": name})
            #     id1 = 0
            #     for i in results_node2:
            #         id1 = i["id"]
            #     return id1
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


def getIDHP(name, session):
    # 从字典中获取 ID
    id_map = {
        '吴信东': 1086445,
        '吴超': 1086410,
        '邹燕': 16596990,
        '吴之胜': 3534216,
        '吴盛双': 3533376,
        '邹晓东': 16596989
    }

    if name in id_map:
        return id_map[name]

    # 查询数据库
    query_word = '''
        MATCH (n:People) WHERE n.name = {arg_1}
        AND NOT 'Super' IN labels(n)
        RETURN id(n) AS id
    '''
    results_node = session.run(query_word, parameters={"arg_1": name})

    for res in results_node:
        return res['id']  # 返回第一个找到的 ID

    return -1


def change_keys_to_chinese(dictionary, translation):
    new_dict = {}
    for key, value in dictionary.items():
        if key in translation:
            new_key = translation[key]
        else:
            new_key = key
        new_dict[new_key] = value
    return new_dict


# 根据id获取节点的属性华谱
def getNodeHP(session, id):
    ans = {}
    query_word1 = f'''
                match(h:People) where id(h)={id}
                return h
            '''
    results_node1 = session.run(query_word1)
    for res in results_node1:
        ans["id"] = res['h'].id
        ans["value"] = 1
        ans["name"] = res['h']._properties["name"]
        ans["url"] = "www.zhonghuapu.com/" + res['h']._properties["name"]
        try:
            ans["timestamp"] = res['h']._properties["timestamp"]
        except KeyError as e:
            ans["timestamp"] = 20240417
        ans["symbolSize"] = 40

        if 'Wikipedia' in list(set(res['h'].labels)):
            ans["category"] = 0
        elif 'CaLe0' in list(set(res['h'].labels)):
            ans['category'] = 3
        elif 'CaLe1' in list(set(res['h'].labels)):
            ans['category'] = 5
        elif 'CaLe2' in list(set(res['h'].labels)):
            ans['category'] = 1
        elif 'ownthink' in list(set(res['h'].labels)):
            ans['category'] = 4
        elif 'selfCreate' in list(set(res['h'].labels)):
            ans['category'] = 8
        elif 'ScholarCSKG' in list(set(res['h'].labels)):
            ans['category'] = 9
        elif 'hyperNode' in list(set(res['h'].labels)):
            ans['category'] = 10
        elif 'HAO' in list(set(res['h'].labels)):
            ans['category'] = 11
        elif 'BaiduBaike' in list(set(res['h'].labels)):
            ans['category'] = 12
        elif 'gpt' in list(set(res['h'].labels)):
            ans['category'] = 13
        elif 'Thingo' in list(set(res['h'].labels)):
            ans['category'] = 14
        else:
            ans['category'] = 2
            # node_set['properties']=node0._properties
        ans['properties'] = change_keys_to_chinese(res['h']._properties, translation_dict)
    return ans


# 根据id获取节点的属性
def getNode(session, id):
    ans = {}
    query_word1 = f'''
                match(h:item) where id(h)={id}
                return h
            '''
    results_node1 = session.run(query_word1)
    for res in results_node1:
        ans["id"] = res['h'].id
        ans["value"] = 1
        ans["name"] = res['h']._properties["name"]
        ans["url"] = res['h']._properties.get("url","")
        try:
            ans["timestamp"] = res['h']._properties["timestamp"]
        except KeyError as e:
            ans["timestamp"] = 20240417
        ans["symbolSize"] = 40

        if 'Wikipedia' in list(set(res['h'].labels)):
            ans["category"] = 0
        elif 'CaLe0' in list(set(res['h'].labels)):
            ans['category'] = 3
        elif 'CaLe1' in list(set(res['h'].labels)):
            ans['category'] = 5
        elif 'CaLe2' in list(set(res['h'].labels)):
            ans['category'] = 1
        elif 'ownthink' in list(set(res['h'].labels)):
            ans['category'] = 4
        elif 'selfCreate' in list(set(res['h'].labels)):
            ans['category'] = 8
        elif 'ScholarCSKG' in list(set(res['h'].labels)):
            ans['category'] = 9
        elif 'hyperNode' in list(set(res['h'].labels)):
            ans['category'] = 10
        elif 'HAO' in list(set(res['h'].labels)):
            ans['category'] = 11
        elif 'BaiduBaike' in list(set(res['h'].labels)):
            ans['category'] = 12
        elif 'gpt' in list(set(res['h'].labels)):
            ans['category'] = 13
        elif 'Thingo' in list(set(res['h'].labels)):
            ans['category'] = 14
        else:
            ans['category'] = 2
            # node_set['properties']=node0._properties
        ans['properties'] = change_keys_to_chinese(res['h']._properties, translation_dict)
    return ans


def determine_database(StartNode, EndNode, id1, id2):
    huapu_set = {'吴根长', '吴超', '吴之胜', '邹燕', '吴盛双', '邹晓东', '吴道二', '夫差', '泰伯', '吴德强', '吴选斌', '张宇英'}
    ko_set = {'倪岳峰', '杨善林', '郑磊', '周志华', '陆汝钤', '付磊', '汪萌', '梁樑', '合肥工业大学', '知识图谱', '智能互联系统安徽省实验室',
              '数据挖掘', '人工智能', '大数据知识工程', '奇瑞'}

    if StartNode in huapu_set or EndNode in huapu_set:
        return 'huapu'
    if StartNode in ko_set or EndNode in ko_set:
        return 'ko'
    id1 = parse_id(id1)
    id2 = parse_id(id2)
    if id1 is not None and id2 is not None:
        in_ko_1 = check_person_in_database(StartNode, id1, 'ko')
        in_ko_2 = check_person_in_database(EndNode, id2, 'ko')
        if in_ko_1 and in_ko_2:
            return 'ko'

        in_huapu_1 = check_person_in_database(StartNode, id1, 'huapu')
        in_huapu_2 = check_person_in_database(EndNode, id2, 'huapu')
        if in_huapu_1 and in_huapu_2:
            return 'huapu'

    if id1 is not None:
        in_ko_1 = check_person_in_database(StartNode, id1, 'ko')
        in_huapu_1 = check_person_in_database(StartNode, id1, 'huapu')
        if in_ko_1:
            return 'ko'
        elif in_huapu_1:
            return 'huapu'

    if id2 is not None:
        in_ko_2 = check_person_in_database(EndNode, id2, 'ko')
        in_huapu_2 = check_person_in_database(EndNode, id2, 'huapu')
        if in_ko_2:
            return 'ko'
        elif in_huapu_2:
            return 'huapu'
    print(1111111,flush=True)
    in_ko_1 = check_person_in_database(StartNode, None, 'ko')
    in_ko_2 = check_person_in_database(EndNode, None, 'ko')
    if in_ko_1 and in_ko_2:
        return 'ko'
    print("in_ko_1:",in_ko_1,flush=True)
    in_huapu_1 = check_person_in_database(StartNode, None, 'huapu')
    in_huapu_2 = check_person_in_database(EndNode, None, 'huapu')
    if in_huapu_1 and in_huapu_2:
        return 'huapu'
    print("in_huapu_1:",in_huapu_1,flush=True)
    return 'ko'

def check_person_in_database(name, id, database):
    if database == 'huapu':
        driver = GraphDatabase.driver("bolt://114.213.234.179:7687", auth=("neo4j", "hfut701DMiChp"))
    else:
        # driver1 = "bolt://114.213.232.140:47687"
        # driver2 = "bolt://114.213.232.140:47688"
        # driver3 = "bolt://114.213.232.140:47689"
        # dirver_choice = random.choice([driver1, driver2, driver3])
        # dirver_choice = driver2
        # driver = GraphDatabase.driver(dirver_choice, auth=("neo4j", "123456"))
        driver4 = "bolt://114.213.232.140:17687"
        dirver_choice = driver4
        driver = GraphDatabase.driver(driver4, auth=("neo4j", "DMiChao"))
    print("id:",id)
    session = driver.session()
    try:
        if id is not None:
            query = "MATCH (n) WHERE id(n) = $id AND n.name = $name RETURN n LIMIT 1"
            print("query1:",query)
            print("name:",name)
            result = session.run(query, id=id, name=name).single()
        else:
            query = "MATCH (n) WHERE n.name = $name RETURN n LIMIT 1"
            print("query2:",query)
            print("name:",name)
            result = session.run(query, name=name).single()
        session.close()
        driver.close()
        return result is not None
    except Exception as e:
        print(f"Error occurred: {e}", flush=True)
        session.close()
        driver.close()
        return False


def parse_id(id_str):
    try:
        id_int = int(id_str)
        if id_int >= 0:
            return id_int
        else:
            return None
    except (ValueError, TypeError):
        return None


def get_filtered_path(hp_session, initial_searched_paths, user_id, start_node_id, end_node_id):
    """
       过滤初次查询的结果，并根据过滤后的子图是否连通，决定返回结果
        1 最先判断是否为管理员，若为管理员，则直接判断可以访问
        2 共建家谱（shared id）ID与公开家谱ID（public status==10）的并集
        3 拿出家谱的myID在上述集合中进行查询，判断是否存在

       :param hp_session: 华谱数据库session
       :param initial_searched_paths: 初次查询后的返回结果
       :param user_id: 目前登录用户的ID
       :param start_node_id: 开始节点ID
       :param end_node_id: 结束节点ID
       :return: 按照权限过滤部分用户无权限访问节点&根据连通性返回结果
    """
    # 若用户为管理员，则可以访问所有家谱=>直接返回原数据
    initial_searched_paths_data = list(initial_searched_paths)
    # initial_searched_paths_data = (path for path in initial_searched_paths)
    is_admin = check_user_admin(user_id)
    if is_admin:
        return initial_searched_paths_data

    # print(f"get_filetered_path: {initial_searched_paths.data()}", flush=True)
    # 过滤不可访问的节点后生成的子图（该子图中的每个节点应为用户可访问的）
    accessible_results = filter_accessible_paths(initial_searched_paths_data, user_id)
    # 若图非连通，直接返回空列表；若图连通，则返回原数据
    if check_connectivity(accessible_results, start_node_id, end_node_id) is False:
        return []
    # if ensure_connectivity(accessible_results, start_node_id, end_node_id) is None:
    #     return []
    # print(f"accessible_results: {accessible_results}", flush=True)
    return accessible_results


def filter_accessible_paths(initial_searched_paths_data, user_id):
    """
        过滤掉用户没有权限查看的那些节点
        :param initial_searched_paths_data:
        :param user_id: 登录用户的ID
        :return: 按照权限过滤部分用户无权限访问节点后的子图
    """
    accessible_paths = []
    # 用户参与的共建家谱的ID集合，以及本身为公开访问家谱的ID集合
    shared_ids_set = set(get_shared_family_ids(user_id))
    public_family_ids_set = get_public_family_ids()

    accessible_family_ids_set = shared_ids_set.union(public_family_ids_set)

    for record in initial_searched_paths_data:
        node = record['node']
        # print(f"node: {node}", flush=True)
        family_id_of_node = node['myID']
        # if (family_id_of_node is None) or (family_id_of_node in accessible_family_ids_set):
        #     accessible_paths.append(record)
        #     print(f"accessible_paths[0]['node']['myID']: {accessible_paths[0]['node']['myID']}", flush=True)
        if family_id_of_node in accessible_family_ids_set:
            accessible_paths.append(record)
            # print(f"accessible_paths[0]['node']['myID']: {accessible_paths[0]['node']['myID']}", flush=True)

    return accessible_paths


# def ensure_connectivity(neo4j_result, start_node_id, end_node_id):
#     """
#         检查经权限过滤后的子图是否仍然连通
#         :param neo4j_result: 经在neo4j中查询后的返回结果
#         :param start_node_id: 起始节点ID
#         :param end_node_id: 结束节点ID
#         :return: 若仍连通，返回neo4j_result；否则，返回None
#     """
#     # 将neo4j_result转换为图的格式graph
#     # 采用并查集、BFS或DFS判断graph是否连通
#     # 若连通，则返回neo4j_result；否则返回None
#     return neo4j_result

def check_connectivity(neo4j_result, start_node_id, end_node_id):
    """
        检查经权限过滤后的子图是否仍然连通
        :param neo4j_result: 经在neo4j中查询后的返回结果
        :param start_node_id: 起始节点ID
        :param end_node_id: 结束节点ID
        :return: 若仍连通，返回True；否则，返回False
    """
    if neo4j_result is None or neo4j_result == []:
        return False
    # 抽取neo4j_result中的p并组成Paths(list)
    paths = []
    for record in neo4j_result:
        paths.append(record['p'])
    # 给路径去重
    paths = list(set(paths))

    # 建立邻接表
    graph = defaultdict(list)
    for path in paths:
        nodes = path.nodes
        for i in range(len(nodes)-1):
            if hasattr(nodes[i], 'element_id'):
                node_id_1 = nodes[i].element_id
                graph[node_id_1].append(node_id_1)
            if hasattr(nodes[i+1], 'element_id'):
                node_id_2 = nodes[i+1].element_id
                # 双向添加连接关系
                graph[node_id_2].append(node_id_2)
    # 获取所有出现的节点ID集合
    all_nodes = set(graph.keys())
    # 如果起始节点或者结束节点有一个不在里面，则没必要判断连通性了
    if start_node_id not in all_nodes:
        return False
    if end_node_id not in all_nodes:
        return False

    # 从一个起始节点开始遍历
    start_node = next(iter(all_nodes))
    visited = set()
    queue = deque([start_node])

    # BFS
    while queue:
        node = queue.popleft()
        if node not in visited:
            visited.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    queue.append(neighbor)

    # 如果访问到的节点集合包含所有节点，则图是连通的
    return visited == all_nodes


def check_node_access(user_id, public_status, family_id, shared_family_ids, family_id_name_dict) -> bool:
    """
        对每个节点检查用户是否有对该节点的访问权限
        :param user_id: 登陆用户的ID，用以判断用户的身份
        :param public_status: 数据的公开权限等级，可用以判断数据是否对用户访问
        :param family_id: 家谱ID
        :param shared_family_ids: 共享家谱的ID
        :param family_id_name_dict: 库中已有家谱字典key=family_id（家谱ID）, value=family_name（家谱名称）
        :return: 对于可访问节点，返回True；不可访问节点，返回False
    """
    # 检查用户是否为管理员
    is_admin = check_user_admin(user_id)
    # 检查是否为公开数据
    is_public = public_status == 10
    # 检查家谱是否为共享家谱
    is_shared_family = family_id in shared_family_ids
    # 检查该节点对应的家谱是否存在
    is_family_existed = False if family_id_name_dict.get(family_id) is None else True
    print(f"is_admin:{is_admin}, is_public:{is_public}, is_shared_family:{is_shared_family}, is_family_existed:{is_family_existed}", flush=True)
    return is_family_existed and (is_admin or is_public or is_shared_family)


def execute_mysql_query(query, params=None):
    # 该行代码用以非管理员访问测试
    db = pymysql.connect(host='www.zhonghuapu.com', user='koroot', password='DMiC-4092', database='db_hp',
                         charset='utf8')
    now = datetime.now()
    print(f'Time：{now}\nQuery：{query}\nParams：{params}\n', flush=True)
    cursor = db.cursor()
    cursor.execute(query, params)
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return result


def check_user_admin(userid):
    query = "SELECT isadmin FROM tb_user WHERE id = %s"
    result = execute_mysql_query(query, (userid,))
    return result[0][0] == '1' if result else False


def get_family_public_status(myids):
    print(f'get_family_public_status: ' + str(myids), flush=True)
    query = "SELECT public, creater FROM tb_mydata WHERE id IN %s"
    results = execute_mysql_query(query, (tuple(myids),))
    return {str(result[1]): result[0] for result in results}


def get_shared_family_ids(userid):
    # shared_ids = set()
    # group_query = """
    # SELECT mg.mydataid
    # FROM tb_user_group_relation ug
    # JOIN tb_mydata_group_relation mg ON ug.groupid = mg.groupid
    # WHERE ug.userid = %s
    # """
    # shared_ids.update([mg[0] for mg in execute_mysql_query(group_query, (userid,))])
    #
    # share_query = "SELECT myid FROM tb_mydata_share WHERE userid = %s"
    # shared_ids.update([sg[0] for sg in execute_mysql_query(share_query, (userid,))])
    #
    # return list(shared_ids)
    query = """
        SELECT mg.mydataid FROM tb_user_group_relation ug 
        JOIN tb_mydata_group_relation mg ON ug.groupid = mg.groupid 
        WHERE ug.userid = %s
        UNION
        SELECT myid FROM tb_mydata_share WHERE userid = %s
        """
    results = execute_mysql_query(query, (userid, userid))

    # 直接返回去重后的结果
    return list(str(row[0]) for row in results)  # 使用集合去重


def get_public_family_ids():
    query = "SELECT id FROM tb_mydata WHERE public = 10"
    results = execute_mysql_query(query)
    return [str(row[0]) for row in results]


def get_family_name_from_mysql(myid):
    query = "SELECT name FROM tb_mydata WHERE id = %s"
    result = execute_mysql_query(query, (myid,))
    return result[0][0] if result else None
