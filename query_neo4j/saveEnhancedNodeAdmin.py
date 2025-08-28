# coding:utf-8
from datetime import datetime
from neo4j import GraphDatabase
import json
import pymysql
import copy


def main(request):
    print('***管理员存储接口调用开始***')
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    nodes = request.POST['nodes']

    links = request.POST['links']
    userId = request.POST['userId']
    startId = request.POST['startId']
    endId = request.POST['endId']

    userId = str(json.loads(userId))
    links = json.loads(links)
    nodes = json.loads(nodes)
    startId = str(json.loads(startId))
    endId = str(json.loads(endId))




    current_date = datetime.now().date()
    # 将日期格式化为整数
    formatted_date = str(current_date.strftime('%Y%m%d'))
    backLinks = []
    backNoes = {}
    # 找到候选关系

    for link in links:
        if int(link['target']) < 0 or int(link['source']) < 0:
            backLinks.append(link)
    # 处理node数组方便找name
    for node in nodes:
        backNoes[node['id']] = node['name']
    if len(backLinks)==0:
        # 本次保存的结果为空，认为用户要清空
        queryWord1 = '''
        MATCH (n)-[r]-(m:gpt)
        WHERE id(n) = ''' + startId + ''' OR id(n) = ''' + endId + '''
        SET m.OA=0,
            m.toUser = CASE 
                          WHEN EXISTS(m.toUser) THEN [value IN m.toUser WHERE value <> \"''' + userId + '''\"]
                          ELSE []
                       END
        '''
        session.run(queryWord1)
    # print(f'backLinks:{backLinks}')
    # print("============")
    # print(f'backNoes:{backNoes}')
    for link in backLinks:
        link['targetName'] = backNoes[link['target']]
        link['sourceName'] = backNoes[link['source']]
        des = link['label']['formatter']
        # 找到超点的id
        if int(link['target']) > 0:
            target = link['target']
            name = link['sourceName']
        else:
            target = link['source']
            name = link['targetName']
        url = "https://ko.zhonghuapu.com"

        # 这段语句使用MERGE关键字，尝试匹配或创建一个新节点，该节点的标签为item:gpt，属性包括name、timestamp和url。如果节点已存在，则不会创建新节点，如果不存在，则会创建新节点。
        queryWord = '''
            
            MATCH (target)
            WHERE id(target) = ''' + target + '''
            
            
            MERGE (source:item:gpt {name: \"''' + name + '''\",url:\"''' + url + '''\"})
            SET source.OA = 1,source.timestamp=''' + str(formatted_date) + '''
            
            MERGE (target)-[r:Contain]->(source)
            SET r.des = \"''' + des + '''\"
        '''
        session.run(queryWord)
    print('***管理员存储接口调用结束***')
    return json.dumps(["OK"], ensure_ascii=False)
