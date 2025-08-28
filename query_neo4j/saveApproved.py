# coding:utf-8
from datetime import datetime
from neo4j import GraphDatabase
import json
# import pymysql
import copy
from datetime import datetime
import ast
def manageAddInfo(startNodeId,Source,content,reason,session):
    print(f'startNodeId的数据类型为{type(startNodeId)}')
    addnode = content.split('-')[2]  # 吴信东-地域-合肥
    edge = content.split('-')[1]
    url = "https://ko.zhonghuapu.com"
    # 将日期格式化为整数
    current_date = datetime.now().date()
    formatted_date = str(current_date.strftime('%Y%m%d'))
    queryWord2 = '''
                        MATCH (target)
                        WHERE id(target) = ''' + startNodeId + '''
                        MERGE (source:item:gpt {name: \"''' + addnode + '''\",url:\"''' + url + '''\"})
                        SET source.OA = 1,source.timestamp=''' + str(formatted_date) + '''
                        MERGE (target)-[r:Contain]->(source)
                        SET r.des = \"''' + edge + '''\",r.source=\"''' + Source + '''\",r.reason=\"''' + reason + '''\"
                    '''
    session.run(queryWord2)

def manageDeleteInfo(startNodeId,content,session,userId):
    addnode = content.split('-')[2]
    queryWord1 = '''
            MATCH (n)-[r]-(m:gpt)
            WHERE id(n) = ''' + startNodeId + ''' and m.name = \"''' + addnode + '''\"
            SET m.OA=0,
                m.toUser = CASE
                              WHEN EXISTS(m.toUser) THEN [value IN m.toUser WHERE value <> \"''' + str(userId) + '''\"]
                              ELSE []
                           END
            '''
    session.run(queryWord1)


def manageReviseInfo(startNodeId,Source,content,reason,session):
    addnode = content.split('-')[2]
    edge = content.split('-')[1]

    queryWord1 = '''
        MATCH (n)-[r]-(m:gpt)
        WHERE id(n) = ''' + startNodeId + ''' and m.name =\"''' + addnode+ '''\"
        SET r.des = \"''' + edge + '''\",r.source=\"''' + Source + '''\",r.reason=\"''' + reason + '''\"
        '''
    session.run(queryWord1)



def main(request):
    print('---调用管理员存储知识库接口---')
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    content = request.POST['content'].strip('"')
    operaType = request.POST['operaType'].strip('"') # 删除 增加 修改
    reason = request.POST['reason'].strip('"').strip('\\').strip('"') # 修改理由
    Source = request.POST['source'].strip('"').strip('\\').strip('"') # 尾实体添加边的信息来源
    startNodeId = request.POST['startNodeId'].strip('"') # 头实体的ID
    userId = request.POST['userId'].strip('"')

    print(f'管理审核接口的输入为：{Source}---{content}-----{operaType}---{reason}----{startNodeId}---{userId}')
    if operaType == '添加':
        print('执行管理员审核 添加功能')
        manageAddInfo(startNodeId, Source, content,reason, session)
    if operaType == '删除':
        print('执行管理员审核 删除功能')
        manageDeleteInfo(startNodeId,content, session,userId)
    if operaType == '修改':
        print('执行管理员审核 修改功能')
        manageReviseInfo(startNodeId, Source, content, reason, session)
    print('---结束管理员存储知识库接口---')
    return json.dumps(["OK"], ensure_ascii=False)

