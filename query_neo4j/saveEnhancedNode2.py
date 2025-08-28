# coding:utf-8
from datetime import datetime
from neo4j import GraphDatabase
import json
import pymysql
import copy

from datetime import datetime
def main(request):

    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    userId = request.POST['userId']
    addInfo=request.POST['addInfo']
    deleteInfo=request.POST['deleteInfo']
    reviseInfo=request.POST['reviseInfo']
    userId = str(json.loads(userId))


    addInfo=json.loads(addInfo)
    deleteInfo=json.loads(deleteInfo)
    reviseInfo=json.loads(reviseInfo)
    manageAddInfo(addInfo,session,userId)
    manageDeleteInfo(deleteInfo,session,userId)
    manageReviseInfo(reviseInfo,session)
    session.close()
    return json.dumps(["OK"], ensure_ascii=False)
#处理新增的
def manageAddInfo(addInfo,session,userId):
    print("addinfo")
    print(addInfo)
    url="https://ko.zhonghuapu.com"
    # 将日期格式化为整数
    current_date = datetime.now().date()
    formatted_date = str(current_date.strftime('%Y%m%d'))
    addlinks=[]
    for i in addInfo['addContent']:
        if len(i)!=0:
            addlinks.append([i[1],i[2],i[4],i[5],i[6]])#1超点id 2新建节点name 4关系name 5关系source 6关系reason
            # if len(i[0])!=0 and len(i[2])!=0:#和头尾节点都有关联i
            #     addlinks.append([addInfo['startNodeInfo'][1],i[1],i[0],i[3],i[5]])
            #     addlinks.append([addInfo['endNodeInfo'][1],i[1],i[2],i[4],i[5]])
            # else:
            #     if len(i[0])==0:#只和尾节点关联
            #         addlinks.append([addInfo['endNodeInfo'][1],i[1],i[2],i[4],i[5]])
            #     if len(i[2])==0:#只和头节点关联
            #         addlinks.append([addInfo['startNodeInfo'][1],i[1],i[0],i[3],i[5]])
    for i in addlinks:
        queryWord = '''
            MATCH (target)
            WHERE id(target) = ''' + i[0] + '''
            
            MERGE (source:item:gpt {name: \"''' + i[1] + '''\",url:\"''' + url + '''\"}) 
            ON CREATE SET source.toUser = [toString(''' + userId + ''')]
            ON MATCH SET source.toUser= CASE
                WHEN toString(''' + userId + ''') IN source.toUser THEN source.toUser
                ELSE COALESCE(source.toUser,[])+[toString(''' + userId + ''')]
            END
            set source.timestamp=''' + str(formatted_date) + '''
            
            MERGE (target)-[r:Contain]->(source)
            SET r.des = \"''' + i[2] + '''\",r.source=\"''' + i[3] + '''\",r.reason=\"''' + i[4] + '''\"        
                
            '''
        session.run(queryWord)

# 处理删除的
def manageDeleteInfo(deleteInfo,session,userId):

    deletelinks=[]

    for i in deleteInfo['deleteContent']:
        if len(i)!=0:
            deletelinks.append([i[1],i[2]])
            # if len(i[0])!=0 and len(i[2])!=0:#和头尾节点都有关联
            #     deletelinks.append([deleteInfo['startNodeInfo'][1],i[1],i[0]])
            #     deletelinks.append([deleteInfo['endNodeInfo'][1],i[1],i[2]])
            # else:
            #     if len(i[0])==0:#只和尾节点关联
            #         deletelinks.append([deleteInfo['endNodeInfo'][1],i[1],i[2]])
            #     if len(i[2])==0:#只和头节点关联
            #         deletelinks.append([deleteInfo['startNodeInfo'][1],i[1],i[0]])
    for i in deletelinks:
        queryWord1 = '''
            MATCH (n)-[r]-(m:gpt)
            WHERE id(n) = ''' + i[0] + ''' and m.name = \"''' + i[1] + '''\"
            SET m.toUser = CASE
                              WHEN EXISTS(m.toUser) THEN [value IN m.toUser WHERE value <> \"''' + str(userId) + '''\"]
                              ELSE []
                           END
            '''
        session.run(queryWord1)

#处理修改的
def manageReviseInfo(reviseInfo,session):
    reviselinks=[]

    for i in reviseInfo['reviseContent']:
        if len(i)!=0:
            reviselinks.append([i[1],i[2],i[4],i[5],i[6]])#1超点id 2新建节点name 4关系name 5关系source 6关系reason
            # if len(i[0])!=0 and len(i[2])!=0:#和头尾节点都有关联i
            #
            #     reviselinks.append([reviseInfo['startNodeInfo'][1],i[1],i[0],i[3],i[5]])
            #     reviselinks.append([reviseInfo['endNodeInfo'][1],i[1],i[2],i[4],i[5]])
            # else:
            #     if len(i[0])==0:#只和尾节点关联
            #         reviselinks.append([reviseInfo['endNodeInfo'][1],i[1],i[2],i[4],i[5]])
            #     if len(i[2])==0:#只和头节点关联
            #         reviselinks.append([reviseInfo['startNodeInfo'][1],i[1],i[0],i[3],i[5]])
    for i in reviselinks:
        queryWord2 = '''
            MATCH (n)-[r]-(m:gpt)
            WHERE id(n) = ''' + i[0] + ''' and m.name =\"''' + i[1] + '''\"
            SET r.des = \"''' + i[2] + '''\",r.source=\"''' + i[3] + '''\",r.reason=\"''' + i[4] + '''\"
            '''
        session.run(queryWord2)
