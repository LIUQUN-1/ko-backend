from neo4j import GraphDatabase
from datetime import datetime
import json
def main(request):
    name1=request.POST['name1']
    name2=request.POST['name2']
    des1=request.POST['des1']
    des2=request.POST['des2']
    desR=request.POST['desR']
    driver = GraphDatabase.driver("bolt://114.213.232.140:17687", auth=("neo4j", "DMiChao"))
    session = driver.session()
    current_date = datetime.now().date()
    # 将日期格式化为整数
    formatted_date = int(current_date.strftime('%Y%m%d'))
    # 创建节点
    query_word2 = '''
        MERGE (n1:item:gpt {name: \"'''+name1+'''\"})
        MERGE (n2:item:gpt {name: \"'''+name2+'''\"})
        MERGE (n1)-[r:gptRelation]->(n2)
        set n1.des=COALESCE(n1.des,\"'''+des1+'''\"),n1.timestamp= COALESCE(n1.timestamp,\"'''+str(formatted_date)+'''\"),n1.url=COALESCE(n1.url,\"'''+"讯飞大模型"+'''\")
        set n2.des=COALESCE(n2.des,\"'''+des1+'''\"),n2.timestamp= COALESCE(n2.timestamp,\"'''+str(formatted_date)+'''\"),n2.url=COALESCE(n2.url,\"'''+"讯飞大模型"+'''\")
        set r.desc=\"'''+desR+'''\"
        return n1,n2
    '''

    result = session.run(query_word2)
    ans={}
    flag1=0
    flag2=0
    hyperid1=""
    hyperid2=""
    for res in result:
        if res["n1"] is not None:
            ans["n1"]=res["n1"].id
            if 'hyperNode' in list(set(res["n1"].labels)):
                flag1=1
                hyperid1=res["n1"].id
        if res["n2"] is not None:
            ans["n2"]=res["n2"].id
            if 'hyperNode' in list(set(res["n2"].labels)):
                flag2=1
                hyperid2=res["n2"].id
    if flag1==1:
        ans["n1"]=hyperid1
    if flag2==1:
        ans["n2"]=hyperid2
    #超点和非子节点建边了
    if flag1==1 or flag2==1:
        query_word3='''MATCH (a:item)-[r:gptRelation]-(b:item) WHERE id(a) = '''+str(ans["n1"])+''' and id(b)='''+str(ans["n2"])+''' DELETE r'''
        result1 = session.run(query_word3)
    return json.dumps(ans, ensure_ascii=False)
