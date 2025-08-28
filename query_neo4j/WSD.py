import math
from collections import Counter
from query_neo4j.春节接口2 import XiaoQi
import pymysql
from collections import defaultdict

class DatabaseHandler:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish a database connection."""
        self.connection = pymysql.connect(**self.db_config)

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()

    def fetch_entities_by_keyword(self, keyword,userID):
        """Fetch entities associated with a specific keyword."""
        try:
            with self.connection.cursor() as cursor:
                # 查询文件名和文件ID
                sql_query = """
                    SELECT
                        t1.name,
                        t2.file_id
                    FROM
                        `file` AS t1
                    INNER JOIN
                        `entity_to_file` AS t2 ON t1.id = t2.file_id
                    WHERE
                        t2.entity = %s 
                        AND (t1.private = 0 OR t1.userid = %s);
                """
                # 使用参数化查询，避免 SQL 注入
                cursor.execute(sql_query, (keyword, int(userID)))
                initial_result = cursor.fetchall()

                docdict = {}
                docdict_plus={}
                file_dict = {}
                file_dict_rev = {}
                # 遍历初始结果集
                for row in initial_result:
                    file_name, file_id = row
                    # 查询与文件ID关联的实体
                    sql_query = """
                    SELECT
                        t1.id,
                        t1.name,
                        t2.entity,
                        t2.sim
                    FROM
                        `file` AS t1
                    INNER JOIN
                        `entity_to_file` AS t2 ON t1.id = t2.file_id
                    WHERE
                        t2.file_id = %s
                        AND (t1.private = 0 OR t1.userid = %s);
                    """
                    cursor.execute(sql_query, (file_id, int(userID)))
                    entities = cursor.fetchall()

                    # 如果文件名不在字典中，初始化一个空列表
                    if file_name not in docdict:
                        docdict[file_id] = []
                        docdict_plus[file_id]=[]

                    # 将实体添加到文件名对应的列表中
                    for entity_row in entities:
                        # print(entity_row)
                        entity = entity_row[2]
                        file_dict[file_id] = entity_row[1]
                        docdict[file_id].append(entity)
                        docdict_plus[file_id].append([entity,entity_row[3]])
                        file_dict_rev[entity_row[1]] = file_id
                return file_dict, docdict,docdict_plus, file_dict_rev
        except pymysql.MySQLError as e:
            print(f"Error: {e}")
            return None
def cut_text(text):
    return list(text)

def compute_similarity(text1, text2):

    cut_text1 = cut_text(text1)
    cut_text2 = cut_text(text2)

    counter1 = Counter(cut_text1)
    counter2 = Counter(cut_text2)

    dot_product = 0
    for word in counter1:
        if word in counter2:
            dot_product += counter1[word] * counter2[word]

    norm1 = math.sqrt(sum([counter1[word] ** 2 for word in counter1]))
    norm2 = math.sqrt(sum([counter2[word] ** 2 for word in counter2]))

    if norm1 == 0 or norm2 == 0:
        return 0
    else:
        return dot_product / (norm1 * norm2)

def out_fomat(most_similar_entity, most_key_list):
    dictRes = {}
    dictRes["entity"] = most_similar_entity
    dictRes["entity_with_keyword"] = most_key_list
    return dictRes
def xiaoqi_instance(file_text, entity_list):
    max_similarity = -1
    most_similar_entity = ""
    most_key_list = ""

    for entity in entity_list:
        phrase = ",".join(entity_list[entity])
        similarity = compute_similarity(file_text, phrase)
        if similarity > max_similarity:
            max_similarity = similarity
            most_similar_entity = entity
            most_key_list = phrase
    dictRes = out_fomat(most_similar_entity, most_key_list)
    return dictRes



def jiekou_1(content,userID):
    db_config = {
        'host': '114.213.234.179',
        'user': 'koroot',
        'password': 'DMiC-4092',
        'database': 'db_hp',
    }

    db_handler = DatabaseHandler(db_config)
    db_handler.connect()
    file_dict,docdict,docdict_plus,file_dict_rev = db_handler.fetch_entities_by_keyword(content,userID)
    # for key,value in docdict.items():
    #     print(str(key)+" "+str(value))
    return file_dict,docdict,docdict_plus,file_dict_rev
def jiekou_2(searchname,userID):
    '''
        python环境3.9 需要另外安装以下安装包：

        pip install gensim==3.3.0
        pip install scikit-learn==1.3.0
        pip install scipy==1.12.0
        pip install networkx

        3.10的话gensim调整为4.3.3，这个也是4.3.3的
        '''

    _,docdict,docdict_plus,file_dict_rev=jiekou_1(searchname,userID)
    print(docdict)
    if docdict=={}:
        print("没有任何文件！！！")
        return
    rest = XiaoQi(docdict, searchname)
    # print(rest)
    return rest
def get_most_relevant_keywords(file_keywords_list,top_n=5):
    """
    综合筛选与目标人物最相关的关键词

    参数:
        file_keywords_list (list): 每个文件的关键词列表
    返回:
        list: 与目标人物最相关的关键词
    """

    # 基于词频筛选
    high_freq_keywords = filter_by_frequency_and_score(file_keywords_list, top_n=top_n)

    return high_freq_keywords

def filter_by_frequency_and_score(file_keywords_list, top_n=5):
    """
    根据关键词在所有文件中的出现频率与其得分筛选前 N 个关键词

    参数:
        file_keywords_list (list): 每个文件的关键词和得分列表，元素为 (关键词, 得分)。
        top_n (int): 返回前 N 个高频且得分高的关键词

    返回:
        list: 高频且得分高的关键词列表
    """
    word_freq = defaultdict(int)  # 统计关键词的频率
    word_score = defaultdict(int)  # 统计关键词的累计得分

    # 统计所有关键词的频率与得分
    for file_keywords in file_keywords_list:
        for word, score in file_keywords:
            word_freq[word] += 1
            word_score[word] += score

    # 按频率和得分排序，首先按频率降序排列，若频率相同，则按得分降序排列
    sorted_words = sorted(word_freq.items(), key=lambda x: (x[1], word_score[x[0]]), reverse=True)

    # 返回前 N 个关键词
    return [word for word, _ in sorted_words[:top_n]]
def jiekou_3(content, userID):
    file_dict, docdict, docdict_plus, file_dict_rev = jiekou_1(content, userID)
    # print(docdict)
    if len(docdict) > 1:
        resdoccluster = jiekou_2(content, userID)
        print(1)
        print(resdoccluster)
    elif len(docdict) == 1:
        resdoccluster = {}
        for key, value in docdict.items():
            resdoccluster[str(content) + "1"] = [key]
    else:
        return {}, []
    docdict = docdict_plus
    result = {}
    for target, filenames in resdoccluster.items():
        relate_filekeyword = []
        temp1 = []
        for filename in filenames:
            # print(filename)
            relate_filekeyword.append(docdict[str(filename)])
            for i11 in docdict[str(filename)]:
                temp1.append(i11)
        unique_data = {item[0]: item for item in temp1}.values()

        temp1 = list(unique_data)
        temp1 = sorted(temp1, key=lambda x: x[1], reverse=True)
        temp = []
        for i in temp1:
            temp.append(i[0])
        relevant_keywords = get_most_relevant_keywords(
            relate_filekeyword,
            top_n=5
        )
        result[target] = [relevant_keywords, filenames]
    return result, file_dict, file_dict_rev
if __name__ == "__main__":
    # 初版实时消歧接口，针对上传的新文件，实时识别该文件相关的实体。本接口不借助任何第三方工具包开发，无需安装任何工具包即可运行。
    # file_text 上传文件解析的文本内容
    # entity_list 文件相关实体词对应到歧义实体列表，包含每个实体的标识词等
    # most_similar 返回与该文件最相似的实体
    file_path = "bb/汪萌.html"
    file_text = "汪萌，1984年12月生，湖北监利人，汉族，中共党员，多媒体信息处理与模式识别专家 [1]，合肥工业大学党委常委、副校长、教授、博士生导师。 [6]\
汪萌于2003年7月获得中国科学技术大学信号与信息处理学士学位 [14]；2008年7月获得中国科学技术大学信号与信息处理博士学位；2008年7月—2011年8月先后在微软亚洲研究院、\
新加坡国立大学从事科学研究工作；2011年8月任合肥工业大学黄山学者特聘教授；2012年入选教育部“新世纪优秀人才支持计划”；2013年获得“国家自然科学基金委优秀青年科学基金”资助；\
2016年1月起历任合肥工业大学计算机与信息学院（人工智能学院）副院长、执行院长，合肥工业大学软件学院院长；2017年任合肥工业大学计算机与信息学院院长，同年获得“国家自然科学基金委杰出青年科学基金”资助 [1]，\
12月任中国共产主义青年团安徽省第十四届委员会副书记 [9]；2019年12月任合肥综合性国家科学与中心人工智能研究院副院长 [6]；2023年7月任合肥工业大学党委常委、副校长。 [5]\
汪萌主要研究领域为多媒体信息处理与模式识别。"

    entity_list = {
        "汪萌1":['汪萌', '教授', '院长', '学院', '合肥工业大学'],
        "汪萌2":["重庆大学"],
        "汪萌3":['汪萌', '事物', '汉族', '男', '两点'],
    }
    dictRes = xiaoqi_instance(file_text, entity_list)
    dictRes["minio_file_path_list"] = [file_path]
    print(dictRes)
