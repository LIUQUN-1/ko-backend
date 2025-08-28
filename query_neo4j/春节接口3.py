import json
from collections import defaultdict

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



if __name__ == "__main__":
    # 读取JSON文件
    with open("input(1).json", "r", encoding="utf-8") as f:
        data = json.load(f)

    resdoccluster = data[0]
    docdict = data[1]
    result = []
    for target,filenames in resdoccluster.items():
        relate_filekeyword = []
        for filename in filenames:
            relate_filekeyword.append(docdict[filename])
        relevant_keywords = get_most_relevant_keywords(
            relate_filekeyword,
            top_n=5
        )
        result.append({target:relevant_keywords})
    # 保存为JSON文件
    with open('output.json', "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
