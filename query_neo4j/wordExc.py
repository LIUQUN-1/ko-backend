import jieba
import jieba.posseg as pseg
def extract_words(words, tags):
    extracted_words = []
    i = 0
    while i < len(tags):
        if tags[i] == 'n' or tags[i] == 'nr' or tags[i] == 'nz' or tags[i] == 'eng':
            j = i + 1
            while j < len(tags) and tags[j] not in ['p', 'c']:
                j += 1
            extracted_words.append(''.join(words[i:j]))
            i = j - 1
        elif tags[i] == 'c' or tags[i] == 'p':
            j = i + 1
            while j < len(tags) and tags[j] not in ['nd', 'uj']:
                j += 1
            extracted_words.append(''.join(words[i+1:j]))
            i = j - 1
        i += 1
    res=[]
    res.append(extracted_words[0])
    res.append(extracted_words[1])
    return res

# words = [['请', '告诉', '我', '计算机', '网络', '和', '图论', '的', '关系']]
# tags = [['v', 'v', 'r', 'n', 'n', 'c', 'n', 'u', 'n']]
# words = [['说明', '吴信东', '和', '计算机', '网络', '的', '关系']]
# tags =[['v', 'nh', 'c', 'n', 'n', 'u', 'n']]
# words = [['说明', '吴信东', '和', '卜晨阳', '的', '关系']]
# tags =[['v', 'nh', 'c', 'nh', 'u', 'n']]
# words =[['请', '告诉', '我', '计算机', '网络', '和', '图论', '的', '关系']]
# tags =[['v', 'v', 'r', 'n', 'n', 'c', 'n', 'u', 'n']]
# words =[['计算机', '网络', '和', '图论']]
# tags =[['n', 'n', 'c', 'n']]
# words =[['计算机', '网络', '和', '数据', '挖掘']]
# tags =[['n', 'n', 'c', 'n', 'v']]
# result = extract_words(words[0], tags[0])
# print(result)
def start(sentence):

    try:
        seg_list = jieba.cut(sentence, cut_all=False)
        res = pseg.cut(sentence)
        words = []
        flags = []
        for word, flag in res:
            words.append(word)
            flags.append(flag)
        result = extract_words(words, flags)
        # result = extract_words(extract.cws[0], extract.pos[0])

    except IndexError:
        result=['根科目','图论']
    return result
if __name__ == '__main__':
    print(start("吴信东和卜晨阳"))
