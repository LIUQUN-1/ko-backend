import datetime
from gensim.models import Word2Vec
from sklearn.cluster import DBSCAN
import random
import math
import numpy as np
import networkx as nx
import argparse


def parameter_parser():
    """
    A method to parse up command line parameters.
    The default hyperparameters give a good quality representation without grid search.
    Representations are sorted by ID.
    """
    parser = argparse.ArgumentParser(description="Run RoleBased2Vec_for_multiplex.")

    parser.add_argument("--input",
                        nargs="?",
                        default=r"../data/",
                        help="Input graph path -- edge list edges.")

    parser.add_argument("--output",
                        nargs="?",
                        default=r"../output/",
                        help="Embeddings output path.")

    parser.add_argument('--dataset', nargs='?', default='brazil-test/',
                        help='Input graph path ')
    ##后面自己加的
    parser.add_argument("--output-rolelist",
                        nargs="?",
                        default=r"../output/",
                        help="role fesature path.")

    parser.add_argument("--walk_length",
                        type=int,
                        default=60,
                        help="Walk length. Default is 80.")

    parser.add_argument("--num_walks",
                        type=int,
                        default=4,
                        help="Number of random walks. Default is 10.")

    parser.add_argument("--dimensions",
                        type=int,
                        default=20,
                        help="Number of dimensions. Default is 128.")

    parser.add_argument("--window_size",
                        type=int,
                        default=14,
                        help="Window size for skip-gram. Default is 10.")

    parser.add_argument("--alpha",
                        type=float,
                        default=0.02,
                        help="Initial learning rate. Default is 0.025.")

    parser.add_argument("--matrix_reg",
                        type=float,
                        default=0.05,
                        help="Final learning rate. Default is 0.025.")

    parser.add_argument("--num_epoch",
                        type=int,
                        default=200,
                        help="num_iters. Default is 2.")

    parser.add_argument("--epochs",
                        type=int,
                        default=10,
                        help="Number of epochs. Default is 10.")

    parser.add_argument("--seed",
                        type=int,
                        default=42,
                        help="Sklearn random seed. Default is 42.")

    return parser.parse_args(args=[])


class DataSet():

    def __init__(self, file_path="./"):

        self.file_path = file_path  # 待消歧文件路径
        self.paper_authorlist_dict = {}  # 每篇论文的作者集去掉待消歧名字后剩下的作者集合，列表只有一个作者的则不保存
        self.paper_list = []  # 论文列表索引，从1开始保存
        self.coauthor_list = []  # 将所有的合作者的名字保留起来
        self.label_list_dict = {}  # 每篇论文的真实标记集合
        self.C_Graph = nx.Graph()  # 合作者网络
        self.D_Graph = nx.Graph()  # 文献网络
        self.num_nnz = 0  # 三个网络的边集合
        self.paper_doc_dict = {}

    def reader_fromDict(self, dictList, searchname, hop=1):
        paper_index = 0
        coauthor_set = set()
        ego_name = searchname
        for dic in dictList:
            paper_index += 1  ##表示序号从1开始，每一篇论文
            self.paper_list.append(paper_index)
            self.paper_doc_dict[paper_index] = dic
            author_list = dictList[dic]
            if len(author_list) > 1:
                if ego_name in author_list:
                    author_list.remove(ego_name)
                    self.paper_authorlist_dict[paper_index] = author_list
                else:
                    self.paper_authorlist_dict[paper_index] = author_list

                for co_author in author_list:
                    coauthor_set.add(co_author)

                # construct the coauthorship graph
                for pos in range(0, len(author_list) - 1):  # range in python3
                    for inpos in range(pos + 1, len(author_list)):
                        src_node = author_list[pos]
                        dest_node = author_list[inpos]
                        if not self.C_Graph.has_edge(src_node, dest_node):
                            self.C_Graph.add_edge(src_node, dest_node, weight=1)
                        else:
                            edge_weight = self.C_Graph[src_node][dest_node]['weight']
                            edge_weight += 1
                            self.C_Graph[src_node][dest_node]['weight'] = edge_weight
            else:
                self.paper_authorlist_dict[paper_index] = []  # 只选择合作者数量大于1的论文
        self.coauthor_list = list(coauthor_set)
        paper_2hop_dict = {}
        for paper_idx in self.paper_list:
            temp = set()
            if self.paper_authorlist_dict[paper_idx] != []:
                for first_hop in self.paper_authorlist_dict[paper_idx]:
                    temp.add(first_hop)
                    if self.C_Graph.has_node(first_hop):
                        for snd_hop in self.C_Graph.neighbors(first_hop):
                            temp.add(snd_hop)
            paper_2hop_dict[paper_idx] = temp
        if hop == 1:
            dochopdict = self.paper_authorlist_dict
        else:
            dochopdict = paper_2hop_dict

        for node in self.paper_list:
            self.D_Graph.add_node(str(node))

        for idx1 in range(0, len(self.paper_list) - 1):
            for idx2 in range(idx1 + 1, len(self.paper_list)):
                temp_set1 = set(dochopdict[self.paper_list[idx1]])
                temp_set2 = set(dochopdict[self.paper_list[idx2]])

                edge_weight = len(temp_set1.intersection(temp_set2))
                if edge_weight != 0:
                    # self.D_Graph.add_edge(self.paper_list[idx1],
                    #                       self.paper_list[idx2],
                    #                       weight=edge_weight)
                    self.D_Graph.add_edge(str(self.paper_list[idx1]),
                                          str(self.paper_list[idx2]),
                                          weight=edge_weight)

    def getordlabel(self):

        label = []
        for id in self.label_list_dict:
            label.append(self.label_list_dict[int(id)])
        return label


def sigmoid(x):
    return float(1) / (1 + math.exp(-x))


class BprOptimizer():

    def __init__(self, latent_dimen, alpha, matrix_reg):
        self.latent_dimen = latent_dimen
        self.alpha = alpha
        self.matrix_reg = matrix_reg

    def init_model(self, dataset, W, id2word):
        word2id = {}
        for idx, con in enumerate(id2word):
            word2id[con] = idx
        self.paper_latent_matrix = {}
        for paper_idx in dataset.paper_list:
            self.paper_latent_matrix[paper_idx] = W[word2id[str(paper_idx)], :]

    def update_dd_gradient(self, fst, snd, third):
        x = self.predict_score(fst, snd, "dd") - \
            self.predict_score(fst, third, "dd")
        common_term = sigmoid(x) - 1

        grad_fst = common_term * (self.paper_latent_matrix[snd] - \
                                  self.paper_latent_matrix[third]) + \
                   2 * self.matrix_reg * self.paper_latent_matrix[fst]
        self.paper_latent_matrix[fst] = self.paper_latent_matrix[fst] - \
                                        self.alpha * grad_fst

        grad_snd = common_term * self.paper_latent_matrix[fst] + \
                   2 * self.matrix_reg * self.paper_latent_matrix[snd]
        self.paper_latent_matrix[snd] = self.paper_latent_matrix[snd] - \
                                        self.alpha * grad_snd

        grad_third = -common_term * self.paper_latent_matrix[fst] + \
                     2 * self.matrix_reg * self.paper_latent_matrix[third]
        self.paper_latent_matrix[third] = self.paper_latent_matrix[third] - \
                                          self.alpha * grad_third

    def compute_dd_loss(self, fst, snd, third):
        x = self.predict_score(fst, snd, "dd") - \
            self.predict_score(fst, third, "dd")
        ranking_loss = -np.log(sigmoid(x))

        complexity = 0.0
        complexity += self.matrix_reg * np.dot(self.paper_latent_matrix[fst],
                                               self.paper_latent_matrix[fst])
        complexity += self.matrix_reg * np.dot(self.paper_latent_matrix[snd],
                                               self.paper_latent_matrix[snd])
        complexity += self.matrix_reg * np.dot(self.paper_latent_matrix[third],
                                               self.paper_latent_matrix[third])
        return ranking_loss + complexity

    def predict_score(self, fst, snd, graph_type):
        if graph_type == "dd":
            return np.dot(self.paper_latent_matrix[fst], self.paper_latent_matrix[snd])


def dbscan(D_matrix):
    return DBSCAN(eps=1.5, min_samples=2).fit_predict(D_matrix)


def construct_doc_matrix(dict, paper_list):
    D_matrix = dict[paper_list[0]]
    for idx in range(1, len(paper_list)):
        D_matrix = np.vstack((D_matrix, dict[paper_list[idx]]))
    return D_matrix


def generate_triplet_randwalk(cur, walk, nodeList):
    d_i = cur
    d_j = random.choice(list(set(walk)))
    neg_list = set(nodeList).difference(walk)
    while True:
        if d_j != d_i:
            d_t = random.choice(list(neg_list))
            yield d_i, d_j, d_t
            break
        else:
            d_j = random.choice(list(set(walk)))


def testhelper(num_epoch, dataset, bpr_optimizer, walks, W, id2word):
    bpr_optimizer.init_model(dataset, W, id2word)
    nodeList = list(dataset.paper_list)
    for _ in range(0, num_epoch):
        bpr_loss = 0.0
        for walk in walks:
            if len(set(walk)) < 2 or len(set(walk)) == len(set(nodeList)):
                continue
            for idx, cur in enumerate(walk):
                for i, j, t in generate_triplet_randwalk(cur, walk, nodeList):
                    bpr_optimizer.update_dd_gradient(i, j, t)
                    bpr_loss += bpr_optimizer.compute_dd_loss(i, j, t)
        D_matrix = construct_doc_matrix(bpr_optimizer.paper_latent_matrix, dataset.paper_list)
        y_pred = dbscan(D_matrix)
    return y_pred


def OutForm(dataset, y_pred, searchname):
    ii = 0
    ydict = {}
    for yi in y_pred:
        if yi in ydict:
            ydict[yi].append(dataset.paper_list[ii])
        else:
            ydict[yi] = [dataset.paper_list[ii]]
        ii = ii + 1
    dataset.paper_doc_dict
    resdict = {}
    rni = 1
    for keyi in ydict:
        tnamek = searchname + str(rni)
        tmp = []
        for vi in ydict[keyi]:
            tmp.append(dataset.paper_doc_dict[int(vi)])
        resdict[tnamek] = tmp
        rni = rni + 1
    return resdict


class RoleBased2Vec():
    def __init__(self, args, G, num_walks, walk_length):  #
        self.args = args
        self.G = G
        self.is_directed = False
        self.num_walks = num_walks
        self.walk_length = walk_length

    def Comwalk_step(self, v):
        nbs = list(self.G.neighbors(v))
        wei_dict = self.G.adj  ##
        weight_1 = [1] * len(nbs)
        for i, x in enumerate(nbs):
            weight_1[i] = wei_dict[v][x]['weight']  ##
        weights = weight_1
        return random.choices(nbs, weights=weights, k=1)[0]

    def random_walk(self):
        # random walk with every node as start point
        walks = []
        for node in self.G.nodes():
            walk = [node]
            nbs = list(self.G.neighbors(node))
            if len(nbs) > 0:
                walk.append(random.choice(nbs))
                for i in range(2, self.walk_length):
                    # v = self.walk_step(walk[-1])
                    v = str(self.Comwalk_step(walk[-1]))
                    if not v:
                        break
                    walk.append(v)
            walk = [str(x) for x in walk]
            walks.append(walk)
        return walks

    def Comsentenses(self):
        sts = []
        for _ in range(self.num_walks):
            sts.extend(self.random_walk())
        return sts


def model(dataset, args):
    G = dataset.D_Graph
    model = RoleBased2Vec(args, G, num_walks=args.num_walks, walk_length=args.walk_length)
    walks = model.Comsentenses()
    w2v = Word2Vec(sentences=walks, vector_size=args.dimensions, window=args.window_size, epochs=2, sg=1, hs=1, min_count=0,
                   workers=4)
    W = w2v.wv.vectors
    id2word = w2v.wv.index_to_key
    walks = list(map(lambda pair: list(map(int, pair)), walks))
    return walks, W, id2word


def XiaoQi(docdict, searchname):
    args = parameter_parser()
    args.alpha = 0.006734688489761792
    args.dimensions = 30
    args.matrix_reg = 0.01837016916318325
    args.num_epoch = 100
    args.num_walks = 6
    args.walk_length = 40
    args.window_size = 40

    start = datetime.datetime.now()
    dataset = DataSet()
    dataset.reader_fromDict(docdict, searchname, hop=1)
    walks, W, id2word = model(dataset, args)
    bpr_optimizer = BprOptimizer(args.dimensions, args.alpha, args.matrix_reg)
    y_pred = testhelper(args.num_epoch, dataset, bpr_optimizer, walks, W, id2word)
    rescluster = OutForm(dataset, y_pred, searchname)
    end = datetime.datetime.now()
    runtime = end - start
    print(runtime)
    return rescluster


if __name__ == "__main__":
    '''
    python环境3.9 需要另外安装以下安装包：

    pip install gensim==4.3.3
    pip install scikit-learn==1.3.0
    pip install scipy==1.12.0
    pip install networkx
    
    3.10的话gensim调整为4.3.3，这个也是4.3.3的
    '''

    docdict = {
        "文件名1": ["合肥工业大学", "杰出青年", "吴信东", "中国", "汪萌"],
        "文件名2": ["合肥工业大学", "科技探索奖", "中国", "论文", "汪萌"],
        "文件名3": ["成都大学", "中国", "汪萌"],
        "文件名4": ["重庆医科大学", "汪萌"],
        "文件名5": ["重庆大学", "汪萌"],
        "文件名6": ["重庆医大学", "汪萌"],
    }
    searchname = "汪萌"
    rest = XiaoQi(docdict, searchname)
    print(rest)