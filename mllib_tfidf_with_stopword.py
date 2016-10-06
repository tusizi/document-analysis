# encoding=utf-8

import json

import jieba
from pyspark import SparkContext
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer


def tf_me(cor):
    vectorizer = CountVectorizer()

    # 该类会统计每个词语的tf-idf权值
    transformer = TfidfTransformer()

    # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))

    # 获取词袋模型中的所有词语
    word = vectorizer.get_feature_names()

    # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
    weight = tfidf.toarray()

    # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
    for i in range(len(weight)):
        filename = "/vagrant/vocabulary/mllib.txt"
        for j in range(len(word)):
            r = json.dumps([word[j], weight[i][j]], ensure_ascii=False)
            fo = open(filename, "a+")
            fo.write(r.encode("utf-8"))
        fo.write("\n")
        fo.close()


sc = SparkContext(appName='DocumentsRewrite')
rdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: json.loads(x)['content'])


# corpus = rdd.map(lambda x: (jieba.cut(x, cut_all=True)))


def cut(x):
    # print x.encode("utf-8")
    list = jieba.cut(x, cut_all=True)
    result = []
    for value in list:
        result.append(value)
    return " ".join(result)


corpus = rdd.map(cut).collect()
# 该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频
# for i in corpus:
#     print i.encode("utf-8")

tf_me(corpus)
