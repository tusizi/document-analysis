# encoding=utf-8

import json

import jieba
from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


def cut(id, sentences):
    seg_list = jieba.cut(sentences, cut_all=True)
    filename = "/vagrant/word/160930/" + id
    r = ",".join(seg_list)
    fo = open(filename, "a+")
    fo.write(r.encode("utf-8"))
    fo.close()


rdd = sc.textFile('/vagrant/data/160930/*').map(lambda x: (json.loads(x)['id'], json.loads(x)['content']));
rdd.foreach(cut)
