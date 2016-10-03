# encoding=utf-8

import json

from jieba import analyse
from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


def encode(x):
    return x.encode("utf-8")


def analysis(entity):
    # seg_list = jieba.cut(entity[1], cut_all=True)
    seg_list = analyse.extract_tags(entity[1], 20, True)
    filename = "/vagrant/word/160930/" + entity[0]
    filtered_list = filter(lambda (x, y): x != "", seg_list)
    encode_list = map(lambda (x, y): (encode(x), y), filtered_list)
    r = json.dumps(encode_list, ensure_ascii=False)
    fo = open(filename, "a+")
    fo.write(r)
    fo.close()


rdd = sc.textFile('/vagrant/data/160930/*').map(lambda x: (json.loads(x)['id'], json.loads(x)['content']));
rdd.foreach(analysis)
