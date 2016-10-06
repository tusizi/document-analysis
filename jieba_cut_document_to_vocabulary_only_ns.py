# encoding=utf-8

import json

import jieba.posseg as pseg
from pyspark import SparkContext

sc = SparkContext(appName='CutDocument')


def encode(x):
    return x.encode("utf-8")


def cut(text):
    seg_list = list(pseg.cut(text))
    filtered_list = filter(lambda (word, flag): flag != "x" and flag != "d", seg_list)
    encode_list = map(lambda (x, y): encode(x), filtered_list)
    return encode_list;


def output(items):
    filename = "/vagrant/vocabulary/pseg.txt"
    join = " ".join(items)
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


rdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: json.loads(x)['content'])
stopWordRdd = sc.textFile("stop_words.txt").map(lambda x: x.encode("utf-8")).collect()
cutWordRdd = rdd.map(cut)


def filter_item(l):
    return filter(lambda m: m not in stopWordRdd, l)


result = cutWordRdd.map(filter_item)
result.foreach(output)
