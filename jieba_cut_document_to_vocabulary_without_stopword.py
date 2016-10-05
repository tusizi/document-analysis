# encoding=utf-8

import json
import types

import jieba
from pyspark import SparkContext

sc = SparkContext(appName='CutDocument')


def encode(x):
    return x.encode("utf-8")


def cut(text):
    seg_list = list(jieba.cut(text, cut_all=True))
    filtered_list = filter(lambda x: x != "", seg_list)
    string_list = filter(lambda x: lambda x: type(x) is not types.FloatType, filtered_list)
    encode_list = map(lambda x: encode(x), string_list)
    return encode_list;


def output(items):
    filename = "/vagrant/vocabulary/data.txt"
    join = " ".join(items)
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


rdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: json.loads(x)['content'])
stopWordRdd = sc.textFile("stop_words.txt").collect()
cutWordRdd = rdd.map(cut)

result = cutWordRdd.map(lambda x: filter(lambda y: y not in stopWordRdd, x))
result.foreach(output)
