# encoding=utf-8

import json
import types

from jieba import analyse
from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


def encode(x):
    return x.encode("utf-8")


def analysis(entity):
    analyse.set_stop_words("stop_words.txt")
    seg_list = analyse.extract_tags(entity[1], 500, False)
    filtered_list = filter(lambda x: x != "", seg_list)
    string_list = filter(lambda x: lambda x: type(x) is not types.FloatType, filtered_list)
    encode_list = map(lambda x: encode(x), string_list)
    join = " ".join(encode_list)
    filename = "/vagrant/vocabulary/jieba.txt"
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


rdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: (json.loads(x)['id'], json.loads(x)['content']));
rdd.foreach(analysis)
