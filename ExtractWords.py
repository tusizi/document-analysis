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
    seg_list = analyse.extract_tags(entity[1], 20, True)
    filename = "/vagrant/word/160930/" + entity[0]
    filtered_list = filter(lambda (x, y): x != "", seg_list)
    string_list = filter(lambda (x, y): lambda x: type(x) is not types.FloatType, filtered_list)
    encode_list = map(lambda (x, y): (encode(x), y), string_list)
    r = json.dumps(encode_list, ensure_ascii=False)
    fo = open(filename, "a+")
    fo.write(r)
    fo.close()


rdd = sc.textFile('/vagrant/data/160930/*').map(lambda x: (json.loads(x)['id'], json.loads(x)['content']));
rdd.foreach(analysis)
