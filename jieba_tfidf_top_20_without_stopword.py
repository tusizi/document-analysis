# encoding=utf-8
# 使用结巴获取tfidf的前20个词,这个可以对每个文章处理，直接修改src_file就可以了

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
    filename = "/vagrant/result/jieba_tfidf_20.txt"
    filtered_list = filter(lambda (x, y): x != "", seg_list)
    string_list = filter(lambda (x, y): lambda x: type(x) is not types.FloatType, filtered_list)
    encode_list = map(lambda (x, y): (encode(x), y), string_list)
    r = json.dumps(encode_list, ensure_ascii=False)
    fo = open(filename, "a+")
    fo.write(r)
    fo.write("\n")
    fo.close()


src_file = '/vagrant/data/data.txt'
rdd = sc.textFile(src_file).map(lambda x: (json.loads(x)['id'], json.loads(x)['content']));
rdd.foreach(analysis)
