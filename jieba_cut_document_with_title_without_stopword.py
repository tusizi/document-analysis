# encoding=utf-8
# 使用结巴抽取文章的标题和内容两个里面的分词,这个也是用于处理多个文章的，另一种思路输出内容给LDA
import json
import types

from jieba import analyse
from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


def encode(x):
    return x.encode("utf-8")


def analysis(entity):
    analyse.set_stop_words("stop_words.txt")
    content_list = analyse.extract_tags(entity[1] + entity[0], 500, False)
    filtered_list = filter(lambda x: x != "", content_list)
    string_list = filter(lambda x: lambda x: type(x) is not types.FloatType, filtered_list)
    encode_list = map(lambda x: encode(x), string_list)
    join = " ".join(encode_list)
    filename = "/vagrant/result/jieba_tfidf_with_title.txt"
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


rdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: (json.loads(x)['title'], json.loads(x)['content']));
rdd.foreach(analysis)
