# encoding=utf-8
# 使用结巴去停用词 并且按照给定的列表获取词性，用来处理所有文章用的，输出来的结果是给JAVA的LDA程序使用
import json

import jieba.posseg as pseg
from pyspark import SparkContext

sc = SparkContext(appName='CutDocument')


def encode(x):
    return x.encode("utf-8")


def cut(text):
    seg_list = list(pseg.cut(text))
    al = ["n", "nr", "nr1", "nr2", "nrj", "nrf", "ns", "nsf", "nt", "nz", "nl", "ng", "s", "f", "v", "vd", "vn", "vf",
          "vx", "vi", "vl", "vga", "ad", "an", "ag", "al"]
    filtered_list = filter(lambda (word, flag): flag in al, seg_list)
    encode_list = map(lambda (x, y): encode(x), filtered_list)
    return encode_list;


def output(items):
    filename = "/vagrant/vocabulary/mominal.txt"
    join = " ".join(items)
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


src_file = '/vagrant/data/data.txt'
rdd = sc.textFile(src_file).map(lambda x: json.loads(x)['content'])
stopWordRdd = sc.textFile("stop_words.txt").map(lambda x: x.encode("utf-8")).collect()
cutWordRdd = rdd.map(cut)


def filter_item(l):
    return filter(lambda m: m not in stopWordRdd, l)


result = cutWordRdd.map(filter_item)
result.foreach(output)
