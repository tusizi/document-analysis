# encoding=utf-8
# 使用结巴去停用词 并且按照给定的列表获取词性，用来处理所有文章用的，输出来的结果是给JAVA的LDA程序使用
import json

import jieba.posseg as pseg
from pyspark import SparkContext

sc = SparkContext(appName='CutDocument')


# 分词并抽取特定词性的词
def cut(text):
    seg_list = list(pseg.cut(text))
    al = ["n", "nr", "nr1", "nr2", "nrj", "nrf", "ns", "nsf", "nt", "nz", "nl", "ng", "s", "f", "v", "vd", "vn", "vf",
          "vx", "vi", "vl", "vga", "ad", "an", "ag", "al"]
    filtered_list = filter(lambda (word, flag): flag in al, seg_list)
    encode_list = map(lambda (x, y): x.encode("utf-8"), filtered_list)
    return encode_list;


# 格式化输出
def output(items):
    filename = "/vagrant/vocabulary/mominal.txt"
    join = " ".join(items)
    fo = open(filename, "a+")
    fo.write(join)
    fo.write("\n")
    fo.close()


# 去除停用词
def filter_stopword(l):
    return filter(lambda m: m not in stopWordRdd, l)

# 读入源文件，每一行就是一篇文章，里面的content属性就是文章内容，解析出文章内容
src_file = '/vagrant/data/data.txt'
rdd = sc.textFile(src_file).map(lambda x: json.loads(x)['content'])

# 读入停用词表
stopWordRdd = sc.textFile("stop_words.txt").map(lambda x: x.encode("utf-8")).collect()

# 循环分词，得到的结果过滤停用词，最后输出到文件
result = rdd.map(cut).map(filter_stopword).foreach(output)