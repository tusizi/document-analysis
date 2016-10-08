# encoding=utf-8
# 把多个文章合并成一个文章

import json

from pyspark import SparkContext

sc = SparkContext(appName='DocumentsRewrite')


def rewrite(entity):
    dist_file = "/vagrant/data/data.txt"
    r = json.dumps(entity, ensure_ascii=False)
    fo = open(dist_file, "a+")
    fo.write(r.encode("utf-8"))
    fo.write("\n")
    fo.close()


src_files = '/vagrant/data/160930/*'
rdd = sc.textFile(src_files).map(lambda x: json.loads(x));
rdd.foreach(rewrite)
