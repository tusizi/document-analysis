# encoding=utf-8

import json

from pyspark import SparkContext

sc = SparkContext(appName='DocumentsRewrite')


def rewrite(entity):
    filename = "/vagrant/data/data.txt"
    r = json.dumps(entity, ensure_ascii=False)
    fo = open(filename, "a+")
    fo.write(r.encode("utf-8"))
    fo.write("\n")
    fo.close()


rdd = sc.textFile('/vagrant/data/160930/*').map(lambda x: json.loads(x));
rdd.foreach(rewrite)
