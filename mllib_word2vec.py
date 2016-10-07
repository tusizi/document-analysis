# encoding=utf-8
from __future__ import print_function

import json
import operator

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

sc = SparkContext(appName='Word2Vec')

vocabularyRdd = sc.textFile('/vagrant/vocabulary/data.txt').map(lambda row: row.split(" "))
ldaRdd = sc.textFile('/vagrant/word/data.txt')

word2vec = Word2Vec()
model = word2vec.fit(vocabularyRdd)


def output(value):
    filename = "/vagrant/word2vec/data.txt"
    r = json.dumps(value, ensure_ascii=False)
    fo = open(filename, "a+")
    fo.write(r.encode("utf-8"))
    fo.write("\n")
    fo.close()


def get_synonyms(lda_list):
    word_dict = {}
    for item in lda_list:
        word_dict[item[0]] = item[1]
        synonyms = model.findSynonyms(item[0], 20)
        for s_item in synonyms:
            if word_dict.has_keys(s_item[0]):
                word_dict[s_item[0]] = word_dict[s_item[0]] + item[1] * s_item[1]
            else:
                word_dict[s_item[0]] = item[1] * s_item[1]
    sorted_dict = sorted(word_dict.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_dict


ldaRdd.map(get_synonyms).foreach(output)
sc.stop()
