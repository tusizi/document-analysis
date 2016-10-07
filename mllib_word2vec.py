# encoding=utf-8
from __future__ import print_function

import json
import operator
import tempfile

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel

sc = SparkContext(appName='Word2Vec')

vocabularyRdd = sc.textFile('/vagrant/vocabulary/data.txt').map(lambda row: row.split(" "))
ldaRdd = sc.textFile('/vagrant/word/data.txt').map(json.loads)

word2vec = Word2Vec()
model = word2vec.fit(vocabularyRdd)
path = tempfile.mkdtemp()
model.save(sc, path)


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
        same_model = Word2VecModel.load(sc, path)
        synonyms = same_model.findSynonyms(item[0], 20)
        for word, cosine_distance in synonyms:
            print("{}: {}".format(word.encode("utf-8"), cosine_distance))
            if word_dict.has_key(word):
                word_dict[word] = word_dict[word] + item[1] * cosine_distance
            else:
                word_dict[word] = item[1] * cosine_distance
    sorted_dict = sorted(word_dict.items(), key=operator.itemgetter(1), reverse=True)
    output(sorted_dict)


map(lambda x: get_synonyms(x), ldaRdd.collect())

sc.stop()
