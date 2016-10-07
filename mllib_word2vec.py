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

"""
        //读入带有权重的词列表文件
        Input vocabulary.txt
        //读入刚分好的词的列表文件
        Input word.txt

         //解析每行的词和权重放入列表
        List<List<Vocabulary>> vocabularyList <- extract vocabulary.txt

        //把读入的word文件调用word2vec然后把模型保存到model里面
        model <- word2vec fit word.txt

        //循环文章的词和权重的列表
        for vocabularies <- vocabularyList {

            //定义可以由文字做key的map
            Map<String, Double> wordMap

            //循环每一个文章的词
            for word,percent <- vocabularies {
                //调用w2v生成的model，获取相似的20个词
                relatedWords <- get related from word2vec

                //循环相关词
                for rword,rpercent <- relatedWords {
                    //如果在map中就相加他们的权重
                    if rword in word map :
                        wordMap[rword] <- wordMap[rword] + rpercent * percent
                    //如果不在map里面就放进去
                    else :
                        wordMap[rword] <- rpercent * percent
                }
                //如果当前词
            }

            //输出排序后的权重最高的20个词
            Output top 20 words and percent
        }

        //话题里面每一个词的内容和权重的类
        class Vocabulary {
            具体的词汇名字
            name

            当前词在topic中的权重值
            percent
        }
"""


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
