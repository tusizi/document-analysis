# encoding=utf-8

import csv
import json
import math
import types

from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


"""
        //读入带有权重的词列表文件
        Input vocabulary.txt
        //读入原文中tag的列表文件
        Input tag.txt

         //解析每行的词和权重放入列表
        List<List<Vocabulary>> vocabularyList <- extract vocabulary.txt

        //解析每一个tag并且把tag的权重设为默认值为1
        List<List<Vocabulary>> tagList <- extract tag.txt

        //循环文章的词和权重的列表
        for index,vocabularies <- vocabularyList {
            //获取当前文章关联的tag列表
            tags <- tagList[index]

            //把词列表和tag列表去重合并到一起
            unionList = union(tags,vocabularies)

            //定义补0后的tag和vocabularies
            combine_tag = []
            combine_vocabularies = []

            //循环unionList，分别构造新的两个列表，没有的位置补0
            for index,word <- unionList {
                //如果tag里面包含当前词获取权重，否则补0
                if tags contains word
                    combine_tag[index] <- combine_tag[word].percent
                else combine_tag[index] <- 0

                //如果词表里面包含当前词获取权重，否则补0
                if vocabularies contains word
                    combine_vocabularies[index] <- vocabularies[word].percent
                else combine_vocabularies[index] <- 0
            }

            //定义分子分母
            cell,floor1,floor2

            //循环unionList计算分子和分母
             for index <- unionList {

                //tag和词的权重相乘，追加到分子上
                cell <- cell + combine_vocabularies[index] * combine_tag[index]
                //分别计算tag和词的权重的平方，追加到需要加和的分母
                floor1 += combine_vocabularies[index] * combine_vocabularies[index]
                floor2 += combine_tag[index] * combine_tag[index]
            }

            //计算结果并输出
            Output cell/(sqrt floor1 + sqrt floor2)
        }
        //话题里面每一个词的内容和权重的类
        class Vocabulary {
            具体的词汇名字
            name

            当前词在topic中的权重值
            percent
        }
"""

def add(i, l):
    if i not in flatten(l):
        l.append(i)


def get_percent(item, l):
    for i in l:
        if item in i:
            return i[1]

    return 0


def vsm(entity):
    r = json.dumps(entity, ensure_ascii=False)
    vec_list = entity[1]
    tag_list = entity[0][1]
    vec_tag_list = filter(lambda x: type(x) is not types.FloatType, flatten(vec_list))
    union_list = list(vec_tag_list)

    for i in tag_list:
        add(i, union_list)

    vec_vector_list = []
    tag_vector_list = []

    for item in union_list:
        if item in tag_list:
            tag_vector_list.append(1)
        else:
            tag_vector_list.append(0)
        if item in vec_tag_list:
            vec_vector_list.append(get_percent(item, vec_list))
        else:
            vec_vector_list.append(0)

    cell = 0
    floor1 = 0
    floor2 = 0
    floor = 0
    index = 0
    for i in union_list:
        cell = cell + tag_vector_list[index] * vec_vector_list[index]
        floor1 += tag_vector_list[index] * tag_vector_list[index]
        floor2 += vec_vector_list[index] * vec_vector_list[index]
        index += 1
    floor = math.sqrt(floor1) * math.sqrt(floor2)
    if floor == 0:
        rt = 0.0
    else:
        rt = cell / floor
    return [rt]


def flatten(vec_list):
    return [y for x in vec_list for y in x]

tagRdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: (json.loads(x)['id'], json.loads(x)['tags']))
vecRdd = sc.textFile('/vagrant/word/data.txt').map(lambda x: (json.loads(x)))

rdd = tagRdd.zip(vecRdd)
csvFile = file('vsm.csv', 'wb')
writer = csv.writer(csvFile)
writer.writerow(['percent'])

result = rdd.map(vsm).collect()

writer.writerows(result)

csvFile.close()
