# encoding=utf-8

import csv
import json
import math
import types

from pyspark import SparkContext

sc = SparkContext(appName='DocumentAnalysis')


def encode(x):
    return x.encode("utf-8")


def add(i, l):
    if i not in flatten(l):
        l.append(i)


def get_percent(item, l):
    for i in l:
        if item in i:
            return i[1]

    return 0


def print_list(l):
    for i in l:
        print(i)


def vsm(entity):
    r = json.dumps(entity, ensure_ascii=False)
    vec_list = entity[1]
    tag_list = entity[0][1]
    vec_tag_list = filter(lambda x: type(x) is not types.FloatType, flatten(vec_list))
    # print_list(vec_tag_list)

    union_list = list(vec_tag_list)

    for i in tag_list:
        add(i, union_list)

    # print_list(union_list)

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

    # print(len(union_list))
    # print(len(tag_vector_list))
    # print(len(vec_vector_list))

    cell = 0
    floor1 = 0
    floor2 = 0
    floor = 1
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
    # rw.writerow(s.encode("utf-8"))
    # fo = open(filename, "a+")
    # fo.write(s.encode("utf-8"))
    # fo.write("\n")
    # fo.close()


def flatten(vec_list):
    return [y for x in vec_list for y in x]


tagRdd = sc.textFile('/vagrant/data/data.txt').map(lambda x: (json.loads(x)['id'], json.loads(x)['tags']));
vecRdd = sc.textFile('/vagrant/word/data.txt').map(lambda x: (json.loads(x)));

rdd = tagRdd.zip(vecRdd)
csvFile = file('vsm.csv', 'wb')
writer = csv.writer(csvFile)
writer.writerow(['percent'])

result = rdd.map(vsm).collect()
print(type(result))
print_list(result)

writer.writerows(result)

csvFile.close()