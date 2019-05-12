import json
from random import random
from pyspark import SparkContext, SparkConf


def assign_train_test_label(line):
    is_train = random() < 0.7
    return (is_train, line)


def assign_train_validate_test_lable(line):
    random_val = random()
    label = 0
    if 0.7 <= random_val < 0.9:
        label = 1
    elif random_val >= 0.9:
        label = 2
    return (label, line)


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext.getOrCreate()
    sc.stop()
    sc = SparkContext(conf=conf)

    # fileName = './p-2013.json,./p-2014.json,./p-2015.json,./p-2016.json,./p2017.json'
    fileName = "twitter/part/p-2018-10-08.json"
    personal_rdd = sc.textFile(fileName)
    train_test_label_rdd = personal_rdd.map(
        json.loads).map(assign_train_test_label)
    train_test_label_rdd.persist()

    p_level_train_rdd = train_test_label_rdd.filter(lambda row: row[0]).map(
        lambda row: row[1])
    p_level_test_rdd = train_test_label_rdd.filter(lambda row: not row[0]).map(
        lambda row: row[1])

    print('length of original dataset:', personal_rdd.count())
    print('length of train set:', p_level_train_rdd.count())
    print('length of test set:', p_level_test_rdd.count())