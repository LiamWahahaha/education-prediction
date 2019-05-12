# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 17:02:57 2019

@author: Username
"""

from pyspark import SparkConf
from pyspark import SparkContext

import numpy as np
from pprint import pprint
from scipy import stats                                     
import csv
import json
import pandas as pd
from numpy import linalg
import sys
import re


from collections import defaultdict

#import uszipcode


def transform_personal_to_county__modified(county_tweets_info):
    fip = county_tweets_info[0]
#    education_info = fips_dict[fip]
    county_tweets = county_tweets_info[1]
    transformed_record = dict()
    transformed_record['fip'] = fip
    words_dict = defaultdict(float)
    tweet_count = len(county_tweets)
    emoji_count = 0
    url_count = 0
    for tweet_info in county_tweets:
        tweet_body = tweet_info['filtered_text']
        tweet_words = tweet_body.split()
        # how to measure the emoji and url?
        emoji_count += tweet_info['emoji_count']
        url_count += tweet_info['url_count']
        for tweet_word in tweet_words:
            words_dict[tweet_word] += 1
    total_word_freq = sum(words_dict.values())
    for word in words_dict:
        words_dict[word] /= total_word_freq
    transformed_record['words'] = words_dict
    transformed_record['tweet_count'] = tweet_count
    transformed_record['avg_url_count'] = url_count / tweet_count
    transformed_record['avg_emoji_count'] = emoji_count / tweet_count
    sample = county_tweets[0]
    transformed_record['education_info'] = [sample["education_level_1"], sample["education_level_2"], sample["education_level_3"], sample["education_level_4"]]
    return transformed_record

def person2county(sc, argv):
    rdd = sc.textFile("p-2018-10-08.json").map(json.loads)
    tweets_groupby_fips = rdd.groupBy(lambda x: x['fip']).mapValues(list)
    testRdd = tweets_groupby_fips.map(lambda x: transform_personal_to_county__modified(x))
    return testRdd

def runTests(sc, argv, level, testRdd, fileStr):    
#    testFile = './intermediate_01.csv'
    # contyFile = argv[2]#"./countyoutcomes.csv"
    
#    testRdd = sc.textFile(testFile).mapPartitions(lambda line: csv.reader(line))
    
    STOP_WORD_PATH = "./StopWords.txt"
    stopWordL = sc.textFile(STOP_WORD_PATH).collect()
    stopWordSet = set(stopWordL)
    stopWordSetBC = sc.broadcast(stopWordSet)
    
    LEVEL = level
    WORD_COUNT_THRESHOLD = 3
    
    
#    pprint(testRdd.take(10))


    
#    
#    
#
#    testRdd = sc.textFile('./intermediate_01.csv').mapPartitions(lambda line: csv.reader(line))
#    pprint(testRdd.take(5))  #uncomment to see data    
    
    
    ##########################################################
#    testRdd = sc.textFile("./county_level_info.json ").map(lambda x: json.loads(x))
#    pprint(testRdd.take(5))  #uncomment to see data    
#    
    def flatMapFunc(e):
        fip = int(e["fip"])
        education_info = float(e["education_info"][LEVEL]) / 100.0
        words = e["words"]
        return [(fip, key, value, education_info) for key, value in words.items()]
    testRdd = testRdd.flatMap(flatMapFunc)
    testRdd = testRdd.filter(lambda e: e[1].replace("’", "'") not in stopWordSetBC.value)
#    pprint(testRdd.take(10))
    
    
    testRdd = testRdd.map(lambda e: (e[0], re.sub('^[^a-zA-z0-9@]*|[^a-zA-Z0-9@]*$','',e[1]), e[2], e[3]))\
                    .filter(lambda s: s != "")
                    
                    
                    
    testRdd = testRdd.filter(lambda e: e[1] != "NaN")
#    pprint(testRdd.take(10))  
    

#    testRdd = testRdd.filter(lambda e: e[0] != "group_id") # filter out the first line
#    
#    # pprint("readed")
    testRdd = testRdd.groupBy(lambda e: e[1])
#    
#    # pprint("grouped")
#    
#    def toList(iterable):
#        r = []
#        for e in iterable:
#            r.append(e[2:4])
#        return (r[0], r[1], ))
    
    def toList(e):
        k, iterable = e
        r = []
        for e in iterable:
            r.append([e[2], e[3]])
        return (k, np.array(r))
#    
    testRdd = testRdd.map(toList)
#    pprint(testRdd.take(10))  
#    testRdd = testRdd
#    
#    # pprint("listed")
#    
#    
#    testRdd = testRdd.mapValues(toList)
#    
#    def toDict(tuple_):
#        [key, val] = tuple_
#        list_ = np.array([
#                    [
#                        float(e[3]),  
#                        contyHeartDict_Bc.value[e[0]],
#                        contyIncomeDict_Bc.value[e[0]]
#                    ]
#                    for e in val if (e[0] in contyHeartDict_Bc.value)
#                ])
#        return (key, list_)
#    testRdd = testRdd.map(toDict)
    testRdd = testRdd.filter(lambda e: e[1].shape[0] > WORD_COUNT_THRESHOLD)
#    testRdd = testRdd.map(lambda e: e[1].shape[0])
#    pprint(testRdd.take(100))
##    pprint(testRdd.take(10))
##    # pprint("dictioned")
##    
##    
    def linearReg1(tuple_):
        key, val = tuple_
        x, y = val[:, 0],  val[:, 1]
        norm_x = (x - x.mean()) / x.std()
        norm_y = (y - y.mean()) / y.std()
        # calc the linear regression
        # return (key, (x, x.mean(), x - x.mean(), x.std()), y, norm_x, norm_y)
        slope, intercept, r_value, p_value, std_err = stats.linregress(norm_x, norm_y)
        return (key, LEVEL, slope, p_value, val.shape[0])
#    
#    
    testRdd1 = testRdd.map(linearReg1)
#    testRdd2 = testRdd.map(linearReg2)
#    # pprint("regressioned")
    resultList1 = testRdd1.collect()
#    resultList2 = testRdd2.collect()
#    
    pprint(resultList1[:20])
##    pprint(resultList[-20:])
##    
##    pprint(testRdd.take(20))  #uncomment to see data
    
    fileName = "./word_result_{}_for_{}.csv".format(LEVEL, fileStr)
    df = pd.DataFrame(resultList1)
    df.to_csv(fileName, index=False)
#    
#    df = pd.DataFrame(resultList2)
#    df.to_csv("./word_result_2.csv")
#    
    
    
    return 

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext.getOrCreate()
    sc.stop()
    sc = SparkContext(conf=conf)
    
    fileName = "./p-2018-10-08.json"
    fileStr = fileName.strip("./").split(".")[0]
    outputStr = "word_result_for_{}.csv".format(fileStr)
    
    testRdd = person2county(sc, sys.argv)
#    runTests(sc, sys.argv, level)
    dfList = []
    for level in range(4):
        runTests(sc, sys.argv, level, testRdd, fileStr)
        df = pd.read_csv("./word_result_{}_for_{}.csv".format(level, fileStr))
        dfList.append(df)
        
    df = pd.concat(dfList, ignore_index=True)
    df.columns = "word level slope pval count".split(" ")
    df.to_csv(outputStr, index=False)
    
        
        
        
        
        
        
        

    df = pd.read_csv(outputStr)
    df.columns = "word level slope pval count".split(" ")
    # df.pval = df.pval * df.shape[0]
    # df = df[df.pval < 0.2]
    df;
    
    df = df[(df.level == 3) & (df["count"] > 15) & (df.pval < 0.2)]
    
    TOP_N = 20
    print("The top 20 word positively correlated:")
    df = df.sort_values(by="slope", ascending=False)
    df = df.reset_index(drop=True)
    # df.index = df.index + 1
    df.index = df.index + 1
    print(df.head(TOP_N))
    print("\n\n")
    
    print("The top 20 word negatively correlated:")
    df = df.sort_values(by="slope", ascending=True)
    df = df.reset_index(drop=True)
    # df.index = df.index + 1
    df.index = df.index + 1
    print(df.head(TOP_N))
    print("\n\n")
    
