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




def runTests(sc, argv, level):    
#    testFile = './intermediate_01.csv'
    # contyFile = argv[2]#"./countyoutcomes.csv"
    
#    testRdd = sc.textFile(testFile).mapPartitions(lambda line: csv.reader(line))
    
    STOP_WORD_PATH = "./StopWords.txt"
    stopWordL = sc.textFile(STOP_WORD_PATH).collect()
    stopWordSet = set(stopWordL)
    stopWordSetBC = sc.broadcast(stopWordSet)
    
    LEVEL = level
    WORD_COUNT_THRESHOLD = 3
    
    
#    
#    
#
#    testRdd = sc.textFile('./intermediate_01.csv').mapPartitions(lambda line: csv.reader(line))
#    pprint(testRdd.take(5))  #uncomment to see data    
    
    testRdd = sc.textFile("./county_level_info.json ").map(lambda x: json.loads(x))
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
    testRdd = testRdd.map(lambda e: (e[0], re.sub('^[^a-zA-z0-9]*|[^a-zA-Z0-9]*$','',e[1]), e[2], e[3]))\
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
    
    fileName = "./word_result_{}.csv".format(LEVEL)
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
    for level in range(4):
        runTests(sc, sys.argv, level)
        
        
        dfList = []
        for i in range(4):
            df = pd.read_csv("./word_result_{}.csv".format(i))
            dfList.append(df)
        df = pd.concat(dfList, ignore_index=True)
        df.columns = "word level slope pval count".split(" ")
        df.to_csv("word_result.csv", index=False)
        
        
        
        
        
        
        
        

        df = pd.read_csv("./word_result.csv")
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
    
