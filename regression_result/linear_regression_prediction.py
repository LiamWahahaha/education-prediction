''''
Liam Wang: 111407491
Oswaldo Crespo: 107700568
Varun Goel: 109991128
Ziang Wang: 112077534
'''

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


def transform_raw_to_person_level__modified(person_tweets_info):
    user_id = person_tweets_info[0]
#    education_info = fips_dict[fip]
    person_tweets = person_tweets_info[1]
    transformed_record = dict()
    transformed_record['user_id'] = user_id
    words_dict = defaultdict(float)
    tweet_count = len(person_tweets)
    emoji_count = 0
    url_count = 0
    for tweet_info in person_tweets:
        fip = tweet_info["fip"]
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
    sample = person_tweets[0]
    transformed_record['education_info'] = [sample["education_level_1"], sample["education_level_2"], sample["education_level_3"], sample["education_level_4"]]
    transformed_record['fip'] = fip
    return transformed_record

def raw2person(sc, argv, fileName):
    rdd = sc.textFile(fileName).map(json.loads)
    tweets_groupby_uids = rdd.groupBy(lambda x: x['user_id']).mapValues(list)
    testRdd = tweets_groupby_uids.map(lambda x: transform_raw_to_person_level__modified(x))
    return testRdd




def regResult2wordDict(df):
    m = df.values
    wordDict = {}
    # for index, row in df.iterrows():
    #     if row["word"] not in wordDict:
    #         wordDict[row["word"]] = [0.0, 0.0, 0.0, 0.0]
    #     wordDict[row["word"]][row["level"]] = row["slope"]
    # wordDict

    for row in m:
        [word, level, slope, pval, count] = list(row)
        if word not in wordDict:
            wordDict[word] = {"slope":[0.0] * 4, "pval":[0.0] * 4, "count":[0] * 4}

        wordDict[word]["slope"][level] = slope
        wordDict[word]["pval"][level] = pval
        wordDict[word]["count"][level] = count
    return wordDict


def classifyPerson(e, wordDictBC):
    wordFreq = e["words"]
    wordDict = wordDictBC.value
    [wordL, freqL] = list(zip(*(wordFreq.items())))
    wordL, freqL = list(wordL), np.array(freqL).reshape((1, -1))
    
    wightL = []
    for w in wordL:
        if w not in wordDict:
            wightL.append([0.0]*4)
        else:
            wightL.append(wordDict[w]["slope"])
    wightL = np.array(wightL)
    wightL
    pred = np.dot(freqL, wightL)[0]
    pred
    
    
#    discountL = np.array([1.0, 0, 0, 1.0])
#    pred = discountL * pred
    
    
    predictedLevel = np.argmax(pred)
    return {"predictedLevel":predictedLevel, "fip":e["fip"], "education_info":e["education_info"]}

def prediction2freq(groupByRes):
    fip, eL = groupByRes 
    eduInfo = np.array(eL[0]["education_info"]) / 100.0
    
    predL = [e["predictedLevel"] for e in eL]
    unique, counts = np.unique(predL, return_counts=True)
    countDict = dict(zip(unique, counts))
    countL = np.array([countDict.get(level, 0) for level in range(4)])
    predLevelL = countL / countL.sum()
    error = np.abs(predLevelL - eduInfo).mean()
    return (fip, eduInfo, predLevelL, error)
    

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext.getOrCreate()
    sc.stop()
    sc = SparkContext(conf=conf)
    
#    fileName = "./p-2018-10-08.json"
#    fileStr = fileName.strip("./").split(".")[0]
    fileName = "./p-2018-10-08.json"
    
    ############# gen wordDict
    csvFile = "./word_result_for_p-2018-10-08.csv"
    resultDf = pd.read_csv(csvFile)
    wordDictBC = sc.broadcast(regResult2wordDict(resultDf))
    ############
#    pprint("@appdafadsadfsle" in wordDictBC.value)
    
    personLevelRdd = raw2person(sc, sys.argv, fileName)
    # pprint(personLevelRdd.take(3))
    # transformed to person level rdd
    personLevelRdd = personLevelRdd.filter(lambda e: len(e["words"]) > 0)
    personLevelRdd = personLevelRdd.map(lambda e: classifyPerson(e, wordDictBC))
#    pprint(personLevelRdd.take(3))
    
    countyLevelRdd = personLevelRdd.groupBy(lambda e: e["fip"]).mapValues(list)
    countyLevelRdd = countyLevelRdd.map(prediction2freq)
    pprint(countyLevelRdd.take(50))
    result = countyLevelRdd.collect()
    pprint(np.array([e[-1] for e in result]).mean())
    
