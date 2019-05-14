import sys
import re
import json
import csv
from collections import defaultdict
from random import random
from pyspark import SparkContext, SparkConf

import pandas as pd
import numpy as np
from numpy import linalg
from scipy import stats

from pprint import pprint

##################################
# Control variables
##################################
INTERMEDIATE_FILE = './twitter/p-2013.json'
STOP_WORD_FILE = './StopWords.txt'
WORD_COUNT_THRESHOLD = 3


def decode(line):
    """
    load intermediate file into json format, return a fake record
    if the json.loads failed
    """
    try:
        return json.loads(line)
    except:
        fake_record = {}
        fake_record['fip'] = 0
        fake_record['user_id'] = 0
        fake_record['text'] = ''
        fake_record['filtered_text'] = ''
        fake_record['url_count'] = 0
        fake_record['emoji_count'] = 0
        fake_record['education_level_1'] = 0
        fake_record['education_level_2'] = 0
        fake_record['education_level_3'] = 0
        fake_record['education_level_4'] = 0
        return fake_record


###########################################################
# functions for separating data into train set and test set
###########################################################
def assign_train_test_label(line):
    """
    Separate data into two part:
    True: train set (randomly select ~70% of the records)
    False: test set (randomly select ~30% of the records)
    """
    is_train = random() < 0.7
    return (is_train, line)


def assign_train_validate_test_lable(line):
    """
    Separate data into three part:
    0: train set      (randomly select ~70% of the records)
    1: validation set (randomly select ~20% of the records)
    2: test set       (randomly select ~10% of the records)
    """
    random_val = random()
    label = 0
    if 0.7 <= random_val < 0.9:
        label = 1
    elif random_val >= 0.9:
        label = 2
    return (label, line)


##########################################
# functions for learning word's importancy
##########################################
def transform_personal_to_county_level(county_tweets_info):
    """
    generate county level info includes the following features:
    'fip':             one's location info
    'words':           a dictionary of used word
    'tweet_count':     the total number of tweets posted in a county
    'avg_url_count':   the average number of urls posted in a county
    'avg_emoji_count': the average number of emojis posted in a county
    'education_info':  a county's education indicator
    """
    fip = county_tweets_info[0]
    county_tweets = county_tweets_info[1]
    county_level_record = dict()
    county_level_record['fip'] = fip
    words_dict = defaultdict(float)
    tweet_count = len(county_tweets)
    emoji_count = 0
    url_count = 0
    for tweet_info in county_tweets:
        tweet_body = tweet_info['filtered_text']
        tweet_words = tweet_body.split()
        emoji_count += tweet_info['emoji_count']
        url_count += tweet_info['url_count']
        for tweet_word in tweet_words:
            words_dict[tweet_word] += 1
    total_word_freq = sum(words_dict.values())
    for word in words_dict:
        words_dict[word] /= total_word_freq
    county_level_record['words'] = words_dict
    county_level_record['tweet_count'] = tweet_count
    county_level_record['avg_url_count'] = url_count / tweet_count
    county_level_record['avg_emoji_count'] = emoji_count / tweet_count
    sample = county_tweets[0]
    county_level_record['education_info'] = [
        sample["education_level_1"], sample["education_level_2"],
        sample["education_level_3"], sample["education_level_4"]
    ]
    return county_level_record


def person2county(train_rdd):
    tweets_groupby_fips = train_rdd.groupBy(lambda x: x['fip']).mapValues(list)
    return tweets_groupby_fips.map(lambda x:
                                   transform_personal_to_county_level(x))


def learn_word_importancy(sc, train_rdd):
    """
    return a rdd with 
    key: word
    value: a dictionary,
        {
            'slope': [],
            'pval': [],
            'count': []
        }, each list contains 4 values which map to education level
    """

    def flatMap_func(e, level):
        fip = int(e['fip'])
        education_info = float(e["education_info"][level]) / 100.0
        words = e["words"]
        return [(fip, key, value, education_info)
                for key, value in words.items()]

    def to_list(e):
        try:
            k, iterable = e
            r = []
            for e in iterable:
                r.append([e[2], e[3]])
            return (k, np.array(r))
        except:
            return ('ErrorData', np.array([]))

    def linear_regression(tuple_, level, bonferroni_correction):
        key, val = tuple_
        x, y = val[:, 0], val[:, 1]
        norm_x = (x - x.mean()) / x.std()
        norm_y = (y - y.mean()) / y.std()
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            norm_x, norm_y)
        corrected_p_value = p_value * bonferroni_correction
        return (key, (level, slope, corrected_p_value, val.shape[0]))

    def word_dictionary(word_importancy_rdd):
        word, level_list = word_importancy_rdd
        word_dict = {'slope': [0.0] * 4, 'pval': [0.0] * 4, 'count': [0] * 4}
        for row in level_list:
            level, slope, pval, count = row
            word_dict['slope'][level] = slop
            word_dict['pval'][level] = pval
            word_dict['count'][level] = count
        return (word, word_dict)

    stop_word_list = sc.textFile(STOP_WORD_FILE).collect()
    stop_word_set = set(stop_word_list)
    stop_word_set_bc = sc.broadcast(stop_word_set)
    bonferroni_correction = train_rdd.flatMap(lambda e: flatMap_func(e, 0))\
                                     .filter(lambda e: e[1].replace("’", "'")
                                         not in stop_word_set_bc.value)\
                                     .map(to_list).filter(lambda e: e[1].shape[0] >
                                         WORD_COUNT_THRESHOLD)\
                                     .count()
    word_importancy_rdds = []
    for level in range(4):
        tmp_rdd = train_rdd.flatMap(lambda e: flatMap_func(e, level))\
                           .filter(lambda e: e[1].replace("’", "'")
                                         not in stop_word_set_bc.value)\
                           .map(to_list).filter(lambda e: e[1].shape[0] >
                                         WORD_COUNT_THRESHOLD)\
                           .map(lambda tuple_: linear_regression(tuple_, level, bonferroni_correction))
        word_importancy_rdds.append(tmp_rdd)
    union_word_importancy_rdd = sc.union(word_importancy_rdds)
    word_importancy_of_each_level_rdd = union_word_importancy_rdd.groupByKey()\
                                                                 .map(word_dictionary)
    return word_importancy_of_each_level_rdd


#################################################
# functions for predicting person education level
#################################################
def transform_raw_to_person_level(person_tweets_info):
    """
    generate person level info includes the following features:
    'fip':             one's location info
    'words':           a dictionary of used word
    'tweet_count':     the total number of tweets does this person posted
    'avg_url_count':   the average number of urls does this person mention in tweets
    'avg_emoji_count': the average number of emojis does this person mention in tweets
    'education_info':  a person's education indicator
    """
    user_id = person_tweets_info[0]
    person_tweets = person_tweets_info[1]
    person_level_record = dict()
    person_level_record['user_id'] = user_id
    words_dict = defaultdict(float)
    tweet_count = len(person_tweets)
    emoji_count = 0
    url_count = 0
    for tweet_info in person_tweets:
        fip = tweet_info["fip"]
        tweet_body = tweet_info['filtered_text']
        tweet_words = tweet_body.split()
        emoji_count += tweet_info['emoji_count']
        url_count += tweet_info['url_count']
        for tweet_word in tweet_words:
            words_dict[tweet_word] += 1
    total_word_freq = sum(words_dict.values())
    for word in words_dict:
        words_dict[word] /= total_word_freq
    person_level_record['fip'] = fip
    person_level_record['words'] = words_dict
    person_level_record['tweet_count'] = tweet_count
    person_level_record['avg_url_count'] = url_count / tweet_count
    person_level_record['avg_emoji_count'] = emoji_count / tweet_count
    sample = person_tweets[0]
    person_level_record['education_info'] = [
        sample["education_level_1"], sample["education_level_2"],
        sample["education_level_3"], sample["education_level_4"]
    ]
    return person_level_record


def raw2person(test_rdd):
    tweets_groupby_uids = test_rdd.groupBy(lambda x: x['user_id']).mapValues(
        list)
    return tweets_groupby_uids.map(lambda x: transform_raw_to_person_level(x))


def classify_person(e, word_dict_bc):
    """
    predict one's education level by using the regression slop
    """
    word_freq = e["words"]
    word_dict = word_dict_bc.value
    [word_list, freq_list] = list(zip(*(word_freq.items())))
    word_list = list(word_list)
    freq_list = np.array(freq_list).reshape((1, -1))
    weight_list = []
    for w in word_list:
        if w not in word_dict:
            weight_list.append([0.0] * 4)
        else:
            weight_list.append(word_dict[w]["slope"])
    weight_list = np.array(weight_list)
    pred = np.dot(freq_list, weight_list)[0]
    predicted_level = np.argmax(pred)
    return {
        "predicted_level": predicted_level,
        "fip": e["fip"],
        "education_info": e["education_info"]
    }


def prediction2freq(group_by_res):
    fip, edu_list = group_by_res
    edu_info = np.array(edu_list[0]["education_info"]) / 100.0
    pred_list = [e["predicted_level"] for e in edu_list]
    unique, counts = np.unique(pred_list, return_counts=True)
    count_dict = dict(zip(unique, counts))
    count_list = np.array([count_dict.get(level, 0) for level in range(4)])
    pred_level_list = count_list / count_list.sum()
    error = np.abs(pred_level_list - edu_info).mean()
    return (fip, edu_info, pred_level_list, error)


if __name__ == "__main__":
    sc = SparkContext(conf=SparkConf())
    sc.setLogLevel('ERROR')

    #########################################################################
    # read intermediate json file and separate it into train set and test set
    #########################################################################
    intermediate_rdd = sc.textFile(INTERMEDIATE_FILE)
    train_test_label_rdd = intermediate_rdd.map(lambda x: decode(x))\
                                           .map(assign_train_test_label)
    train_test_label_rdd.persist()

    p_level_raw_train_rdd = train_test_label_rdd.filter(lambda row: row[0])\
                                                .map(lambda row: row[1])
    p_level_raw_test_rdd = train_test_label_rdd.filter(lambda row: not row[0])\
                                               .map(lambda row: row[1])

    #####################################################
    # use train set to learn the linear regression model
    #####################################################
    county_level_train_rdd = person2county(p_level_raw_train_rdd)
    word_importancy_rdds = learn_word_importancy(sc, county_level_train_rdd)
    word_importancy_list = word_importancy_rdds.collect()
    word_importancy_dict = {
        word: importancy
        for word, importancy in word_importancy_list
    }
    word_dict_bc = sc.broadcast(word_importancy_dict)

    #####################################################
    # use test set to predict
    #####################################################
    person_level_test_rdd = raw2person(p_level_raw_test_rdd)
    person_level_test_rdd = person_level_test_rdd.filter(lambda e: e["words"])
    person_level_test_rdd = person_level_test_rdd.map(
        lambda e: classify_person(e, word_dict_bc))

    county_level_rdd = person_level_test_rdd.groupBy(lambda e: e["fip"]
                                                     ).mapValues(list)
    county_level_rdd = county_level_rdd.map(prediction2freq)
    pprint(county_level_rdd.take(50))
    result = county_level_rdd.collect()
    pprint(np.array([e[-1] for e in result]).mean())