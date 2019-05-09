import sys
import csv
from pprint import pprint
import json
from pyspark import SparkContext, SparkConf
from tweet_utils import *
from education_location_utils import *

def aggregate_county_data(county_tweets_info, fips_dict):
    fip = county_tweets_info[0]
    education_info = fips_dict[fip]

    county_tweets = county_tweets_info[1]

    aggregate_record = {}
    aggregate_record['fip'] = fip
    
    words_dict = {}
    tweet_count = 0

    for tweet_info in county_tweets:
        tweet_count += 1
        tweet_body = tweet_info['text']
        tweet_words = tweet_body.split()
        
        for tweet_word in tweet_words:
            if tweet_word not in words_dict:
                words_dict[tweet_word] = 1
            words_dict[tweet_word] += 1

    total_word_freq = sum(words_dict.values())

    for word in words_dict:
        words_dict[word] /= total_word_freq


    aggregate_record['words'] = words_dict
    aggregate_record['tweet_count'] = tweet_count
    aggregate_record['education_info'] = education_info[-4:]

    return aggregate_record


if __name__ == "__main__":
    sc = SparkContext(conf=SparkConf())
    sc.setLogLevel("ERROR")

    attainment_data = 'education_attainment.csv'
    zipcode_data = 'ZIP-COUNTY-FIPS_2018-03.csv'

    f = open('twitter/twitter_file_list.txt')
    twitter_file_list = f.readline()
    raw_twitter_data = sc.textFile(twitter_file_list)
    raw_twitter_json = raw_twitter_data.map(json.loads)

    # pprint(raw_twitter_json)
    filtered_twitter = raw_twitter_json.map(filter_twitter_raw_data)

    twitter_en_user_w_loc = filtered_twitter.filter(english_user_with_location)

    twitter_w_proper_area = twitter_en_user_w_loc.map(add_state_county_features)


    #get all the data for education stats; mapped by FIPS and County name
    attainment_data_rdd = sc.textFile(attainment_data).mapPartitions(lambda line: csv.reader(line))
    attainment_data_header = attainment_data_rdd.first()
    attainment_data_rdd = attainment_data_rdd.filter(lambda line: line != attainment_data_header)
    attainment_data_rdd = sc.broadcast(attainment_data_rdd.collect())
    attainment_data_rdd_values = attainment_data_rdd.value
    #get info by FIPS and county name
    fips_dict, county_dict = create_education_dicts(attainment_data_rdd_values)
    #print(county_dict)
    
    #pprint(fips_dict)


    #map zipcodes to fips
    zipcode_data_rdd = sc.textFile(zipcode_data).mapPartitions(lambda line: csv.reader(line))
    zipcode_data_header = zipcode_data_rdd.first()
    zipcode_data_rdd = zipcode_data_rdd.filter(lambda line: line != zipcode_data_header)
    zipcode_data_rdd = sc.broadcast(zipcode_data_rdd.collect())
    zipcode_data_rdd_values = zipcode_data_rdd.value
    #get info by zipcode
    zipcode_dict = create_zipcode_dict(zipcode_data_rdd_values)
    #pprint(zipcode_dict)

    
    twitter_w_proper_area = twitter_w_proper_area.map(lambda x : map_tweet_to_location(x, fips_dict, county_dict, zipcode_dict))
    # pprint(twitter_w_proper_area.take(1))
    # pprint(twitter_w_proper_area.count())

    tweets_w_fips = twitter_w_proper_area.filter(lambda x : x['fips'] != None)
    #tweets_w_fips = tweets_w_fips.groupBy(lambda x : x['fips'])
    tweets_w_fips = tweets_w_fips.groupBy(lambda x : x['fips']).mapValues(list)
    pprint(tweets_w_fips.take(25))
    pprint(tweets_w_fips.count())

    #aggregate info for each county (words used, education levels, etc)
    county_level_info = tweets_w_fips.map(lambda x : aggregate_county_data(x, fips_dict))
    pprint(county_level_info.take(25))
    pprint(county_level_info.count())