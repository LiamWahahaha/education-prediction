import sys
import csv
from pprint import pprint
import json
from pyspark import SparkContext

sc = SparkContext()
sc.setLogLevel("ERROR")
f = open('twitter/twitter_file_list.txt')
twitter_file_list = f.readline()
raw_twitter_data = sc.textFile(twitter_file_list)
raw_twitter_json = raw_twitter_data.map(json.loads)

# TODO: combine twitter data with education attainment data
#raw_education_data = sc.textFile(
#    'education_attainment.csv').mapPartitions(lambda line: csv.reader(line))


def filter_twitter_raw_data(line):
    try:
        return ({
            'user_id': line['user']['id'],
            'lang': line['user']['lang'],
            'location': line['user']['location'],
            'state': None,
            'county': None,
            'time_zone': line['user']['time_zone'],
            'text': line['text'],
            'created_at': line['created_at']
        })
    except (KeyError, AttributeError):
        return ({
            'user_id': None,
            'lang': None,
            'location': None,
            'state': None,
            'county': None,
            'time_zone': None,
            'text': None,
            'created_at': None
        })


filtered_twitter = raw_twitter_json.map(filter_twitter_raw_data)


def english_user_with_location(line):
    return line['user_id'] and line['location'] and line['lang'] == 'en'


twitter_en_user_w_loc = filtered_twitter.filter(english_user_with_location)


# TODO: construct a more sophisticated rule to parse location info
def parse_location(location):
    location_info = location.lower().split(',')
    county = location_info[0]
    state = None

    if len(location_info) > 1:
        state = location_info[1].replace(' ', '')

    return state, county


def remove_trailing_chars(string):
    last_idx = len(string) - 1
    last_char = string[last_idx]

    while last_idx and string[last_idx - 1] == last_char:
        last_idx -= 1

    return string[:last_idx + 1]


def add_state_county_features(line):
    # assume that if user provide county info, it will be only 'county', or 'county, states'
    line['state'], line['county'] = parse_location(line['location'])

    return ({
        'user_id': line['user_id'],
        'location': line['location'],
        'state': line['state'],
        'county': line['county'],
        'time_zone': line['time_zone'],
        'text': line['text'],
        'created_at': line['created_at']
    })


twitter_w_proper_area = twitter_en_user_w_loc.map(add_state_county_features)

pprint(twitter_w_proper_area.take(5))