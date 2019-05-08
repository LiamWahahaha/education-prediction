import sys
import csv
from pprint import pprint
import json
from pyspark import SparkContext, SparkConf
from uszipcode import Zipcode
from uszipcode import SearchEngine


def remove_trailing_chars(string):
    if not string:
        return string
    last_idx = len(string) - 1
    last_char = string[last_idx]

    while last_idx and string[last_idx - 1] == last_char:
        last_idx -= 1

    return string[:last_idx + 1]


def create_education_dicts(attainment_data_rdd_values):
    fips_dict = {}
    county_dict = {}

    for attainment_data_rdd_value in attainment_data_rdd_values:
        fips = attainment_data_rdd_value[0]
        county = attainment_data_rdd_value[2]
        county = county.replace('County', '').replace('Borough', '').replace('Municipio', '')\
        .replace('Municipality', '').replace('Census', '').replace('Area', '')\
        .lower().replace(' ', '')
        county = remove_trailing_chars(county)
        fips_dict[fips] = attainment_data_rdd_value[1:]
        county_dict[county] = attainment_data_rdd_value[0:2] + attainment_data_rdd_value[3:]
        #map state names as well
        if (int(fips) % 1000 == 0):
            state_name = attainment_data_rdd_value[1].lower()
            county_dict[state_name] = attainment_data_rdd_value[0:1] + attainment_data_rdd_value[2:]

    return fips_dict, county_dict


def create_zipcode_dict(zipcode_data_rdd_values):
    zipcode_dict = {}

    for zipcode_data_rdd_value in zipcode_data_rdd_values:
        zipcode = str(zipcode_data_rdd_value[0])
        fips = str(zipcode_data_rdd_value[1])
        if fips[0] == '0':
            fips = fips[1:]
        zipcode_dict[zipcode] = fips

    return zipcode_dict


def map_tweet_to_location(tweet_info, fips_dict, county_dict, zipcode_dict):
    county = tweet_info['county']
    state = tweet_info['state']
    coordinates = tweet_info['coordinates']
    tweet_info['education_stats'] = None

    if county in county_dict:
        tweet_info['education_stats'] = county_dict[county]
    elif state in county_dict:
        tweet_info['education_stats'] = county_dict[state]
    elif coordinates:
        lat = coordinates[0]
        long = coordinates[1]
        zipcode_search = SearchEngine(simple_zipcode=False)
        result = zipcode_search.by_coordinates(lat, long, radius=10, returns=1)
        if len(result) != 0:
            place = result[0]
            try:
                fip = zipcode_dict[place.zipcode]
                if fip in fips_dict:
                    tweet_info['education_stats'] = fips_dict[fip]
            except KeyError:
                pass

    return tweet_info


def parse_location(location):
    if not location:
        return None, None

    location_info = location.lower().split(',')
    county = remove_trailing_chars(location_info[0])
    county = county.replace('County', '').replace('Borough', '').replace('Municipio', '')\
        .replace('Municipality', '').replace('Census', '').replace('Area', '')\
        .lower().replace(' ', '')
    state = None

    if len(location_info) > 1:
        state = location_info[1].replace(' ', '')

    if state:
        state = remove_trailing_chars(state)

    return state, county


def parse_geo(geo):
    if not geo or not geo['coordinates']:
        return None
    return geo['coordinates']


def add_state_county_features(line):
    # assume that if user provide county info, it will be only 'county', or 'county, states'
    line['state'], line['county'] = parse_location(line['location'])
    line['coordinates'] = parse_geo(line['geo'])

    return ({
        'user_id': line['user_id'],
        'location': line['location'],
        'state': line['state'],
        'county': line['county'],
        'time_zone': line['time_zone'],
        'text': line['text'],
        'created_at': line['created_at'],
        'coordinates': line['coordinates']
    })


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
            'created_at': line['created_at'],
            'geo': line['geo']
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
            'created_at': None,
            'geo': None
        })


def english_user_with_location(line):
    return line['user_id'] != None and (line['location'] != None or line['geo'] != None) and line['lang'] == 'en'


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

    pprint(twitter_w_proper_area.take(5))

    #get all the data for education stats; mapped by FIPS and County name
    attainment_data_rdd = sc.textFile(
        attainment_data).mapPartitions(lambda line: csv.reader(line))
    attainment_data_header = attainment_data_rdd.first()
    attainment_data_rdd = attainment_data_rdd.filter(lambda line: line != attainment_data_header)
    attainment_data_rdd = sc.broadcast(attainment_data_rdd.collect())
    attainment_data_rdd_values = attainment_data_rdd.value
    #get info by FIPS and county name
    fips_dict, county_dict = create_education_dicts(attainment_data_rdd_values)
    #pprint(fips_dict)

    #map zipcodes to fips
    zipcode_data_rdd = sc.textFile(
        zipcode_data).mapPartitions(lambda line: csv.reader(line))
    zipcode_data_header = zipcode_data_rdd.first()
    zipcode_data_rdd = zipcode_data_rdd.filter(lambda line: line != zipcode_data_header)
    zipcode_data_rdd = sc.broadcast(zipcode_data_rdd.collect())
    zipcode_data_rdd_values = zipcode_data_rdd.value
    #get info by zipcode
    zipcode_dict = create_zipcode_dict(zipcode_data_rdd_values)
    #pprint(zipcode_dict)

    twitter_w_proper_area = twitter_w_proper_area.map(
        lambda x: map_tweet_to_location(x, fips_dict, county_dict, zipcode_dict
                                        ))
    pprint(twitter_w_proper_area.take(1))
    pprint(twitter_w_proper_area.count())

    twitter_w_education_stats = twitter_w_proper_area.filter(lambda x: x['education_stats'] != None)
    pprint(twitter_w_education_stats.take(5))
    pprint(twitter_w_education_stats.count())
