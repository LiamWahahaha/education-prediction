import sys
import csv
from pprint import pprint
import json
from pyspark import SparkContext
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="specify_your_app_name_here")
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
    state, county = parse_location(line['location'])
    return ({
        'user_id': line['user_id'],
        'location': line['location'],
        'state': state,
        'county': county,
        'time_zone': line['time_zone'],
        'text': line['text'],
        'created_at': line['created_at']
    })


twitter_w_proper_area = twitter_en_user_w_loc.map(add_state_county_features)


def geo_location(line):
    location = line['location'].split(',')
    try:
        latitude = float(location[0])
        longitude = float(location[1])
        address = geolocator.reverse('{},{}'.format(latitude,
                                                    longitude)).address
        if address[-3:] == 'USA':
            return (int(address.split(',')[-2]),
                    (address, line['user_id'], line['text'],
                     line['created_at']))
        else:
            return (None, None)
    except (KeyError, ValueError, IndexError):
        latitude = None
        longitude = None
        return (None, None)
    except:
        # query time out
        return (None, None)


# twitter_parsed_by_geo = twitter_w_proper_area.filter(lambda line: line['state'] == None).map(geo_location)
twitter_parsed_by_geo = twitter_w_proper_area.map(
    geo_location).filter(lambda line: line[0] is not None)

raw_zipcode2fips_data = sc.textFile(
    'ZIP-COUNTY-FIPS_2018-03.csv').mapPartitions(lambda line: csv.reader(line))
zipcode2fips_header = raw_zipcode2fips_data.first()
zipcode2fips_data = raw_zipcode2fips_data.filter(
    lambda line: line != zipcode2fips_header).map(lambda line: (int(line[0]),
                                                                int(line[1])))

intermediate = twitter_parsed_by_geo.join(zipcode2fips_data)


def transfer2intermediate(line):
    ((address, user_id, text, created_at), fips) = line[1]
    return fips, user_id, text, created_at


def to_csv(line):
    return ','.join(str(data) for data in line)


lines = intermediate.map(transfer2intermediate).map(to_csv)
lines.saveAsTextFile('./twitter/2011-09-intermediate.csv')
print('****************************')
print('****************************')
print('total records:', raw_twitter_data.count())
print('valid records:', intermediate.count())
print('****************************')
print('****************************')