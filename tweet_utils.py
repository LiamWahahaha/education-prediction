''''
Liam Wang: 111407491
Oswaldo Crespo: 107700568
Varun Goel: 109991128
Ziang Wang: 112077534
'''
'''
A utility file that converts raw tweet data to the format desired by us. The tweet body is stripped of stop words.
The tweet's location is first checked via the user provided location to see if it maps to a county that we know of.
If not, we check the coordinates. If yes, we map the coordinates to a zipcode to check which county the tweet belongs to.
'''

import re
from uszipcode import Zipcode
from uszipcode import SearchEngine
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import defaultdict
from emoji import UNICODE_EMOJI


def remove_trailing_chars(string):
    '''
    We observed cases where people reported their location to be Miamiiiiii instead of Miami. This helps catch such sitations
    '''
    if not string:
        return string
    last_idx = len(string) - 1
    last_char = string[last_idx]

    while last_idx and string[last_idx - 1] == last_char:
        last_idx -= 1

    return string[:last_idx + 1]


def map_tweet_to_location(tweet_info, fips_dict, county_dict, zipcode_dict):
    '''
    For a given tweet, tries to map the tweet to a known location
    If it's from a know location, the tweet is assigned the FIP and the body (text) is cleaned for processing
    '''
    county = tweet_info['county']
    state = tweet_info['state']
    coordinates = tweet_info['coordinates']
    #tweet_info['education_stats'] = None
    tweet_info['fips'] = None

    if county != None and county in county_dict:
        #tweet_info['education_stats'] = county_dict[county]
        tweet_info['fips'] = fip = county_dict[county][0]
    elif state != None and state in county_dict:
        #tweet_info['education_stats'] = county_dict[state]
        tweet_info['fips'] = fip = county_dict[state][0]
    elif coordinates != None and len(coordinates) != 0:
        lat = coordinates[0]
        long = coordinates[1]
        zipcode_search = SearchEngine(simple_zipcode=False)
        result = zipcode_search.by_coordinates(lat, long, radius=10, returns=1)

        if len(result) != 0:
            place = result[0]
            try:
                fip = zipcode_dict[place.zipcode]
                if fip in fips_dict:
                    #tweet_info['fips'] = fip
                    tweet_info['education_stats'] = fips_dict[fip]
            except KeyError:
                print('Cannot map this zipcode {} to fips'.format(
                    place.zipcode))

    #clean the tweet body by getting rid of stop words
    if tweet_info['fips'] != None:
        tweet_body = tweet_info['text']
        tweet_info['tweet_length_unfiltered'] = len(tweet_body)
        stop_words = set(stopwords.words('english'))
        stop_words.add(' ')

        tweet_body_tokens = tweet_body.split()
        filtered_sentence = [
            w for w in tweet_body_tokens if not w in stop_words
        ]

        tweet_info['text'] = ' '.join(filtered_sentence)

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
    return line['user_id'] != None and\
           line['lang'] == 'en' and\
           (line['location'] != None or line['geo']!= None)


def is_url(text):
    return re.search('https?://+', text) is not None


def is_emoji(text):
    return any(map(lambda char: char in UNICODE_EMOJI, text))


def aggregate_personal_data(line, fips_dict):
    fip = line['fips']
    user = line['user_id']
    text = line['text']
    filtered_text = []
    emoji_count = 0
    url_count = 0
    education_info = fips_dict[fip]
    aggregate_record = dict()

    tweet_words = text.lower().split()

    for tweet_word in tweet_words:
        if is_url(tweet_word):
            url_count += 1
        elif is_emoji(tweet_word):
            emoji_count += 1
        else:
            filtered_text.append(tweet_word)

    aggregate_record['fip'] = fip
    aggregate_record['user_id'] = user
    aggregate_record['text'] = text
    aggregate_record['filtered_text'] = ' '.join(filtered_text)
    aggregate_record['url_count'] = url_count
    aggregate_record['emoji_count'] = emoji_count
    aggregate_record['education_level_1'] = education_info[-4]
    aggregate_record['education_level_2'] = education_info[-3]
    aggregate_record['education_level_3'] = education_info[-2]
    aggregate_record['education_level_4'] = education_info[-1]

    return aggregate_record


def aggregate_county_data(county_tweets_info, fips_dict):
    fip = county_tweets_info[0]
    education_info = fips_dict[fip]

    county_tweets = county_tweets_info[1]

    aggregate_record = dict()
    aggregate_record['fip'] = fip

    words_dict = defaultdict(float)
    tweet_count = 0
    emoji_count = 0
    url_count = 0

    for tweet_info in county_tweets:
        tweet_count += 1
        tweet_body = tweet_info['text']
        tweet_words = tweet_body.lower().split()

        for tweet_word in tweet_words:
            if is_url(tweet_word):
                url_count += 1
            elif is_emoji(tweet_word):
                emoji_count += 1
            else:
                words_dict[tweet_word] += 1

    total_word_freq = sum(words_dict.values())

    for word in words_dict:
        words_dict[word] /= total_word_freq

    aggregate_record['words'] = words_dict
    aggregate_record['tweet_count'] = tweet_count
    aggregate_record['url_count'] = url_count
    aggregate_record['emoji_count'] = emoji_count
    aggregate_record['education_info'] = education_info[-4:]

    return aggregate_record
