import sys
import csv
from collections import defaultdict
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


def str2float(line):
    return line[:3] + list(map(float, line[3:]))


def preprocess_attainment_data(sc, attainment_data_file):
    rdd = sc.textFile(attainment_data_file)\
            .mapPartitions(lambda line: csv.reader(line))
    header = rdd.first()
    attainment_data_rdd = rdd.filter(lambda line: line != header).map(str2float)
    return attainment_data_rdd


def map_edcation_level(edcation_level):
    return ''.join(['{:5f}'.format(str(val)) for val in education_level[-4:]])


def education_statistic(attainment_data_rdd_values, top_nth=10):
    education_lv = defaultdict(list)
    for idx in range(4):
        education_lv[idx] = sorted(
            attainment_data_rdd_values,
            key=lambda row: row[~idx],
            reverse=True)
        print('education level{} top {} counties:'.format(~idx, top_nth))
        for nth in range(top_nth):
            print('{:>9s}{:^6s}{:<29s}{:>7.2f} {:>7.2f} {:>7.2f} {:>7.2f}'\
                .format(
                    education_lv[idx][nth][0], education_lv[idx][nth][1],
                    education_lv[idx][nth][2], education_lv[idx][nth][-4],
                    education_lv[idx][nth][-3], education_lv[idx][nth][-2],
                    education_lv[idx][nth][-1]))
        print()
    return education_lv


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
        county_dict[county] = attainment_data_rdd_value[
            0:2] + attainment_data_rdd_value[3:]
        #map state names as well
        if (int(fips) % 1000 == 0):
            state_name = attainment_data_rdd_value[1].lower()
            county_dict[state_name] = attainment_data_rdd_value[
                0:1] + attainment_data_rdd_value[2:]

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