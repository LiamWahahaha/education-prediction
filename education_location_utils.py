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
        if(int(fips) % 1000 == 0):
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