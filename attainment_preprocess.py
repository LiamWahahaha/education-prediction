import csv
import json
import pyspark
from pprint import pprint

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

raw_education_data = sc.textFile(
    'education_attainment.csv').mapPartitions(lambda line: csv.reader(line))
education_header = raw_education_data.first()
education_table = {key: idx for idx, key in enumerate(education_header)}
education_data = raw_education_data.filter(lambda line: line !=
                                           education_header)


def remove_trailing_chars(string):
    last_idx = len(string) - 1
    last_char = string[last_idx]
    while last_idx and string[last_idx - 1] == last_char:
        last_idx -= 1
    return string[:last_idx + 1]


def education_data_preprocess(line):
    parsed_data = [None] * len(education_header)
    for idx, key in enumerate(education_header):
        if key == 'FIPS':
            parsed_data[idx] = int(line[idx])
        elif key == 'State':
            parsed_data[idx] = line[idx].lower()
        elif key == 'Area':
            parsed_data[idx] = line[idx].replace('County', '').lower().replace(
                ' ', '')
        else:
            try:
                parsed_data[idx] = float(line[idx])
            except ValueError:
                parsed_data[idx] = 0.0
    return tuple(parsed_data)


valid_education = education_data.map(
    education_data_preprocess).filter(lambda line: line[0] % 1000)

print(valid_education.count())
print(valid_education.take(5))
