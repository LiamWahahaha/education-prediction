import sys
import json


def get_output_filename():
    output_files = {
        'county_lv': 'twitter/county_level_intermediate.json',
        'personal_lv': 'twitter/personal_level_intermediate.csv'
    }

    try:
        folder = 'twitter/'
        output_files['county_lv'] = '{}{}'.format(folder, sys.argv[1])
        output_files['personal_lv'] = '{}{}'.format(folder, sys.argv[2])
    except:
        print('Write files to default directory')

    return output_files


def transfer2intermediate(line, header):
    return [line[key] for key in header]


def to_csv(line):
    return '\t'.join(str(data) for data in line)


def write_output_file(rdd, filepath):
    if filepath[-4:] == 'json':
        write_json_file(rdd, filepath)
    elif filepath[-3:] == 'csv':
        write_csv_file(rdd, filepath)


def write_json_file(rdd, filepath):
    json_string = rdd.map(json.dumps).reduce(lambda x, y: x + "\n" + y)
    with open(filepath, 'w') as f:
        f.write(json_string)


def write_csv_file(rdd, filepath):
    header = list(rdd.take(1)[0])
    lines = rdd.map(lambda x: transfer2intermediate(x, header)).map(to_csv)
    try:
        lines.saveAsTextFile(filepath)
    except:
        print('Exception: File Already Exists')