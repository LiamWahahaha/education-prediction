import pandas as pd

''''
Liam Wang: 111407491
Oswaldo Crespo: 107700568
Varun Goel: 109991128
Ziang Wang: 112077534
'''

cols = [
    'FIPS Code', 'State', 'Area name',
    'Percent of adults with less than a high school diploma, 1980',
    'Percent of adults with a high school diploma only, 1980',
    'Percent of adults completing some college (1-3 years), 1980',
    'Percent of adults completing four years of college or higher, 1980',
    'Percent of adults with less than a high school diploma, 1990',
    'Percent of adults with a high school diploma only, 1990',
    'Percent of adults completing some college or associate\'s degree, 1990',
    'Percent of adults with a bachelor\'s degree or higher, 1990',
    'Percent of adults with less than a high school diploma, 2000',
    'Percent of adults with a high school diploma only, 2000',
    'Percent of adults completing some college or associate\'s degree, 2000',
    'Percent of adults with a bachelor\'s degree or higher, 2000',
    'Percent of adults with less than a high school diploma, 2013-17',
    'Percent of adults with a high school diploma only, 2013-17',
    'Percent of adults completing some college or associate\'s degree, 2013-17',
    'Percent of adults with a bachelor\'s degree or higher, 2013-17'
]
attainment = pd.read_excel(
    'education_attainment.xls', skiprows=4, usecols=cols)
attainment.rename(
    columns={
        'FIPS Code':
        'FIPS',
        'Area name':
        'Area',
        'Percent of adults with less than a high school diploma, 1980':
        '1980-0',
        'Percent of adults with a high school diploma only, 1980':
        '1980-1',
        'Percent of adults completing some college (1-3 years), 1980':
        '1980-2',
        'Percent of adults completing four years of college or higher, 1980':
        '1980-3',
        'Percent of adults with less than a high school diploma, 1990':
        '1990-0',
        'Percent of adults with a high school diploma only, 1990':
        '1990-1',
        'Percent of adults completing some college or associate\'s degree, 1990':
        '1990-2',
        'Percent of adults with a bachelor\'s degree or higher, 1990':
        '1990-3',
        'Percent of adults with less than a high school diploma, 2000':
        '2000-0',
        'Percent of adults with a high school diploma only, 2000':
        '2000-1',
        'Percent of adults completing some college or associate\'s degree, 2000':
        '2000-2',
        'Percent of adults with a bachelor\'s degree or higher, 2000':
        '2000-3',
        'Percent of adults with less than a high school diploma, 2013-17':
        '2013-0',
        'Percent of adults with a high school diploma only, 2013-17':
        '2013-1',
        'Percent of adults completing some college or associate\'s degree, 2013-17':
        '2013-2',
        'Percent of adults with a bachelor\'s degree or higher, 2013-17':
        '2013-3'
    },
    inplace=True)
attainment.fillna(0.0, inplace=True)
attainment.to_csv(path_or_buf='education_attainment.csv', index=False)