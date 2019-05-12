# Education Level Prediction Using Twitter
## Data Collection Example
```
$ wget https://archive.org/download/archiveteam-twitter-stream-2017-10/twitter-stream-2017-10-01.tar
$ tar -xvf twitter-stream-2017-10-01.tar
$ find . -name "*.bz2" -exec bunzip2 {} \;
$ python3 gather_twitter_file.py
```
## Data Preprocessing
```
$ python3 tweet_parser.py
```
tweet_parser.py will generate 2 intermediate files. one is a csv file, another is a json file.
## Analysis and Modeling
### Preprocessed personal level tweet data
2013-10: Ongoing

2014-10: Ongoing

2018-10(3GB): https://drive.google.com/file/d/15USANDjsysTDHZBX2IBXSaQCr7--3pbt/view?usp=sharing 
* raw data source(52GB): https://archive.org/details/archiveteam-twitter-stream-2018-10
### Hypothesis Testing
* Whether the word usage in the same education group are similar enough?
* Whether the word usage in different education group are different enough?


## Reference
[1] County Tweet Lexical Bank: https://github.com/wwbp/county_tweet_lexical_bank (U.S. County level word and topic loading derived from a 10% Twitter sample from 2009-2015.)
