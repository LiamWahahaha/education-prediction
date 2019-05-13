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
2013-10(1GB): https://drive.google.com/file/d/1jqOjM_7CHcmNxnkolbn4KdDPWAb47HI0/view?usp=sharing
* raw data source (43.6GB): https://archive.org/details/archiveteam-twitter-stream-2013-10

2014-10: Ongoing
* raw data source (47.6GB): https://archive.org/details/archiveteam-twitter-stream-2014-10

2015-10:
* raw data source (42.9GB): https://archive.org/details/archiveteam-twitter-stream-2015-10

2016-10: Ongoing
* raw data source(40GB): https://archive.org/details/archiveteam-twitter-stream-2016-10

2017-10: Ongoing
* raw data source(25.5GB): https://archive.org/details/archiveteam-twitter-stream-2017-10

2018-10(3GB): https://drive.google.com/file/d/15USANDjsysTDHZBX2IBXSaQCr7--3pbt/view?usp=sharing 
* raw data source(52GB): https://archive.org/details/archiveteam-twitter-stream-2018-10
### Hypothesis Testing
* Whether the word usage in the same education group are similar enough?
* Whether the word usage in different education group are different enough?


## Reference
[1] County Tweet Lexical Bank: https://github.com/wwbp/county_tweet_lexical_bank (U.S. County level word and topic loading derived from a 10% Twitter sample from 2009-2015.)
