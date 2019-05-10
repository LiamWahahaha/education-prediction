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
