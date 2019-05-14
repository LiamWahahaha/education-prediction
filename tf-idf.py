''''
Liam Wang: 111407491
Oswaldo Crespo: 107700568
Varun Goel: 109991128
Ziang Wang: 112077534
'''

# Reference:
# https://towardsdatascience.com/tfidf-for-piece-of-text-in-python-43feccaa74f8
# https://medium.freecodecamp.org/how-to-process-textual-data-using-tf-idf-in-python-cd2bbc0a94a3

import nltk
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
import math


def remove_string_special_characters(s):
    """
    This function removes special charaters within a string
    """
    stripped = re.sub('[^\w\s]', '', s)
    stripped = re.sub('_', '', stripped)
    stripped = re.sub('\s+', ' ', stripped)
    stripped = stripped.strip()

    return stripped

def get_doc(corpus):
    """
    This function splits the text into sentences and considering
    each sentence as a document, calculates the total word count
    of each.
    """
    doc_info = []

    for idx, text in enumerate(corpus):
        count = len(word_tokenize(text))
        doc_info.append({'doc_length': count})

    return doc_info

def create_freq_dict(corpus, doc_info):
    """
    This function creates a frequency dictionary in doc_info for
    each word in each document.
    """
    for idx, content in enumerate(corpus):
        word_freq_table = {}
        splitted_sentence = content.split()
        for word in splitted_sentence:
            word = word.lower()
            if word not in word_freq_table:
                word_freq_table[word] = 1
            else:
                word_freq_table[word] += 1
        doc_info[idx]['freq_dict'] = word_freq_table

def compute_TF(doc_info):
    """
    tf = (frequency of the term in the doc/total number of terms in the doc)
    """
    tf_scores = []

    for idx, doc in enumerate(doc_info):
        tf_score_table = {}
        for word in doc['freq_dict'].keys():
            count = doc['freq_dict'][word]
            tf_score_table[word] = count/doc_info[idx]['doc_length']
        tf_scores.append(tf_score_table)

    return tf_scores

def compute_IDF(doc_info):
    """
    idf = ln(total number of docs/number of docs with term in it)
    """
    number_of_docs = len(doc_info)
    idf_table = {}

    for idx, doc in enumerate(doc_info):
        for word in doc['freq_dict']:
            if word not in idf_table:
                idf_table[word] = 1
            else:
                idf_table[word] += 1

    for word in idf_table.keys():
        idf_table[word] = math.log(number_of_docs/idf_table[word])

    return idf_table

def compute_TFIDF(tf_scores, idf_scores):
    tfidf_table = []
    for idx, tf_score in enumerate(tf_scores):
        temp = {}
        for word in tf_score.keys():
            temp[word] = tf_score[word] * idf_scores[word]

        tfidf_table.append(temp)

    return tfidf_table

def print_message(tf_scores, idf_scores, tfidf_scores):
    for idx, (tf_score, tfidf_score) in enumerate(zip(tf_scores, tfidf_scores)):
        print('Document #{}'.format(idx))
        for word in tf_score:
            print('key: {}, TF-score: {}, IDF-score: {}, TFIDF-score: {}'.\
                    format(word, tf_score[word], idf_scores[word], tfidf_score[word]))

        print('\n')

def main():
    example_text = """If you like tuna and tomato sauce - try combining the two.
                      It's really not as bad as it sounds.
                      If the Easter Bunny and the Tooth Fairy had babies
                       would they take your teeth and leave chocolate for you?"""
    text_sents = sent_tokenize(example_text)
    text_sents_clean = [remove_string_special_characters(text) for text in text_sents]
    doc_info = get_doc(text_sents_clean)
    create_freq_dict(text_sents_clean, doc_info)
    tf_scores = compute_TF(doc_info)
    idf_scores = compute_IDF(doc_info)
    tfidf_scores = compute_TFIDF(tf_scores, idf_scores)
    print_message(tf_scores, idf_scores, tfidf_scores)

if __name__ == '__main__':
    main()
