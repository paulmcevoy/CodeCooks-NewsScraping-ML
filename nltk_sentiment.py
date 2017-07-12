
# coding: utf-8

# In[70]:


#!/usr/bin/python
import psycopg2
import sys
import pprint
from datetime import datetime
import re
import nltk
import pandas.io.sql as sql
import pandas as pd
import csv

def get_entities():
    conn_string = "host='localhost' dbname='newsapp' user='postgres' password='postgres'"
    print ("Connecting to database\n    ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    df_table = sql.read_sql("SELECT backend_article.uniqueid, backend_article.publishedat, backend_article.url, backend_article.source, backend_sentiment.score, backend_article.text FROM backend_article INNER JOIN backend_sentiment ON backend_article.uniqueid = backend_sentiment.article;", conn)  
    df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
    df_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_table["publishedat"]]

    df_table["length"] = [len(text) for text in df_table["text"]]
   
    print(len(df_table))
    return df_table

df_table = get_entities()
df_table_date = df_table[df_table.publishedat == '06_07_2017']
df_table_date = df_table_date[:50]


import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

from nltk import tokenize
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
df_table_date['nltk_polarity'] = 0
df_table_date['cleaned_text'] = 0
sid = SIA()

for index, row in df_table_date.iterrows():
    df_table_date.loc[index, 'cleaned_text'] = re.sub('\n','', row['text']) 

for index, row in df_table_date.iterrows():
    lines_list = tokenizer.tokenize(row['cleaned_text'])
    for sentence in lines_list:
        ss = sid.polarity_scores(sentence)
    df_table_date.loc[index, 'nltk_polarity'] = ss['compound']

df_short =  df_table_date.loc[:,['uniqueid','url','score','nltk_polarity']]
df_short.to_csv('df_short_nltk.csv')


