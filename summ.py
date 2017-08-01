#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#import nltk
from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals
import sumy
from sumy.parsers.html import HtmlParser
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words
import pandas.io.sql as sql
import pandas as pd

from get_conn_info import get_conn_info

def get_summary(link):
    url = link
    try:
        parser = HtmlParser.from_url(url, Tokenizer("english"))
    except:
        print("URL does not exist")
        parser = None    
    
    if(parser != None):    
        stemmer = Stemmer("english")
        summarizer = Summarizer(stemmer)
        summarizer.stop_words = get_stop_words("english")
        summary_str = ''
        for sentence in summarizer(parser.document, 5):
            summary_str+=str(sentence)
        return summary_str
    else:
        return ""
    
from get_table_data import get_table_data

df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()

#df_url_table = df_url_table[(df_url_table.addedon == '15_07_2017') ]
print(len(df_url_table))
df_url_table = df_url_table[df_url_table.sumanalyzed == False]
df_url_table_date_set = set(df_url_table['addedon'])

print(len(df_url_table))
total_articles = len(df_url_table)
loop_count = 0
print("Starting summarisation")
print(len(df_url_table))
for date in df_url_table_date_set:
    conn, cursor = get_conn_info()
    df_url_table_one_day = df_url_table[(df_url_table.addedon == date) ] 
    
    for index, row in df_url_table_one_day.iterrows():
        if(loop_count%10 == 0):
            print("{} of {} articles processed".format(loop_count, total_articles))
        uniqueid = row['uniqueid']
        summary_val = get_summary(row['url']) 
        cursor.execute(" update backend_article set summary = (%s) where uniqueid =  (%s) ;", (summary_val, uniqueid,))
        cursor.execute(" update backend_article set sumanalyzed = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
        loop_count+=1

    conn.commit()
    cursor.close()



