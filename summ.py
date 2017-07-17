# -*- coding: utf-8 -*-
#import nltk
from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

from sumy.parsers.html import HtmlParser
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words
import pandas.io.sql as sql
import pandas as pd
#from create_xls_csv import create_xls_csv
import psycopg2

def get_summary(link):
    url = link
    parser = HtmlParser.from_url(url, Tokenizer("english"))
    stemmer = Stemmer("english")
    summarizer = Summarizer(stemmer)
    summarizer.stop_words = get_stop_words("english")
    summary_str = ''
    for sentence in summarizer(parser.document, 5):
        #print(sentence)
        summary_str+=str(sentence)
    return summary_str

conn_string = "host='codecooks.ftp.sh' dbname='newsapp' user='postgres' password='codepostgrescook'"
print ("Connecting to database\n    ->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
df_url_table = sql.read_sql("SELECT backend_article.uniqueid, backend_article.url, backend_article.publishedat FROM backend_article;", conn)  
df_url_table["publishedat"] = [d.to_pydatetime().date() for d in df_url_table["publishedat"]]
df_url_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_url_table["publishedat"]]
df_url_table = df_url_table[df_url_table.publishedat == '13_07_2017']

for url in df_url_table.url:
    summary_val = get_summary(url) 
    print(summary_val)
    
for index, row in df_url_table.iterrows():
    summary_val = get_summary(row['url']) 
    print(summary_val)
    cursor.execute(" update backend_article set summary = (%s) where uniqueid = '2315459912' or uniqueid = '3426863152';", (summary_val))



conn.commit()
cursor.close()