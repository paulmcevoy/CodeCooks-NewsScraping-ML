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
        for sentence in summarizer(parser.document, 4):
            #print(sentence)
            summary_str+=str(sentence)
        return summary_str
    else:
        return ""
    
conn, cursor = get_conn_info()

df_url_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.url, \
                            backend_article.publishedat, \
                            backend_article.sumanalyzed \
                            FROM backend_article;", conn)  

df_url_table["publishedat"] = [d.to_pydatetime().date() for d in df_url_table["publishedat"]]
df_url_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_url_table["publishedat"]]

df_url_table = df_url_table[df_url_table.sumanalyzed == False]
total_articles = len(df_url_table)
loop_count = 0

for index, row in df_url_table.iterrows():
    if(loop_count%100 == 0):
        print("{} of {} articles processed".format(loop_count, total_articles))
    uniqueid = row['uniqueid']
    summary_val = get_summary(row['url']) 
    cursor.execute(" update backend_article set summary = (%s) where uniqueid =  (%s) ;", (summary_val, uniqueid,))
    cursor.execute(" update backend_article set sumanalyzed = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
    loop_count+=1

conn.commit()
cursor.close()



