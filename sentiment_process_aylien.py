#!/usr/bin/python

import nltk
import math
from aylienapiclient import textapi
from get_conn_info import get_conn_info
import time
from get_table_data import get_table_data
from nltk import tokenize
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
from datetime import date

df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()

adate = date(2017, 7, 28)
df_table_date = df_art_table[df_art_table.publishedatpy > adate]

c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")

df_table_date_aylien = df_table_date[df_art_table.aylien_sentiment_adv == False]
#df_table_date_aylien = df_table_date_aylien[5:10]
df_table_date_aylien_set = set(df_table_date_aylien['publishedat'])

print(len(df_table_date_aylien))
total_articles = len(df_table_date_aylien)
loop_count = 0
print("Starting Aylien sentiment analysis")
for eachdate in df_table_date_aylien_set:
    conn, cursor = get_conn_info()
    df_table_date_aylien_one_day = df_table_date_aylien[(df_table_date_aylien.publishedat == eachdate) ] 
    
    for index, row in df_table_date_aylien_one_day.iterrows():
        if(loop_count%10 == 0):
            print("{} of {} articles processed".format(loop_count, total_articles))
            
        lines_list = tokenizer.tokenize(row['text'])
        title = row['title']
        num_sents = len(lines_list)
        section_length = math.floor(num_sents/2)
        section1 = lines_list[:len(lines_list)//2]  
        section1_joined = " ".join(section1)
        section2 = lines_list[(-len(lines_list)//2):] 
        section2_joined = " ".join(section2)

        aresp = c.Sentiment(section1_joined)
        time.sleep(1) 
        s1_norm = aresp['polarity']
        aresp = c.Sentiment(section2_joined)
        time.sleep(1) 
        s2_norm = aresp['polarity']
        aresp = c.Sentiment(title)
        time.sleep(1) 
        stitle = aresp['polarity']

        #print("\n--------TITLE--------")
        #print("{}\n".format(title))
        #print("NLTK:\n{}\nTitle {}\nTotal: {}\nSection1: {}\nSection2: {}\n\n"\
        #      .format(row['uniqueid'], stitle, sfull_norm, s1_norm, s2_norm))            
        
        uniqueid = row['uniqueid']
        cursor.execute(" update backend_sentiment set aylien_paragraph1_sentiment = (%s) where article =  (%s) ;", (s1_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_paragraph2_sentiment = (%s) where article =  (%s) ;", (s2_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_title_sentiment = (%s) where article =  (%s) ;", (stitle, uniqueid,))
        cursor.execute(" update backend_article set aylien_sentiment_adv = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
        loop_count+=1
    
    conn.commit()
    cursor.close()
print("{} articles processed by Aylien".format(loop_count))


#df_short =  df_table_date_nltk.loc[:,['uniqueid','url','score','nltk_polarity', 'nltk_polarity_title', 'nltk_polarity_section1', 'nltk_polarity_section2', 'aylien_polarity']]
#df_short.to_csv('df_short_nltk.csv')



