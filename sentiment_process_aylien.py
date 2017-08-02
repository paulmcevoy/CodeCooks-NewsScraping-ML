#!/usr/bin/python3.6
#!/usr/bin/python

import nltk
import math
from aylienapiclient import textapi
from get_conn_info import get_conn_info
import time
from get_table_data import get_table_data
from nltk import tokenize
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
import datetime
def get_aylien(df_sample):
    conn, cursor = get_conn_info()
 #   c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a") #paul
    c = textapi.Client("0ae83fa3", "9b5206bca074e0c2cc55aa055aac92d0") #ed
   
    loop_count = 0
    for index, row in df_sample.iterrows():
        if(loop_count%5 == 0 | loop_count == len(df_sample)):
            print("{} of {} articles processed".format(loop_count, len(df_sample)))
            
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
        s1_norm_conf = aresp['polarity_confidence']
        aresp = c.Sentiment(section2_joined)
        time.sleep(1) 
        s2_norm = aresp['polarity']
        s2_norm_conf = aresp['polarity_confidence']
        aresp = c.Sentiment(title)
        time.sleep(1) 
        stitle = aresp['polarity']
        stitle_conf = aresp['polarity_confidence']
    
#        print("\n--------TITLE--------")
#        print("{}\n".format(title))
#        print("Results:\n{}\nTitle {}\nSection1: {}\nSection2: {}\n\n"\
#              .format(row['uniqueid'], stitle, s1_norm, s2_norm))            
         
        uniqueid = row['uniqueid']
        cursor.execute(" update backend_sentiment set aylien_paragraph1_sentiment = (%s) where article =  (%s) ;", (s1_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_paragraph2_sentiment = (%s) where article =  (%s) ;", (s2_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_title_sentiment = (%s) where article =  (%s) ;", (stitle, uniqueid,))

        cursor.execute(" update backend_sentiment set aylien_paragraph1_confidence = (%s) where article =  (%s) ;", (s1_norm_conf, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_paragraph2_confidence = (%s) where article =  (%s) ;", (s2_norm_conf, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_title_confidence = (%s) where article =  (%s) ;", (stitle_conf, uniqueid,))

        cursor.execute(" update backend_article set aylien_sentiment_adv = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
        loop_count+=1
    
    conn.commit()
    cursor.close()    

df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()

adate = datetime.date(2017, 7, 31)
#print ('Today  :', datetime.datetime.today())
df_table_date = df_art_table[df_art_table.addedonpy > adate]


df_table_date_aylien = df_table_date.loc[df_art_table.aylien_sentiment_adv == False]
df_table_date_aylien_set = set(df_table_date_aylien['addedon'])
#print("Dates: {}".format(df_table_date_aylien_set))

print("Aylien sentiment articles to analyse: {}".format(len(df_table_date_aylien)))
for eachdate in df_table_date_aylien_set:
    df_table_date_aylien_one_day = df_table_date_aylien[(df_table_date_aylien.addedon == eachdate) ] 
    get_aylien(df_table_date_aylien_one_day)





