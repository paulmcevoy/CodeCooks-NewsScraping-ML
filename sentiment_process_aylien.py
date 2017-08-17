#!/usr/bin/python3.6
#!/usr/bin/python

import nltk
import math
from aylienapiclient import textapi
from get_conn_info import get_conn_info
import time
from get_table_data import get_art_table, get_ent_table, get_url_table
from nltk import tokenize

"""This file gets all of the advanced sentiment info like heading, paragraph etc

The program carries out the following steps:
1. Gets the article data from the DB
2. Gets articles after 31st July, due to Unit limits
3. Gets articles that have not been analyzed yet
4. Splits the articles into 1st and 2nd half using NLTK tokenizer
5. Gets the sentiment polarity and confidence from Aylien for Title, 1st and 2nd sections

The main arguments for this function is

uniqueid                - unique article ID
addedon                 - date article was added on
title                   - title of article
aylien_sentiment_adv    - flag to indicate sentiment has been added
text                    - clean article text

"""

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
import datetime
def get_aylien(df_sample):
    conn, cursor = get_conn_info()
    #Sometimes we need to cycle accounts to get enough units, however this is done manually
 #   c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a") #paul
 #   c = textapi.Client("0ae83fa3", "9b5206bca074e0c2cc55aa055aac92d0") #ed
    c = textapi.Client("b08da84e", "eb6d7a1184b9aaf845646e66a513429f") #ty
   
    loop_count = 0

    #The argument for this function is a dataframe with all the articles we want to get the Aylien 
    #sentiment details for.
    for index, row in df_sample.iterrows():

        #Report the progress every 5 articles
        if(loop_count%5 == 0 | loop_count == len(df_sample)):
            print("{} of {} articles processed".format(loop_count, len(df_sample)))
        
        #How many lines are in the text    
        lines_list = tokenizer.tokenize(row['text'])
        title = row['title']
        num_sents = len(lines_list)

        #Get the section length and then the text sections for upper and lower
        section_length = math.floor(num_sents/2)
        section1 = lines_list[:len(lines_list)//2]  
        section1_joined = " ".join(section1)
        section2 = lines_list[(-len(lines_list)//2):] 
        section2_joined = " ".join(section2)
    
        aresp = c.Sentiment(section1_joined)
        #need to delay requests to ensure we adhere to Aylien requirements
        time.sleep(1) 
        #extract the info from the response
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
    
        #We want to insert into the DB based on the uniqueID
        uniqueid = row['uniqueid']
        cursor.execute(" update backend_sentiment set aylien_paragraph1_sentiment = (%s) where article =  (%s) ;", (s1_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_paragraph2_sentiment = (%s) where article =  (%s) ;", (s2_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_title_sentiment = (%s) where article =  (%s) ;", (stitle, uniqueid,))

        cursor.execute(" update backend_sentiment set aylien_paragraph1_confidence = (%s) where article =  (%s) ;", (s1_norm_conf, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_paragraph2_confidence = (%s) where article =  (%s) ;", (s2_norm_conf, uniqueid,))
        cursor.execute(" update backend_sentiment set aylien_title_confidence = (%s) where article =  (%s) ;", (stitle_conf, uniqueid,))

        cursor.execute(" update backend_article set aylien_sentiment_adv = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
        #keep an eye on the ammount of requests we have done
        loop_count+=1
    
    conn.commit()
    cursor.close()    

df_art_table = get_art_table()

#We decided to starting getting advanced sentiment detail at the end of July
#We wouldn't have enough units to go back and get Aylien info for all older articles
#only get Aylien data after this date due to limits
adate = datetime.date(2017, 7, 31)
df_table_date = df_art_table[df_art_table.addedonpy > adate]

#Only get articles that have not been analysed yet
df_table_date_aylien = df_table_date.loc[df_art_table.aylien_sentiment_adv == False]
#get a list of all the dates
df_table_date_aylien_set = set(df_table_date_aylien['addedon'])

print("Aylien sentiment articles to analyse: {}".format(len(df_table_date_aylien)))
for eachdate in df_table_date_aylien_set:
    df_table_date_aylien_one_day = df_table_date_aylien[(df_table_date_aylien.addedon == eachdate) ] 
    #Send the list of articles to get sentiment scores in groups of each date
    get_aylien(df_table_date_aylien_one_day)





