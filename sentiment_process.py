#!/usr/bin/python

import re
import nltk
import math
from aylienapiclient import textapi

from get_clean_table_data import get_clean_table_data

df_art_table, df_ent_table = get_clean_table_data()

df_table_date = df_art_table[df_art_table.publishedat == '06_07_2017']
df_table_date = df_table_date[:20]


from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
df_table_date['nltk_polarity'] = 0
df_table_date['nltk_polarity_title'] = 0
df_table_date['nltk_polarity_section1'] = 0
df_table_date['nltk_polarity_section2'] = 0
df_table_date['aylien_polarity'] = 0
df_table_date['aylien_confidence'] = 0

do_aylien = True
do_nltk = True


df_table_date['cleaned_text'] = 0
sid = SIA()
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")

for index, row in df_table_date.iterrows():
    df_table_date.loc[index, 'cleaned_text'] = re.sub('\n','', row['text']) 

for index, row in df_table_date.iterrows():
    lines_list = tokenizer.tokenize(row['cleaned_text'])
    title = tokenizer.tokenize(row['title'])
    num_sents = len(lines_list)
    section_length = math.floor(num_sents/2)
    section1 = lines_list[:len(lines_list)//2]    
    section2 = lines_list[(-len(lines_list)//2):] 
    if(do_nltk):
   
        print("section1 {}\n section2{}\n length{}\n\n".format(section1, section2,section_length ))
        for sentence in lines_list:
            ss = sid.polarity_scores(sentence)
        df_table_date.loc[index, 'nltk_polarity'] = ss['compound']
        for sentence in title:
            ss = sid.polarity_scores(sentence)
        df_table_date.loc[index, 'nltk_polarity_title'] = ss['compound']
        for sentence in section1:
            ss = sid.polarity_scores(sentence)
        df_table_date.loc[index, 'nltk_polarity_section1'] = ss['compound']
        for sentence in section2:
            ss = sid.polarity_scores(sentence)
        df_table_date.loc[index, 'nltk_polarity_section2'] = ss['compound']
        
    if(do_aylien):  
        aresp = c.Sentiment({'text': row['cleaned_text']})
        aylien_resp_pol = aresp['polarity']
        aylien_resp_conf = aresp['polarity_confidence']

        df_table_date.loc[index, 'aylien_polarity'] = aylien_resp_pol
        df_table_date.loc[index, 'aylien_confidence'] = aylien_resp_conf
        

df_short =  df_table_date.loc[:,['uniqueid','url','score','nltk_polarity', 'aylien_polarity']]
df_short.to_csv('df_short_nltk.csv')



