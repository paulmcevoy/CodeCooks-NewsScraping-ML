#!/usr/bin/python

import re
import nltk
import math
from aylienapiclient import textapi

from get_table_data import get_table_data

#df_art_table, df_ent_table = get_clean_table_data()
df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()
df_table_date = df_art_table[df_art_table.publishedat == '18_07_2017']
#df_table_date = df_table_date[:20]
#df_table_date = df_table_date[50:150]
print(df_table_date)
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
df_table_date['nltk_polarity'] = 0
df_table_date['nltk_polarity_title'] = 0
df_table_date['nltk_polarity_section1'] = 0
df_table_date['nltk_polarity_section2'] = 0
df_table_date['aylien_polarity'] = 0
df_table_date['aylien_confidence'] = 0

do_aylien = False
do_nltk = True

df_table_date['cleaned_text'] = 0
sid = SIA()
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")
import unicodedata
#for index, row in df_table_date.iterrows():
    #df_table_date.loc[index, 'cleaned_text'] = re.sub('\\','', row['text']) 
    #print( df_table_date.loc[index, 'cleaned_text'])
    #df_table_date.loc[index, 'cleaned_text'] = row['text'] 
    #df_table_date.loc[index, 'cleaned_text'] = unicodedata.normalize("NFKD", row['text'])
    #print("Old str:\n {}\n".format(row['text']))
    #print("New str:\n {}\n".format(new_str))

for index, row in df_table_date.iterrows():
    lines_list = tokenizer.tokenize(row['text'])
    title = tokenizer.tokenize(row['title'])
    num_sents = len(lines_list)
    section_length = math.floor(num_sents/2)
    section1 = lines_list[:len(lines_list)//2]    
    section2 = lines_list[(-len(lines_list)//2):] 
    if(do_nltk):
        sfull = 0
        stitle = 0
        s1 = 0
        s2 = 0
        #print("section1 {}\n section2{}\n length{}\n\n".format(section1, section2,section_length ))
        for sentence in lines_list:
            sfull += sid.polarity_scores(sentence)['compound']
        sfull_norm = sfull/num_sents
        df_table_date.loc[index, 'nltk_polarity'] = sfull_norm
        for sentence in title:
            stitle+= sid.polarity_scores(sentence)['compound']
        df_table_date.loc[index, 'nltk_polarity_title'] = stitle
        print("--------Section1--------")
        for sentence in section1:
            s1 += sid.polarity_scores(sentence)['compound']
            print("{}\nPolarity {}\n\n".format(sentence, sid.polarity_scores(sentence)['compound']))
        s1_norm = s1/section_length
        df_table_date.loc[index, 'nltk_polarity_section1'] = s1_norm
        print("--------Section2--------")
        for sentence in section2:
            s2 += sid.polarity_scores(sentence)['compound']
            print("{}\nPolarity {}\n".format(sentence, sid.polarity_scores(sentence)['compound']))
        s2_norm = s2/section_length
        df_table_date.loc[index, 'nltk_polarity_section2'] = s2_norm
        print("\n--------TITLE--------")
        print("{}\n".format(title))
        print("NLTK:\n{}\nTitle {}\nTotal: {}\nSection1: {}\nSection2: {}\n\n"\
              .format(row['uniqueid'], stitle, sfull_norm, s1_norm, s2_norm))

    if(do_aylien): 
        print("Doing Aylien") 
        aresp = c.Sentiment({'text': row['text']})
        aylien_resp_pol = aresp['polarity']
        aylien_resp_conf = aresp['polarity_confidence']

        df_table_date.loc[index, 'aylien_polarity'] = aylien_resp_pol
        df_table_date.loc[index, 'aylien_confidence'] = aylien_resp_conf
        

df_short =  df_table_date.loc[:,['uniqueid','url','score','nltk_polarity', 'nltk_polarity_title', 'nltk_polarity_section1', 'nltk_polarity_section2', 'aylien_polarity']]
df_short.to_csv('df_short_nltk.csv')



