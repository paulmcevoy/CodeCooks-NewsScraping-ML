
# coding: utf-8

# In[ ]:


from datetime import datetime
import pandas as pd
from get_clean_table_data import get_clean_table_data

df_art_table, df_ent_table = get_clean_table_data()

#convert to csv
df_table_short = df_art_table[['uniqueid', 'url', 'publishedat', 'length', 'source', 'watson_score', 'text']]

from collections import  defaultdict
df_table_short1 = df_table_short[(df_table_short.publishedat == '07_07_2017') | (df_table_short.publishedat == '05_07_2017') | (df_table_short.publishedat == '03_07_2017') | (df_table_short.publishedat == '01_07_2017') | (df_table_short.publishedat == '29_06_2017')]
df_table_short_id = df_table_short1[(df_table_short.length < 10000) & (df_table_short.length > 3000) ]

df_table_short_id_rand = df_table_short_id.sample(n=50, random_state=345)

# this is the set of articles we will distribute without any other scores
rand_articles = df_table_short_id_rand[['url', 'publishedat']]
rand_articles['Your Rating (-1 to + 1)'] = 'SCORE'

rand_articles.to_csv('rand_articles.csv')
writer = pd.ExcelWriter('rand_articles.xlsx', engine='xlsxwriter')
rand_articles.to_excel(writer, sheet_name='Sheet1')
writer.save() 

from aylienapiclient import textapi
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import nltk
import re
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
sid = SIA()
df_table_date = df_table_short_id_rand
df_table_date['aylien_polarity'] = 0
df_table_date['cleaned_text'] = 0
df_table_date['nltk_polarity'] = 0
df_table_date['resp'] = 0

for index, row in df_table_date.iterrows():
    df_table_date.loc[index, 'cleaned_text'] = re.sub('\n','', row['text']) 

for index, row in df_table_date.iterrows():
    aresp = c.Sentiment({'text': row['cleaned_text']})
    resp_val = aresp['polarity']
    df_table_date.loc[index, 'aylien_polarity'] = resp_val
    lines_list = tokenizer.tokenize(row['cleaned_text'])
    for sentence in lines_list:
        ss = sid.polarity_scores(sentence)
    df_table_date.loc[index, 'nltk_polarity'] = ss['compound']
    
df_full =  df_table_date.loc[:,['uniqueid','url','watson_score','aylien_polarity', 'nltk_polarity', 'length']]
df_full.to_csv('df_full.csv')

