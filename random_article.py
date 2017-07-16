
# coding: utf-8

# In[ ]:


from datetime import datetime
import pandas as pd
from get_table_data import get_table_data

df_table, df_ent_table = get_table_data()
df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
df_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_table["publishedat"]]

df_ent_table["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table["publishedat"]]
df_ent_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["publishedat"]]
df_table["length"] = [len(text) for text in df_table["text"]]
df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]
#df_ent_table.drop(['text'])

#convert to csv
df_ent_table.to_csv('df_ent_table.csv')
df_table_short = df_table[['uniqueid', 'url', 'publishedat', 'length', 'source', 'text']]


# In[ ]:


import random
from collections import  defaultdict
df_ents_current_dict = defaultdict(list)
df_ents_sim_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
df_ents_data_set = set(df_table_short['publishedat'])
df_table_short1 = df_table_short[(df_table_short.publishedat == '07_07_2017') | (df_table_short.publishedat == '05_07_2017') | (df_table_short.publishedat == '03_07_2017') | (df_table_short.publishedat == '01_07_2017') | (df_table_short.publishedat == '29_06_2017')]
df_table_short_id = df_table_short1[(df_table_short.length < 10000) & (df_table_short.length > 2000) ]

df_table_short_id.to_csv('df_table_short_id.csv')

df_table_short_id_rand = df_table_short_id.sample(n=25, random_state=234)

df_table_short_id_rand.to_csv('df_table_short_id_rand.csv')
writer = pd.ExcelWriter('df_table_short_id_rand.xlsx', engine='xlsxwriter')
df_table_short_id_rand.to_excel(writer, sheet_name='Sheet1')
writer.save() 

rand_articles =   df_table_short_id_rand         
rand_articles = df_table_short_id_rand[['url', 'publishedat']]

rand_articles.to_csv('rand_articles.csv')
writer = pd.ExcelWriter('rand_articles.xlsx', engine='xlsxwriter')
rand_articles.to_excel(writer, sheet_name='Sheet1')
writer.save() 

df_table_short_id_rand
#rand_articles


# In[ ]:


from aylienapiclient import textapi
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import nltk
import re
from nltk import tokenize
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
    
df_short =  df_table_date.loc[:,['uniqueid','url','score','aylien_polarity', 'nltk_polarity']]
df_short.to_csv('df_short.csv')

