
# coding: utf-8

# In[ ]:


#!/usr/bin/python
import psycopg2
import sys
import pprint
from datetime import datetime

import pandas.io.sql as sql
import pandas as pd

def get_entities():
    conn_string = "host='localhost' dbname='newsapp' user='postgres' password='postgres'"
    print ("Connecting to database\n    ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    df_table = sql.read_sql("SELECT backend_article.uniqueid, backend_article.publishedat, backend_article.url, backend_article.source, backend_sentiment.score, backend_article.text FROM backend_article INNER JOIN backend_sentiment ON backend_article.uniqueid = backend_sentiment.article;", conn)  
    print("Got df_table")
    df_ent_table = sql.read_sql("""SELECT backend_entities.article, backend_article.publishedat, backend_article.source, backend_entities.name, backend_entities.score, backend_article.url, backend_article.text FROM backend_entities
INNER JOIN backend_article
ON backend_article.uniqueid = backend_entities.article;""", conn) 
    print("Got df_ent_table")
    print("Done with DB, number of articles: {}".format(len(df_table)))
    return df_ent_table, df_table
import csv




# In[ ]:


#tidy up

df_ent_table, df_table = get_entities()
df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
df_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_table["publishedat"]]

df_ent_table["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table["publishedat"]]
df_ent_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["publishedat"]]
df_table["length"] = [len(text) for text in df_table["text"]]
df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]
#df_ent_table.drop(['text'])

#convert to csv
df_ent_table.to_csv('df_ent_table.csv')
df_table_short = df_table[['uniqueid', 'url', 'publishedat', 'length', 'source']]
#df_ents_name_and_id = df_ents_name_and_id[df_ents_name_and_id.publishedat == '07_07_2017']


#df_ents_full = df_ents_name_and_id[:500]
#df_ents_full = df_ents_name_and_id

#df_ents_name_and_id = df_ents_name_and_id


# In[ ]:


df_table_short


# In[ ]:





# In[49]:


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

#for key1, value1 in df_ents_sim_dict.items():
#    for key2, value2 in df_ents_sim_dict.items():
#        print("{} {} {}\n".format(key1, key2, sim_num)   )

df_table_short_id_rand
#rand_articles


# In[ ]:


df_table_short_id_rand


# In[ ]:


df_ents_full_temp = set(df_ents_full['publishedat'])

df_ents_full_temp
#len(df_ents_sim)
#df_ents_sim

set(df_table['publishedat'])

#date_str =  df_table['publishedat'][0].strftime('%d-%m-%Y')
#date_str


# In[ ]:


import os
os.getcwd()

