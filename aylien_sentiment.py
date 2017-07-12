
# coding: utf-8

# In[2]:


from aylienapiclient import textapi
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")
s = c.Sentiment({'text': 'John is a very good football player!'})


# In[3]:


s


# In[36]:


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
    df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
    df_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_table["publishedat"]]

    df_table["length"] = [len(text) for text in df_table["text"]]
   
    print(len(df_table))
    return df_table

df_table = get_entities()
df_table_date = df_table[df_table.publishedat == '06_07_2017']

from aylienapiclient import textapi
c = textapi.Client("f40063af", "ddd790cf8730e5934f0a416f4ac44b2a")
#df_table_date = df_table_date[:10]
#s = c.Sentiment({'text': 'John is a very good football player!'})
df_table_date['new_score'] = 0
df_table_date.to_csv('df_table_date.csv')

for index, row in df_table_date.iterrows():
    #df_table.loc[index, 'new_score'] = aylien_sentiment
    #print(type(row['publishedat']))
    print(df_table_date['text'])
    #print( row['new_score']) 
#print("Watson score: {} Aylien Score {} ". format(df_table['score'], df_table['new_score']))
#print("Aylien Score {} ". format(df_table['new_score']))



# In[34]:


df_table


# In[ ]:




