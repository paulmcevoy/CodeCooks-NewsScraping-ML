
# coding: utf-8

# In[69]:


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

    df_ent_table = sql.read_sql("SELECT backend_entities.article, backend_entities.name, backend_entities.type,  backend_entities.score FROM backend_entities;", conn)

    df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
    df_table["length"] = [len(text) for text in df_table["text"]]

    df_ent_table.to_csv('df_ent_table.csv')
    writer = pd.ExcelWriter('df_table.xlsx', engine='xlsxwriter')
    df_table.to_excel(writer, sheet_name='Sheet1')
    writer.save()    
    return df_ent_table

df_ents = get_entities()
 
df_ents_name_and_id =df_ents[['article', 'name']]

#df_ents_name_and_id = df_ents_name_and_id

import csv




# In[ ]:


from collections import  defaultdict
df_ents_name_and_id_dict = defaultdict(list)
df_ents_sim_dict = defaultdict(dict)

for index, row in df_ents_name_and_id.iterrows():
    df_ents_name_and_id_dict[row['article']].append(row['name'])
len_list = []
for key1, value1 in df_ents_name_and_id_dict.items():
    for key2, value2 in df_ents_name_and_id_dict.items():
        sim_num = len(set(value1) & set(value2))
        sim_vals = set(value1) & set(value2)
        if sim_num >= 3 and key1 != key2:
            #print("{} {} {} {}\n".format(key1, key2, sim_num, sim_vals)   )
            df_ents_sim_dict[key1][key2] = sim_num

            
for key1, value1 in df_ents_sim_dict.items():
    for key2, value2 in df_ents_sim_dict.items():
        print("{} {} {}\n".format(key1, key2, sim_num)   )





# In[ ]:




