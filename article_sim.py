
# coding: utf-8

# In[113]:


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
#    df_ent_table = sql.read_sql("SELECT backend_entities.article, backend_entities.name, backend_entities.type,  backend_entities.score FROM backend_entities;", conn)

    df_ent_table = sql.read_sql("""SELECT backend_entities.article, backend_article.publishedat, backend_entities.name, backend_entities.type,  backend_entities.score, backend_article.url FROM backend_entities
INNER JOIN backend_article
ON backend_article.uniqueid = backend_entities.article;""", conn) 

    df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
    df_ent_table["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table["publishedat"]]
    
    df_ent_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["publishedat"]]
   #  if not pd.isnull(d) else '' for d in df1['date']
    df_table["length"] = [len(text) for text in df_table["text"]]

    
    df_ent_table.to_csv('df_ent_table.csv')
    
    writer = pd.ExcelWriter('df_table.xlsx', engine='xlsxwriter')
    df_table.to_excel(writer, sheet_name='Sheet1')
    writer.save() 
    
    writer = pd.ExcelWriter('df_ent_table.xlsx', engine='xlsxwriter')
    df_ent_table.to_excel(writer, sheet_name='Sheet1')
    writer.save()     
    
    print(len(df_table))
    return df_ent_table, df_table

df_ents, df_table = get_entities()
 
df_ents_name_and_id =df_ents[['article', 'name', 'url', 'publishedat']]

#df_ents_full = df_ents_name_and_id[:500]
df_ents_full = df_ents_name_and_id

#df_ents_name_and_id = df_ents_name_and_id

import csv




# In[114]:


df_ents_full


# In[ ]:


from collections import  defaultdict
df_ents_current_dict = defaultdict(list)
df_ents_sim_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
df_ents_data_set = set(df_ents_full['publishedat'])

for date in df_ents_data_set:
    df_ents_current = df_ents_full[df_ents_full.publishedat == date]
    for index, row in df_ents_current.iterrows():
        df_ents_current_dict[row['article']].append(row['name'])
    len_list = []
    for key1, value1 in df_ents_current_dict.items():
        for key2, value2 in df_ents_current_dict.items():
            sim_num = len(set(value1) & set(value2))
            sim_vals = set(value1) & set(value2)
            if sim_num >= 3 and key1 != key2:
                #print("{} {} {} {}\n".format(key1, key2, sim_num, sim_vals)   )
                df_ents_sim_dict[key1][key2] = sim_num
                df_ents_sim = df_ents_sim.append(pd.DataFrame({'article1': key1, 'article2': key2, 'sim_count': sim_num, 'entities': sim_vals, 'publishedat': publishedat}, index=[0]), ignore_index=True)

    writer = pd.ExcelWriter('df_ents_sim_%s.xlsx' % date, engine='xlsxwriter')
    df_ents_sim.to_excel(writer, sheet_name='Sheet1')
    writer.save() 
                    
            
#for key1, value1 in df_ents_sim_dict.items():
#    for key2, value2 in df_ents_sim_dict.items():
#        print("{} {} {}\n".format(key1, key2, sim_num)   )





# In[3]:


writer = pd.ExcelWriter('df_ents_sim.xlsx', engine='xlsxwriter')
df_ents_sim.to_excel(writer, sheet_name='Sheet1')
writer.save() 


# In[90]:


df_ents_full_temp = set(df_ents_full['publishedat'])

df_ents_full_temp
#len(df_ents_sim)
#df_ents_sim

set(df_table['publishedat'])

#date_str =  df_table['publishedat'][0].strftime('%d-%m-%Y')
#date_str

