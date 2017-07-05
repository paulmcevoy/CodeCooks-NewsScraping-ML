
# coding: utf-8

# In[32]:


# %load connect_select.py
# %load connect_select.py
#!/usr/bin/python
import psycopg2
import sys
import pprint
from datetime import datetime

import pandas.io.sql as sql
import pandas as pd


conn_string = "host='localhost' dbname='newsapp' user='postgres' password='postgres'"
# print the connection string we will use to connect
print ("Connecting to database\n    ->%s" % (conn_string))
 
# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)
# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()
# execute our Query
#cursor.execute("SELECT * FROM backend_article")
#the_frame = pdsql.read_frame("SELECT * FROM %s;" % backend_article, conn)
df_art = sql.read_sql("SELECT * FROM backend_article;", conn)
df_sent = sql.read_sql("SELECT * FROM backend_sentiment;", conn)
df_sent_seq = sql.read_sql("SELECT * FROM backend_sentiment_id_seq;", conn)
df_art_seq = sql.read_sql("SELECT * FROM backend_sentiment_id_seq;", conn)
df_table = sql.read_sql("SELECT backend_article.uniqueid, backend_article.publishedat, backend_article.url, backend_article.source, backend_sentiment.score, backend_article.text FROM backend_article INNER JOIN backend_sentiment ON backend_article.uniqueid = backend_sentiment.article;", conn)
df_ent_table = sql.read_sql("SELECT name FROM backend_entities;", conn)

df_table["publishedat"] = [d.to_pydatetime().date() for d in df_table["publishedat"]]
df_table["length"] = [len(text) for text in df_table["text"]]

source_list = list(set(df_table.source))


df_table.to_csv('df_table.csv')
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('df_table.xlsx', engine='xlsxwriter')
# Convert the dataframe to an XlsxWriter Excel object.
df_table.to_excel(writer, sheet_name='Sheet1')
# Close the Pandas Excel writer and output the Excel file.
writer.save()

#records = cursor.fetchall()
#pprint.pprint(records)

 


# In[78]:


#print(df_ent_table)
#df_ent_table.groupby('name').count()
ent_count_table = df_ent_table.name.value_counts()

#print(type(ent_count_table))
print(ent_count_table[:15])
#ent_count_table_df = ent_count_table.to_frame
print(type(ent_count_table))
#ent_count_table.style

ent_count_table_df = ent_count_table.to_frame()

ent_count_table_df[:15].style
ent_count_table_df_top15 = ent_count_table_df[:15]


ent_count_table_df_top15.to_csv('ent_count_table_df_top15.csv')


# In[30]:


from collections import Counter
import matplotlib.pyplot as plt
import matplotlib
counts = Counter(  df_table['source'] )
mean_sent = df_table['score'].mean()

#print( c.items() )
counts.values()
plt.pie([float(v) for v in counts.values()], labels=counts, autopct='%.1f' )
matplotlib.rcParams['font.size'] = 24.0
fig = plt.gcf()
fig.set_size_inches(9,9) # or (4,4) or (5,5) or whatever
plt.axis('equal')
plt.show()


# In[9]:


from collections import defaultdict
sent_dict = defaultdict(int)
for source in source_list:

    print("{0:20} {1}".format(source, (df_table.loc[df_table['source'] == source, 'score'] ).mean()))
    sent_dict[source] = df_table.loc[df_table['source'] == source, 'score'].mean()
sent_dict


# In[10]:


for index, row in df_table.iterrows():
    print (len(row['text']))


# In[11]:


print(sorted(sent_dict.values()))


# In[24]:


import matplotlib.pyplot as plt

plt.bar(range(len(sent_dict)), sent_dict.values(), align='center')
plt.xticks(range(len(sent_dict)), sent_dict.keys(), rotation='vertical', fontsize=16)
plt.ylabel('Sentiment', fontsize=16)

fig = plt.gcf()
fig.set_size_inches(9,9) # or (4,4) or (5,5) or whatever
plt.show()


# In[7]:


df_table

