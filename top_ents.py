#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:58:42 2017

@author: paul
"""
import pandas as pd
import nltk 
import json
from get_table_data import get_top_ents
from get_conn_info import get_conn_info
import datetime
#debug to track memory leaks
#tr = tracker.SummaryTracker()

"""This file:
1. Gets all of the entities from the last 24 hours
2. Sum all of the scores for that entity as an indication of entity strength
3. Multiply the scores by the sentiment of the articles they appear in
4. Gets a top 5 negative and positive from that and sends it to the DB
"""

df_top_ents_table = get_top_ents()

#todays_date =  datetime.date.today()

#We want to grab all stories within in the last 24 hours
one_day_ago = pd.Timestamp.now() - pd.DateOffset(days=1)
df_top_ents_table_date = df_top_ents_table[df_top_ents_table.addedon > one_day_ago]

df_top_ents_table_date_simple = df_top_ents_table_date[['name', 'score', 'model_score']]

#sum all of the scores for that entity as an indication of entity strength
df_top_ents_table_date_simple_sum = pd.DataFrame(df_top_ents_table_date_simple.groupby(['name'],as_index=False)['score', 'model_score'].sum())

#then multiply the scores by the sentiment of the articles they appear in
df_top_ents_table_date_simple_sum['full_ent_score']  = df_top_ents_table_date_simple_sum[['score']].multiply(df_top_ents_table_date_simple_sum['model_score'], axis="index")

#create an ascending and descending list
df_top_ents_table_date_simple_sum_sorted_asc = df_top_ents_table_date_simple_sum.sort_values('full_ent_score', ascending=False)
df_top_ents_table_date_simple_sum_sorted_des = df_top_ents_table_date_simple_sum.sort_values('full_ent_score', ascending=True)

#take the top 5
ents_top5_pos = df_top_ents_table_date_simple_sum_sorted_asc[:5]
ents_top5_neg = df_top_ents_table_date_simple_sum_sorted_des[:5]

#trim away the stuff we don't need
ents_top5_pos_trim = ents_top5_pos[['name', 'full_ent_score']]
ents_top5_neg_trim = ents_top5_neg[['name', 'full_ent_score']]


#send both lists to the DB
conn, cursor = get_conn_info()
cursor.execute("truncate backend_toppositiveentities;")
cursor.execute("truncate backend_topnegativeentities;")

for index, row in ents_top5_pos_trim.iterrows():
	cursor.execute("INSERT INTO backend_toppositiveentities (name, score) VALUES (%s, %s);", (row['name'],row['full_ent_score'])) 

for index, row in ents_top5_neg_trim.iterrows():
	cursor.execute("INSERT INTO backend_topnegativeentities (name, score) VALUES (%s, %s);", (row['name'],row['full_ent_score'])) 


conn.commit()
cursor.close()
conn.close()
print("Top ents done")
print(ents_top5_pos_trim)
print(ents_top5_neg_trim)

