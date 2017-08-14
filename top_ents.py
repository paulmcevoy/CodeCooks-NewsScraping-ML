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

df_top_ents_table = get_top_ents()

todays_date =  datetime.date.today()
df_top_ents_table_date = df_top_ents_table[df_top_ents_table.addedondt == todays_date]
df_top_ents_table_date_simple = df_top_ents_table_date[['name', 'score', 'model_score']]
df_top_ents_table_date_simple_sum = pd.DataFrame(df_top_ents_table_date_simple.groupby(['name'],as_index=False)['score', 'model_score'].sum())
df_top_ents_table_date_simple_sum['full_ent_score']  = df_top_ents_table_date_simple_sum[['score']].multiply(df_top_ents_table_date_simple_sum['model_score'], axis="index")
df_top_ents_table_date_simple_sum_sorted_asc = df_top_ents_table_date_simple_sum.sort_values('full_ent_score', ascending=False)
df_top_ents_table_date_simple_sum_sorted_des = df_top_ents_table_date_simple_sum.sort_values('full_ent_score', ascending=True)

#df_top_ents_table_date_mean = pd.DataFrame(df_top_ents_table_date.groupby(['name'],as_index=False)['ent_sent_score'].mean())
df_top_ents_table_date_simple_sum.to_csv('df_top_ents_table_date_simple_sum.csv')
ents_top5_pos = df_top_ents_table_date_simple_sum_sorted_asc[:5]
ents_top5_neg = df_top_ents_table_date_simple_sum_sorted_des[:5]

ents_top5_pos_trim = ents_top5_pos[['name', 'full_ent_score']]
ents_top5_neg_trim = ents_top5_neg[['name', 'full_ent_score']]

print("Top ents done")
