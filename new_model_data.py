#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info
from get_table_data import get_table_data

df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()
df_mod_table_simple = df_art_table[['uniqueid', 'watson_score', 'nltk_combined_sentiment', 'nltk_title_sentiment']]
#df_mod_table_simple = df_mod_table_simple.loc[df_mod_table_simple.modelprocessed == False]
conn, cursor = get_conn_info()
for index, row in df_mod_table_simple.iterrows():
    if (row['nltk_combined_sentiment'] < 0 and row['watson_score'] < 0):
        new_score = (0.7*row['watson_score'] + 0.3*row['nltk_title_sentiment'])/2
    elif (row['nltk_combined_sentiment'] > 0 and row['watson_score'] > 0):
        new_score = (0.7*row['nltk_combined_sentiment'] + 0.3*row['nltk_title_sentiment'])/2
    else:
        new_score = (0.7* ((row['watson_score'] + row['nltk_title_sentiment'])/2)    +   0.3*row['nltk_title_sentiment']) /2
    print(row['watson_score'], new_score)
    cursor.execute(" update backend_sentiment set model_score = (%s) where article =  (%s) ;", (new_score, row['uniqueid'],))
    #cursor.execute(" update backend_article set modelprocessed = (%s) where uniqueid =  (%s) ;", (True, row['uniqueid'],))
cursor.close()
conn.close()
