#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info

def get_flag_data():
    conn, cursor = get_conn_info()

   
    df_flag_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.analyzed, \
                            backend_article.nltk_sentiment, \
                            backend_article.aylien_sentiment_adv \
                            FROM backend_article;", conn)  


    cursor.close()
    conn.close()
    return df_flag_table


df_flag_table = get_flag_data()

df_flag_table_true  = df_flag_table[(df_flag_table.analyzed == True) & (df_flag_table.nltk_sentiment == True) & (df_flag_table.aylien_sentiment_adv == True)]

conn, cursor = get_conn_info()

print("{} articles fully analyzed".format(len(df_flag_table_true)))

for uniqueid in df_flag_table_true.uniqueid:
    cursor.execute(" update backend_article set sentiment_analyzed = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))

conn.commit()
cursor.close()    