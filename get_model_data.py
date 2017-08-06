#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info
#from sentiment_process_aylien import get_aylien
from collections import  defaultdict


def get_mod_data():
    conn, cursor = get_conn_info()

    df_mod_table = sql.read_sql("""SELECT backend_randomarticleuserrating.uniqueid_id As ArticleID, 
                                    backend_randomarticleuserrating.score as UserScore,
                                    backend_sentiment.watson_score, 
                                    backend_sentiment.aylien_polarity, 
                                    backend_sentiment.aylien_confidence, 
                                    backend_sentiment.aylien_paragraph1_sentiment, 
                                    backend_sentiment.aylien_paragraph1_confidence, 
                                    backend_sentiment.aylien_paragraph2_sentiment, 
                                    backend_sentiment.aylien_paragraph2_confidence, 
                                    backend_sentiment.aylien_title_sentiment, 
                                    backend_sentiment.aylien_title_confidence, 
                                    backend_sentiment.nltk_paragraph1_sentiment, 
                                    backend_sentiment.nltk_paragraph2_sentiment, 
                                    backend_sentiment.nltk_title_sentiment, 
                                    backend_sentiment.nltk_combined_sentiment, 
                                    backend_emotion.anger,
                                    backend_emotion.disgust,
                                    backend_emotion.fear,
                                    backend_emotion.joy,
                                    backend_emotion.sadness
                                    FROM backend_randomarticleuserrating, backend_sentiment, backend_emotion 
where backend_randomarticleuserrating.uniqueid_id = backend_sentiment.article AND backend_randomarticleuserrating.uniqueid_id = backend_emotion.article;""", conn)  
    cursor.close()
    conn.close()
    return df_mod_table


#df_rand_table = get_rand_data()
df_mod_table = get_mod_data()
df_mod_table.to_csv('df_mod_table.csv')


#uniqueid_list = df_mod2_table.uniqueid.tolist()
#uniqueid_list_set = set(uniqueid_list)
#df_rand_table_sample_full = pd.DataFrame()
"""

for eachid in uniqueid_list:
    #print(eachid)
    df_rand_table_sample = df_rand_table[(df_rand_table.uniqueid == str(eachid)) ] 
    df_rand_table_sample_full = df_rand_table_sample_full.append(df_rand_table_sample)

df_rand_table_sample_full_cut = df_rand_table_sample_full[df_rand_table_sample_full.sentiment_analyzed == True]
#df_mod_table = df_mod_table[df_mod_table.aylien_sentiment_adv == False]


df_rand_table_sample_full = df_rand_table_sample_full.drop('text',1)
df_rand_table_sample_full.to_csv('df_rand_table_sample_full_cut.csv')

#get_aylien(df_rand_table_sample_full_cut)
"""
df_mod_table_simple = df_mod_table[['articleid', 'watson_score', 'nltk_combined_sentiment', 'nltk_title_sentiment', 'userscore']]
df_mod_table_simple_mean = pd.DataFrame(df_mod_table_simple.groupby(['articleid', 'watson_score', 'nltk_combined_sentiment','nltk_title_sentiment'])['userscore'].mean())
df_mod_table_simple_mean.to_csv('df_mod_table_simple_mean.csv')


