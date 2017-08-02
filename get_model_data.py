#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info
from sentiment_process_aylien import get_aylien


def get_model_data():
    conn, cursor = get_conn_info()


    df_mod_table = sql.read_sql("""Select backend_sentiment.article,
            backend_randomarticleuserrating.score as User_Rated_Score, 
            watson_name, 
            watson_score, 
            aylien_polarity, 
            aylien_confidence, 
            aylien_paragraph1_sentiment, 
            aylien_paragraph2_sentiment, 
            aylien_title_sentiment, 
            aylien_paragraph1_confidence, 
            aylien_paragraph2_confidence, 
            aylien_title_confidence, 
            nltk_paragraph1_sentiment, 
            nltk_paragraph2_sentiment, 
            nltk_title_sentiment, 
            nltk_combined_sentiment, 
            anger,
            disgust,
            fear, 
            joy, 
            sadness
            FROM 
            backend_sentiment, backend_emotion, backend_randomarticleuserrating 
            WHERE 
            backend_sentiment.article = backend_emotion.article AND
            backend_sentiment.article = backend_randomarticleuserrating.uniqueid_id AND
            backend_sentiment.article IN (select article from backend_randomarticle) order by article;""", conn)  

    df_rand_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.publishedat, \
                            backend_article.url, \
                            backend_article.title, \
                            backend_article.source, \
                            backend_article.aylien_sentiment_adv, \
                            backend_sentiment.watson_score, \
                            backend_sentiment.aylien_polarity, \
                            backend_sentiment.aylien_confidence, \
                            backend_sentiment.aylien_paragraph1_sentiment, \
                            backend_sentiment.aylien_paragraph2_sentiment, \
                            backend_sentiment.aylien_title_sentiment, \
                            backend_sentiment.nltk_paragraph1_sentiment, \
                            backend_sentiment.nltk_paragraph2_sentiment, \
                            backend_sentiment.nltk_title_sentiment, \
                            backend_sentiment.nltk_combined_sentiment, \
                            backend_article.text \
                            FROM \
                            backend_article \
                            INNER JOIN \
                            backend_sentiment \
                            ON backend_article.uniqueid = backend_sentiment.article;"\
                            , conn)  
    cursor.close()
    conn.close()
    return df_mod_table, df_rand_table

df_mod_table, df_rand_table = get_model_data()

uniqueid_list = df_mod_table.article.tolist()

df_rand_table_sample_full = pd.DataFrame()


for eachid in uniqueid_list:
    #print(eachid)
    df_rand_table_sample = df_rand_table[(df_rand_table.uniqueid == str(eachid)) ] 
    df_rand_table_sample_full = df_rand_table_sample_full.append(df_rand_table_sample)

df_rand_table_sample_full_cut = df_rand_table_sample_full[df_rand_table_sample_full.aylien_sentiment_adv == False]

df_rand_table_sample_full = df_rand_table_sample_full.drop('text',1)
df_rand_table_sample_full.to_csv('df_rand_table_sample_full_cut.csv')

#get_aylien(df_rand_table_sample_full_cut)
