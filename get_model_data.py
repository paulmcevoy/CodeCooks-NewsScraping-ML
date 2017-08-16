#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info

#Gets all the latest user ratings and the model score

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
                                    backend_sentiment.model_score, 
                                    backend_emotion.anger,
                                    backend_emotion.disgust,
                                    backend_emotion.fear,
                                    backend_emotion.joy,
                                    backend_emotion.sadness
                                    FROM backend_randomarticleuserrating, backend_sentiment, backend_emotion 
where backend_randomarticleuserrating.uniqueid_id = backend_sentiment.article AND backend_randomarticleuserrating.uniqueid_id = backend_emotion.article;""", conn)  
    cursor.close()
    conn.close()
    #Two versions of the tables are returned. A full one and one where the articles goruped by the mean of the user rating
    df_mod_table_simple = df_mod_table[['articleid', 'watson_score', 'nltk_combined_sentiment', 'nltk_title_sentiment','model_score', 'userscore']]
    df_mod_table_simple_mean = pd.DataFrame(df_mod_table_simple.groupby(['articleid', 'watson_score', 'nltk_combined_sentiment','nltk_title_sentiment', 'model_score'],as_index=False)['userscore'].mean())

    return df_mod_table, df_mod_table_simple_mean

