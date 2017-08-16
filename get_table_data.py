#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info

"""This file does all the backend extraction to get the following tables as needed

get_art_table       - All the articles and text, mainly used for sentiment scoring
get_ent_table       - All the entities, used to get top entities and do normalisation
get_top_ents        - All the entities, but normalised, used to get top entities etc

All functions do some of date conversion at the end and get_art_table adds a length field

"""

def get_art_table():
    conn, cursor = get_conn_info()
    df_art_table = sql.read_sql("SELECT backend_article.uniqueid, \
                                backend_article.addedon, \
                                backend_article.url, \
                                backend_article.title, \
                                backend_article.source, \
                                backend_article.modelprocessed, \
                                backend_sentiment.watson_score, \
                                backend_sentiment.nltk_combined_sentiment, \
                                backend_sentiment.nltk_title_sentiment, \
                                backend_article.entitynormalized, \
                                backend_article.nltk_sentiment, \
                                backend_article.aylien_sentiment_adv, \
                                backend_article.text \
                                FROM \
                                backend_article \
                                INNER JOIN \
                                backend_sentiment \
                                ON backend_article.uniqueid = backend_sentiment.article;"\
                                , conn)  

    df_art_table["addedonpy"] = [d.to_pydatetime() for d in df_art_table["addedon"]]
    df_art_table["addedondt"] = [d.to_pydatetime().date() for d in df_art_table["addedon"]]    
    df_art_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_art_table["addedonpy"]]
    df_art_table["length"] = [len(text) for text in df_art_table["text"]]

    cursor.close()
    conn.close()
    return df_art_table

def get_ent_table():
    conn, cursor = get_conn_info()
    df_ent_table = sql.read_sql("SELECT backend_entities.article, \
                                backend_article.addedon, \
                                backend_article.url, \
                                backend_article.source, \
                                backend_entities.score,  \
                                backend_article.text, \
                                backend_entities.name \
                                FROM \
                                backend_entities\
                                INNER JOIN backend_article \
                                ON backend_article.uniqueid = backend_entities.article;", conn) 


    df_ent_table["addedonpy"] = [d.to_pydatetime() for d in df_ent_table["addedon"]]
    df_ent_table["addedondt"] = [d.to_pydatetime().date() for d in df_ent_table["addedon"]]    
    df_ent_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["addedonpy"]]
    df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]

    cursor.close()
    conn.close()
    return df_ent_table

def get_ent_norm_table():
    conn, cursor = get_conn_info()
  
    df_ent_table_norm = sql.read_sql("SELECT backend_entitiesnormalized.article, \
                                backend_article.addedon, \
                                backend_article.url, \
                                backend_article.source, \
                                backend_entitiesnormalized.score,  \
                                backend_article.text, \
                                backend_entitiesnormalized.name \
                                FROM \
                                backend_entitiesnormalized\
                                INNER JOIN backend_article \
                                ON backend_article.uniqueid = backend_entitiesnormalized.article;", conn)
   
    df_ent_table_norm["addedonpy"] = [d.to_pydatetime() for d in df_ent_table_norm["addedon"]]
    df_ent_table_norm["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table_norm["addedonpy"]]
    df_ent_table_norm["length"] = [len(text) for text in df_ent_table_norm["text"]]

    cursor.close()
    conn.close()
    return df_ent_table_norm


def get_url_table():    
    conn, cursor = get_conn_info()
    df_url_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.url, \
                            backend_article.addedon, \
                            backend_article.sumanalyzed \
                            FROM backend_article;", conn)  
    
    df_url_table["addedonpy"] = [d.to_pydatetime() for d in df_url_table["addedon"]]
    df_url_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_url_table["addedonpy"]]
    cursor.close()
    conn.close()
    return df_url_table

def get_top_ents():
    conn, cursor = get_conn_info()
    df_top_ents_table = sql.read_sql("select backend_article.addedon, backend_entitiesnormalized.name, \
                                        backend_entitiesnormalized.score, \
                                        backend_entitiesnormalized.article,  backend_sentiment.model_score FROM \
                                        backend_sentiment,  backend_article, backend_entitiesnormalized WHERE  \
                                        backend_article.uniqueid = backend_entitiesnormalized.article AND \
                                        backend_article.uniqueid = backend_sentiment.article;", conn)

    df_top_ents_table["addedonpy"] = [d.to_pydatetime() for d in df_top_ents_table["addedon"]]
    df_top_ents_table["addedondt"] = [d.to_pydatetime().date() for d in df_top_ents_table["addedon"]]    
    cursor.close()
    conn.close()
    return df_top_ents_table

    #print("Text cleaned and dates converted")
    #print("Done with DB, number of articles: {}".format(len(df_art_table)))
    #print("Length of each table is df_art_table:{} df_ent_table:{} df_ent_table_norm:{} df_url_table:{}".format(len(df_art_table),len(df_ent_table),len(df_ent_table_norm),len(df_url_table)))  

