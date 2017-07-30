#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info

def get_table_data():
    conn, cursor = get_conn_info()
    df_art_table = sql.read_sql("SELECT backend_article.uniqueid, \
                                backend_article.addedon, \
                                backend_article.url, \
                                backend_article.title, \
                                backend_article.source, \
                                backend_sentiment.watson_score, \
                                backend_sentiment.nltk_combined_sentiment, \
                                backend_article.entitynormalized, \
                                backend_article.nltk_sentiment, \
                                backend_article.aylien_sentiment, \
                                backend_article.aylien_sentiment_adv, \
                                backend_article.text \
                                FROM \
                                backend_article \
                                INNER JOIN \
                                backend_sentiment \
                                ON backend_article.uniqueid = backend_sentiment.article;"\
                                , conn)  
    print("Got df_art_table")
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
    
    print("Got df_ent_table")
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
   
    print("Got df_ent_table_norm")
    df_url_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.url, \
                            backend_article.addedon, \
                            backend_article.sumanalyzed \
                            FROM backend_article;", conn)  



    print("Got df_url_table")
    df_art_table["addedonpy"] = [d.to_pydatetime().date() for d in df_art_table["addedon"]]
    df_art_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_art_table["addedonpy"]]

    df_ent_table["addedonpy"] = [d.to_pydatetime().date() for d in df_ent_table["addedon"]]
    df_ent_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["addedonpy"]]

    df_ent_table_norm["addedonpy"] = [d.to_pydatetime().date() for d in df_ent_table_norm["addedon"]]
    df_ent_table_norm["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table_norm["addedonpy"]]

    df_url_table["addedonpy"] = [d.to_pydatetime().date() for d in df_url_table["addedon"]]
    df_url_table["addedon"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_url_table["addedonpy"]]

    df_art_table["length"] = [len(text) for text in df_art_table["text"]]
    df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]
    df_ent_table_norm["length"] = [len(text) for text in df_ent_table_norm["text"]]

    print("Text cleaned and dates converted")
    print("Done with DB, number of articles: {}".format(len(df_art_table)))
    print("Length of each table is df_art_table:{} df_ent_table:{} df_ent_table_norm:{} df_url_table:{}".format(len(df_art_table),len(df_ent_table),len(df_ent_table_norm),len(df_url_table)))  
    cursor.close()
    conn.close()
    return df_art_table, df_ent_table, df_ent_table_norm, df_url_table
