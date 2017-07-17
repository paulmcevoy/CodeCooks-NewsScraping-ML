#!/usr/bin/python
import psycopg2
import sys
import pandas.io.sql as sql
import pandas as pd

def get_table_data():
    conn_string = "host='codecooks.ftp.sh' dbname='newsapp' user='postgres' password='codepostgrescook'"
    print ("Connecting to database\n    ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    df_art_table = sql.read_sql("SELECT backend_article.uniqueid, backend_article.publishedat, backend_article.url, backend_article.source, backend_sentiment.watson_score, backend_article.text FROM backend_article INNER JOIN backend_sentiment ON backend_article.uniqueid = backend_sentiment.article;", conn)  
    print("Got df_table")
    df_ent_table = sql.read_sql("""SELECT backend_entities.article, backend_article.publishedat, backend_article.url, backend_article.source, backend_entities.score,  backend_article.text, backend_entities.name FROM backend_entities
INNER JOIN backend_article
ON backend_article.uniqueid = backend_entities.article;""", conn) 
    print("Got df_ent_table")
    print("Done with DB, number of articles: {}".format(len(df_art_table)))
    cursor.close()
    conn.close()
    return df_art_table, df_ent_table
import csv
