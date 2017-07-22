# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 11:39:37 2017

@author: paul
"""
import psycopg2

def get_conn_info():
    conn_string = "host='codecooks.ftp.sh' dbname='newsapp' user='postgres' password='codepostgrescook'"
    print ("Connecting to database\n    ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    return conn, cursor