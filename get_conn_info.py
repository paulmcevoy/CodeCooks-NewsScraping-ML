# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 11:39:37 2017

@author: paul
"""
import psycopg2
#print(socket.gethostname())

def get_conn_info():
    conn_string = "host='localhost' dbname='newsapp' user='postgres' password='Doiaereiq43'"

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    return conn, cursor
