# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 11:39:37 2017

@author: paul
"""
import psycopg2
import socket
#print(socket.gethostname())

def get_conn_info():
    this_host = socket.gethostname() 
    if (this_host == 'codecooks'):
        conn_string = "host='localhost' dbname='newsapp' user='postgres' password='Doiaereiq43'"
    elif (this_host == 'LAPTOP-UDG3R9OG'):
        conn_string = "host='codecooks.ftp.sh' dbname='newsapp' user='postgres' password='codepostgrescook'"
        
    else:    
        conn_string = "host='localhost' dbname='newsapp' user='postgres' password='codepostgrescook'"
    
    print ("Connecting to database\n    ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    return conn, cursor
