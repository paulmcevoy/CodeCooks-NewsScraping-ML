# %load connect_select.py
#!/usr/bin/python
import psycopg2
import sys
import pprint

import pandas.io.sql as sql
 
def main():
    conn_string = "host='localhost' dbname='newsapp' user='postgres' password='postgres'"
    # print the connection string we will use to connect
    #print "Connecting to database\n    ->%s" % (conn_string)
 
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
 
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
 
    # execute our Query
    cursor.execute("SELECT * FROM backend_article")

    #the_frame = pdsql.read_frame("SELECT * FROM %s;" % backend_article, conn)
    df = sql.read_sql("SELECT * FROM backend_article;", conn)
#    print(the_frame)
    #print(df)
    #print(df[source[5]])

 
    # retrieve the records from the database
    records = cursor.fetchall()
 
    # print out the records using pretty print
    # note that the NAMES of the columns are not shown, instead just indexes.
    # for most people this isn't very useful so we'll show you how to return
    # columns as a dictionary (hash) in the next example.
    pprint.pprint(records)
 
if __name__ == "__main__":
    main()
