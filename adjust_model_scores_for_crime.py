#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 07:28:27 2017

@author: Frank Humphreys, Ed Hope

Purpose: Retrieve crime entities from the database, adjust model_score 
downwards for articles containing crime entities. Write the new scores 
to the database and set a crimeadjusted flag to true for relevant articles.
"""
## Import needed modules and libraries
import psycopg2
import psycopg2.extensions
u = psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

import sys

def main():
    
    # Create connection to postgres DB
    try:
        
        conn = psycopg2.connect("dbname=newsapp user=newsappuser host=localhost password=Doiaereiq43")
        conn.autocommit = True
        print("\nConnected\n")

    except:

        print("\nCannot connect to database\n")

    # create handle to retrieve records
    curs = conn.cursor()

    # implement SQL query to read from the database
    try:
        curs.execute("""SELECT DISTINCT backend_entities.article, backend_sentiment.model_score, modelprocessed, crimeadjusted 
            FROM 
            backend_entities, backend_sentiment, backend_article 
            WHERE 
            (backend_entities.article = backend_sentiment.article 
            AND backend_entities.article = backend_article.uniqueid 
            AND backend_entities.type='Crime')""")

        conn.commit()

    # print error if query is not executed successfully.
    except psycopg2.DatabaseError as e:
#        print 'Error %s' % e
        print ("Error {}".format(e))    
    
        sys.exit(1)
    
    # return headers
    names = [ x[0] for x in curs.description]
    # return each row
    rows = curs.fetchall()
    
    conn.commit()	

    # Adjust model_score downwards for articles with crime entities
    # and update backend_sentiment table. 
    # The adjustment is on a linear scale, e.g.:
    # 1 --> 0.5; 0 --> -0.25; -0.5 --> -0.625; -1 --> unchanged.

    for row in rows:

        #print("\n" + row[0] + "\n")

        article = row[0]
        original_m_score = row[1]
        modelprocessed = row[2] 
        crimeadjusted = row[3]
        print(original_m_score)
        if (modelprocessed and not(crimeadjusted)):
            new_m_score = original_m_score - min((original_m_score+1)/4,0)
        else:
            new_m_score = original_m_score    
        print(new_m_score)
        if new_m_score is not(None) :
            sql = "UPDATE backend_sentiment SET model_score = %f WHERE article = '%s';" % (new_m_score, article)
            # set flag that model_score has been adjusted for crime entities.
            sql2 = "UPDATE backend_article SET crimeadjusted = True WHERE uniqueid = '%s';" % article

        try:

            curs.execute(sql)
            curs.execute(sql2)
            
            conn.commit()

            print("Model score data entry succeeded for"), row

        except psycopg2.DatabaseError as e:

            #print 'Error %s' % e    
            print ("Error {}".format(e))    

            sys.exit(1)
    
    # Close the connection when done
    if conn is not None:
        conn.close()
        print("\nDatabase connection closed.\n")
main()