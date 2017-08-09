#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 07:28:27 2017

@author: Frank Humphreys, Ed Hope

Purpose: Retrieve sentiment and emotion scores from the database, 
calculates a new sentiment score derived using a linear regression model, and 
then writes the new scores to a dataframe and then to the database.
"""
## Import needed modules and libraries
import psycopg2
import psycopg2.extensions
u = psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

import sys
import pandas as pd
import numbers

#calculate the model score for a row given the parameters
def score(row, parameters):
    # Extract relevant scores from the row if the right type
    # (may not all be used, depending on the parameters)
    watson_score = 0
    if isinstance(row["watson_score"], numbers.Number):
        watson_score = row["watson_score"]
    
    aylien_polarity = "0"
    if row["aylien_polarity"] in ("posiive", "neutral", "negative"):
        aylien_polarity = row["aylien_polarity"]
        
    aylien_confidence = 0
    if row["aylien_confidence"] > 0:
        aylien_confidence = row["aylien_confidence"]
    
    nltk_combined_sentiment = 0
    if isinstance(row["nltk_combined_sentiment"], numbers.Number):
        nltk_combined_sentiment = row["nltk_combined_sentiment"]
        
    nltk_paragraph1_sentiment = 0
    if isinstance(row["nltk_paragraph1_sentiment"], numbers.Number):
        nltk_paragraph1_sentiment = row["nltk_paragraph1_sentiment"]

    nltk_paragraph2_sentiment = 0
    if isinstance(row["nltk_paragraph2_sentiment"], numbers.Number):
        nltk_paragraph1_sentiment = row["nltk_paragraph2_sentiment"]

    nltk_title_sentiment = 0
    if isinstance(row["nltk_title_sentiment"], numbers.Number):
        nltk_title_sentiment = row["nltk_title_sentiment"]
        
    disgust = 0
    if isinstance(row["disgust"], numbers.Number):
        disgust = row["disgust"]
        
    fear = 0
    if isinstance(row["fear"], numbers.Number):
        fear = row["fear"] 
        
    sadness = 0
    if isinstance(row["sadness"], numbers.Number):
        fear = row["sadness"] 
        
    # Calculate the amount, if any, by which watson_score is less than cutoff
    # (may not be used, depending on the parameters)
    #cutoff = parameters.iloc[0][0]
    #very_neg_watson = 0
    #if watson_score < cutoff:
    #    very_neg_watson = watson_score-cutoff

    # The intercept and coefficient were obtained using a separate
    # multiple linear regression model.
    intercept = parameters.iloc[0][1]
    nltk_combined_sentiment_coefficient = parameters.iloc[0][2]
    sadness_coefficient = parameters.iloc[0][3]
    nltk_combined_paragraph2_coefficient = parameters.iloc[0][4]
    nltk_combined_title_coefficient = parameters.iloc[0][5]
    
    # Convert aylien polarity string into a number (may not be used, depending on the parameters)
    if aylien_polarity == "negative": 
        alyien_pole = -1
    elif aylien_polarity == "positive":
        alyien_pole = 1
    else:
        alyien_pole = 0
    
    # Calculate predicted score
    predicted_score= intercept + nltk_combined_sentiment_coefficient*nltk_combined_sentiment + \
        sadness_coefficient*sadness + \
        nltk_combined_paragraph2_coefficient*(nltk_combined_sentiment-nltk_paragraph2_sentiment) + \
        nltk_combined_title_coefficient*(nltk_combined_sentiment-nltk_title_sentiment)
    return (predicted_score)

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
        curs.execute("""select backend_sentiment.article, 
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
            backend_sentiment, backend_emotion 
            WHERE 
            backend_sentiment.article = backend_emotion.article AND
            backend_sentiment.article IN (select uniqueid from backend_article)""")

        conn.commit()

    # print error if query is not executed successfully.
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e    
        sys.exit(1)
    
    # retrun headers
    names = [ x[0] for x in curs.description]
    # return each row
    rows = curs.fetchall()
    
    conn.commit()	

    # put query results in a panda data frame as in https://gist.github.com/mvaz/2006493
    x = pd.DataFrame( rows, columns=names)

    
    parameters = pd.read_csv("parameters.csv", index_col=False)
    x["model_score"] = x.apply (lambda row: score(row, parameters),axis=1)
    print(x.head())

    # Calculate model score and update backend_sentiment table

    for row in rows:

        #print("\n" + row[0] + "\n")

        article = row[0]
        m_score = x.loc[x['article'] == article, 'model_score'].iloc[0]
        #print(m_score)
        if m_score is not(None):
            sql = "UPDATE backend_sentiment SET model_score = %f WHERE article = '%s';" % (m_score, article)
            sql2 = "UPDATE backend_article SET modelprocessed = true WHERE uniqueid = '%s';" % article

        try:

            curs.execute(sql)
            curs.execute(sql2)
            
            conn.commit()

            #print("Model score data entry succeeded for"), row

        except psycopg2.DatabaseError, e:

            print 'Error %s' % e    

            sys.exit(1)
    
    # Close the connection when done
    if conn is not None:
        conn.close()
        print("\nDatabase connection closed.\n")
main()