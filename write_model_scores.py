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
import numpy as np
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
    
    aylien_title_sentiment = "0"
    if row["aylien_title_sentiment"] in ("posiive", "neutral", "negative"):
        aylien_title_sentiment = row["aylien_title_sentiment"]
        
    aylien_title_confidence = 0
    if row["aylien_title_confidence"] > 0:
        aylien_title_confidence = row["aylien_title_confidence"]
    
    aylien_paragraph1_sentiment = "0"
    if row["aylien_paragraph1_sentiment"] in ("posiive", "neutral", "negative"):
        aylien_paragraph1_sentiment = row["aylien_paragraph1_sentiment"]
        
    aylien_paragraph1_confidence = 0
    if row["aylien_paragraph1_confidence"] > 0:
        aylien_paragraph1_confidence = row["aylien_paragraph1_confidence"]

    aylien_paragraph2_sentiment = "0"
    if row["aylien_paragraph2_sentiment"] in ("posiive", "neutral", "negative"):
        aylien_paragraph2_sentiment = row["aylien_paragraph2_sentiment"]
        
    aylien_paragraph2_confidence = 0
    if row["aylien_paragraph2_confidence"] > 0:
        aylien_paragraph2_confidence = row["aylien_paragraph2_confidence"]

    nltk_combined_sentiment = 0
    if isinstance(row["nltk_combined_sentiment"], numbers.Number):
        nltk_combined_sentiment = row["nltk_combined_sentiment"]
        
    nltk_title_sentiment = 0
    if isinstance(row["nltk_title_sentiment"], numbers.Number):
        nltk_title_sentiment = row["nltk_title_sentiment"]
        
    nltk_paragraph1_sentiment = 0
    if isinstance(row["nltk_paragraph1_sentiment"], numbers.Number):
        nltk_paragraph1_sentiment = row["nltk_paragraph1_sentiment"]

    nltk_paragraph2_sentiment = 0
    if isinstance(row["nltk_paragraph2_sentiment"], numbers.Number):
        nltk_paragraph2_sentiment = row["nltk_paragraph2_sentiment"]

    joy = 0
    if isinstance(row["joy"], numbers.Number):
        joy = row["joy"]
        
    anger = 0
    if isinstance(row["anger"], numbers.Number):
        anger = row["anger"]
        
    disgust = 0
    if isinstance(row["disgust"], numbers.Number):
        disgust = row["disgust"]
        
    fear = 0
    if isinstance(row["fear"], numbers.Number):
        fear = row["fear"] 
        
    sadness = 0
    if isinstance(row["sadness"], numbers.Number):
        sadness = row["sadness"] 
        
    '''
    New regressors 
    ---------------
    It has been observed that some sentiment tools may be better at predicting 
    negative sentiments than they are at predicting positive sentiments, and 
    vice-versa. Therefore, we will split each sentiment regressor into positive 
    and negative, so that they can have different weights.
    '''
    watson_positive = np.where(watson_score > 0, \
        watson_score,0)

    watson_negative = np.where(watson_score < 0, \
        watson_score,0)
    
    nltk_combined_positive = np.where(nltk_combined_sentiment > 0, \
        nltk_combined_sentiment,0)
    
    nltk_combined_negative = np.where(nltk_combined_sentiment < 0, \
        nltk_combined_sentiment,0)
    
    nltk_title_positive = np.where(nltk_title_sentiment > 0, \
        nltk_title_sentiment,0)
    
    nltk_title_negative = np.where(nltk_title_sentiment < 0, \
        nltk_title_sentiment,0)

    nltk_paragraph1_positive = np.where(nltk_paragraph1_sentiment > 0, \
        nltk_paragraph1_sentiment,0)
    
    nltk_paragraph1_negative = np.where(nltk_paragraph1_sentiment < 0, \
        nltk_paragraph1_sentiment,0)
    
    nltk_paragraph2_positive = np.where(nltk_paragraph2_sentiment > 0, \
        nltk_paragraph2_sentiment,0)
    
    nltk_paragraph2_negative = np.where(nltk_paragraph2_sentiment < 0, \
        nltk_paragraph2_sentiment,0)

    '''
    Alyien scores are quite different to those produced by NLTK and Watson. 
    There is a large band of neutral scores and the positive and negative
    scores are measures of confidence.
    
    We convert Aylien sentiment scores into ratings, split into positive and
    negative and ignore neutral. Assume neutral is equivalent to -0.2 to 0.2, 
    positive to > 0.2 and negative to < -0.2.
    '''
    
    #Adjust non-zero positve score from the range 0-1 to the range 0.2-1.
    aylien_positive = np.where(aylien_polarity=="positive", \
        aylien_confidence/0.8 + 0.2,0)
    
    # Adjust non-zero negative score from the range 0 to -1 to -0.2 to -1.
    aylien_negative = np.where(aylien_polarity=="negative", \
        aylien_confidence/-0.8 - 0.2,0)

    # As above for other Alyien scores
    aylien_title_positive = np.where(aylien_title_sentiment=="positive", \
        aylien_title_confidence/0.8 + 0.2,0)
    
    aylien_title_negative = np.where(aylien_title_sentiment=="negative", \
        aylien_title_confidence/-0.8 - 0.2,0)

    aylien_paragraph1_positive = np.where(aylien_paragraph1_sentiment=="positive", \
        aylien_paragraph1_confidence/0.8 + 0.2,0)
    
    aylien_paragraph1_negative = np.where(aylien_paragraph1_sentiment=="negative", \
        aylien_paragraph1_confidence/-0.8 - 0.2,0)

    aylien_paragraph2_positive = np.where(aylien_paragraph2_sentiment=="positive", \
        aylien_paragraph2_confidence/0.8 + 0.2,0)
    
    aylien_paragraph2_negative = np.where(aylien_paragraph2_sentiment=="negative", \
        aylien_paragraph2_confidence/-0.8 - 0.2,0)
    
    '''
    Make regressors orthogonal
    --------------------------
    All relative to NLTK, since it is free. 
    '''

    nc_watson_positive = nltk_combined_positive-watson_positive
    nc_nltk_title_positive = nltk_combined_positive-nltk_title_positive
    nc_nltk_paragraph1_positive = nltk_combined_positive-nltk_paragraph1_positive
    nc_nltk_paragraph2_positive = nltk_combined_positive-nltk_paragraph2_positive
    nc_aylien_positive = nltk_combined_positive-aylien_positive
    nc_aylien_title_positive = nltk_combined_positive-aylien_title_positive
    nc_aylien_paragraph1_positive = nltk_combined_positive-aylien_paragraph1_positive
    nc_aylien_paragraph2_positive = nltk_combined_positive-aylien_paragraph2_positive
    nc_joy = nltk_combined_positive-joy
    nc_watson_negative = nltk_combined_negative-watson_negative
    nc_nltk_title_negative = nltk_combined_negative-nltk_title_negative
    nc_nltk_paragraph1_negative = nltk_combined_negative-nltk_paragraph1_negative
    nc_nltk_paragraph2_negative = nltk_combined_negative-nltk_paragraph2_negative
    nc_aylien_negative = nltk_combined_negative-aylien_negative
    nc_aylien_title_negative = nltk_combined_negative-aylien_title_negative
    nc_aylien_paragraph1_negative = nltk_combined_negative-aylien_paragraph1_negative
    nc_aylien_paragraph2_negative = nltk_combined_negative-aylien_paragraph2_negative
    nc_anger = nltk_combined_negative-anger
    nc_fear = nltk_combined_negative-fear
    nc_sadness = nltk_combined_negative-sadness
    nc_disgust = nltk_combined_negative-disgust
    
    '''
    Extract coefficients from file
    ------------------------------
    The intercept and coefficients were obtained using a separate
    multiple linear regression model script.
    '''    
    intercept = parameters.iloc[0]["intercept"]
    nltk_combined_positive_coefficient = 0
    if "nltk_combined_positive" in parameters.columns:
        nltk_combined_positive_coefficient = parameters.iloc[0]["nltk_combined_positive"]
    nc_watson_positive_coefficient = 0
    if "nc_watson_positive" in parameters.columns:
        nc_watson_positive_coefficient = parameters.iloc[0]["nc_watson_positive"]
    nc_nltk_title_positive_coefficient = 0
    if "nc_nltk_title_positive" in parameters.columns:
        nc_nltk_title_positive_coefficient = parameters.iloc[0]["nc_nltk_title_positive"]
    nc_nltk_paragraph1_positive_coefficient = 0
    if "nc_nltk_paragraph1_positive" in parameters.columns:
        nc_nltk_paragraph1_positive_coefficient = parameters.iloc[0]["nc_nltk_paragraph1_positive"]
    nc_nltk_paragraph2_positive_coefficient = 0
    if "nc_nltk_paragraph2_positive" in parameters.columns:
        nc_nltk_paragraph2_positive_coefficient = parameters.iloc[0]["nc_nltk_paragraph2_positive"]
    nc_aylien_positive_coefficient = 0
    if "nc_aylien_positive" in parameters.columns:
        nc_aylien_positive_coefficient = parameters.iloc[0]["nc_aylien_positive"]
    nc_aylien_title_positive_coefficient = 0
    if "nc_aylien_title_positive" in parameters.columns:
        nc_aylien_title_positive_coefficient = parameters.iloc[0]["nc_aylien_title_positive"]
    nc_aylien_paragraph1_positive_coefficient = 0
    if "nc_aylien_paragraph1_positive" in parameters.columns:
        nc_aylien_paragraph1_positive_coefficient = parameters.iloc[0]["nc_aylien_paragraph1_positive"]
    nc_aylien_paragraph2_positive_coefficient = 0
    if "nc_aylien_paragraph2_positive" in parameters.columns:
        nc_aylien_paragraph2_positive_coefficient = parameters.iloc[0]["nc_aylien_paragraph2_positive"]
    nltk_combined_negative_coefficient = 0
    if "nltk_combined_negative" in parameters.columns:
        nltk_combined_negative_coefficient = parameters.iloc[0]["nltk_combined_negative"]
    nc_watson_negative_coefficient = 0
    if "nc_watson_negative" in parameters.columns:
        nc_watson_negative_coefficient = parameters.iloc[0]["nc_watson_negative"]
    nc_nltk_title_negative_coefficient = 0
    if "nc_nltk_title_negative" in parameters.columns:
        nc_nltk_title_negative_coefficient = parameters.iloc[0]["nc_nltk_title_negative"]
    nc_nltk_paragraph1_negative_coefficient = 0
    if "nc_nltk_paragraph1_negative" in parameters.columns:
        nc_nltk_paragraph1_negative_coefficient = parameters.iloc[0]["nc_nltk_paragraph1_negative"]
    nc_nltk_paragraph2_negative_coefficient = 0
    if "nc_nltk_paragraph2_negative" in parameters.columns:
        nc_nltk_paragraph2_negative_coefficient = parameters.iloc[0]["nc_nltk_paragraph2_negative"]
    nc_aylien_negative_coefficient = 0
    if "nc_aylien_negative" in parameters.columns:
        nc_aylien_negative_coefficient = parameters.iloc[0]["nc_aylien_negative"]
    nc_aylien_title_negative_coefficient = 0
    if "nc_aylien_title_negative" in parameters.columns:
        nc_aylien_title_negative_coefficient = parameters.iloc[0]["nc_aylien_title_negative"]
    nc_aylien_paragraph1_negative_coefficient = 0
    if "nc_aylien_paragraph1_negative" in parameters.columns:
        nc_aylien_paragraph1_negative_coefficient = parameters.iloc[0]["nc_aylien_paragraph1_negative"]
    nc_aylien_paragraph2_negative_coefficient = 0
    if "nc_aylien_paragraph2_negative" in parameters.columns:
        nc_aylien_paragraph2_negative_coefficient = parameters.iloc[0]["nc_aylien_paragraph2_negative"]
    nc_joy_coefficient = 0
    if "nc_joy" in parameters.columns:
        nc_joy_coefficient = parameters.iloc[0]["nc_joy"]
    nc_anger_coefficient = 0
    if "nc_anger" in parameters.columns:
        nc_anger_coefficient = parameters.iloc[0]["nc_anger"]
    nc_fear_coefficient = 0
    if "nc_fear" in parameters.columns:
        nc_fear_coefficient = parameters.iloc[0]["nc_fear"]
    nc_sadness_coefficient = 0
    if "nc_sadness" in parameters.columns:
        nc_sadness_coefficient = parameters.iloc[0]["nc_sadness"]
    nc_disgust_coefficient = 0
    if "nc_disgust" in parameters.columns:
        nc_disgust_coefficient = parameters.iloc[0]["nc_disgust"]
    
    # Calculate predicted score
    predicted_score= intercept + nltk_combined_positive_coefficient*nltk_combined_positive + \
        nc_watson_positive_coefficient*nc_watson_positive + \
        nc_nltk_title_positive_coefficient*nc_nltk_title_positive + \
        nc_nltk_paragraph1_positive_coefficient*nc_nltk_paragraph1_positive + \
        nc_nltk_paragraph2_positive_coefficient*nc_nltk_paragraph2_positive + \
        nc_aylien_positive_coefficient*nc_aylien_positive + \
        nc_aylien_title_positive_coefficient*nc_aylien_title_positive + \
        nc_aylien_paragraph1_positive_coefficient*nc_aylien_paragraph1_positive + \
        nc_aylien_paragraph2_positive_coefficient*nc_aylien_paragraph2_positive + \
        nltk_combined_negative_coefficient*nltk_combined_negative + \
        nc_watson_negative_coefficient*nc_watson_negative + \
        nc_nltk_title_negative_coefficient*nc_nltk_title_negative + \
        nc_nltk_paragraph1_negative_coefficient*nc_nltk_paragraph1_negative + \
        nc_nltk_paragraph2_negative_coefficient*nc_nltk_paragraph2_negative + \
        nc_aylien_negative_coefficient*nc_aylien_negative + \
        nc_aylien_title_negative_coefficient*nc_aylien_title_negative + \
        nc_aylien_paragraph1_negative_coefficient*nc_aylien_paragraph1_negative + \
        nc_aylien_paragraph2_negative_coefficient*nc_aylien_paragraph2_negative + \
        nc_joy_coefficient*nc_joy + \
        nc_anger_coefficient*nc_anger + \
        nc_fear_coefficient*nc_fear + \
        nc_sadness_coefficient*nc_sadness + \
        nc_disgust_coefficient*nc_disgust
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
            sadness,
            model_score
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

    # Read model from file
    parameters = pd.read_csv("parameters.csv", index_col=False)
    
    # Calculate (new) model scores
    x["new_model_score"] = x.apply (lambda row: score(row, parameters),axis=1)
    print(x.head())

    # Update backend_sentiment table with new model scores if different
    for row in rows:

        #print("\n" + row[0] + "\n")

        article = row[0]
        new_score = x.loc[x['article'] == article, 'new_model_score'].iloc[0]
        old_score = x.loc[x['article'] == article, 'model_score'].iloc[0]
        #print(m_score)
        
        #if m_score is valid and different to the old model_score
        if isinstance(new_score, numbers.Number):
            if not(np.isclose(new_score, old_score, atol=1e-05)): 
                sql = "UPDATE backend_sentiment SET model_score = %f WHERE article = '%s';" % (new_score, article)
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