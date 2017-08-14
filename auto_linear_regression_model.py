# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 20:09:52 2017

@author: Frank Humphreys

Purpose: This program takes an input file, regression_training_data.csv, and 
outputs a parsimonious linear regression model, including its parameters in 
parameters.csv. The variables in the original data are split into positive 
and negative variables and then made orthogonal in order to handle multi-
collinearity and increase robustness.

max_num_regressors puts a limit on the number of regressors the model will 
contain. Reducing it can speed up the code considerably but it will, of course, 
affect the output.

It is assumed that the input file,
1. contains the following columns:
    articleid, watson_score, aylien_polarity, aylien_confidence, 
    aylien_paragraph1_sentiment, aylien_paragraph1_confidence, 
    aylien_paragraph2_sentiment, aylien_paragraph2_confidence, 
    aylien_title_sentiment, aylien_title_confidence, nltk_paragraph1_sentiment, 
    nltk_paragraph2_sentiment, nltk_title_sentiment, nltk_combined_sentiment, 
    anger, disgust, fear, joy, sadness, mean
2. may also contain random, stdev values, count but no other columns
3. has no missing values other than for aylien columns. 

Any column that you wish to be excluded can be removed from the statement that
creates the regressors variable below.
"""

from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
from sklearn.metrics import explained_variance_score
from scipy.stats import normaltest
from math import isclose
from itertools import chain, combinations

def calc_model_score(filename,x,y,model,max_ar2,k_max_ar2,min_improvement):
    model.fit(x,y)
    
    # Intercept
    #print("Intercept: \n", model.intercept_)
    
    # The coefficients
    coefficients = model.coef_
    #print("Coefficients: \n" + repr(coefficients))
    
    # Shape
    #print ("rows: %d, columns: %d" % (x.shape[0], x.shape[1]))
 
    # R-squared (Coefficient of determination). 1 is perfect prediction.
    meanY = np.mean(y);

    '''
    Calculate R-squared
    -------------------
    
    Although there are built-in functions for R-squared, there appears not 
    to be one for adjusted R-squared. Comparing this hand-coded R-squared
    with the built-in functions, is a useful indirect check that the 
    adjusted R-squared code below is correct.
    '''
    r2 = 1 - (sum((model.predict(x) - y) ** 2)/(sum((y-meanY) ** 2)))
    #print("R-squared / Explained variance score: %.4f:" % r2)
    
    '''
    Check R-squared
    ---------------
    
    3 built-in functions for R-squared. They should give the same result.
    We will do a quick check that they do. 
    Since there may be rounding errors, we will use isclose and check within 
    an error tolerance small enough to make no difference for our purpose
    '''
    if  not(isclose(r2_score(y,model.predict(x)), r2, abs_tol=1e-05) and \
    isclose(model.score(x, y), r2, abs_tol=1e-05) and \
    isclose(explained_variance_score(y, model.predict(x)), r2, abs_tol=1e-05)):
        print("R-squared check: failed.")
        print("R-squared / Explained variance score: %.8f" % r2)
        print("Explained variance score: %.8f" % model.score(x, y))
        print("Explained variance score: %.8f" \
              % explained_variance_score(y, model.predict(x)))
   
    '''
    Calculate adjusted R-squared
    -------------------
    
    As R-squared increases with the number of X parameters, we cannot
    compare models with differing numbers of X variables. To do that
    we need the adjusted R-squared.
    
    The adjusted R-squared is 1 - (sum(error**2)/n-k)/(sum(y**2)/n-1),
    where k is the number of parameters including the intercept
    '''
    n = x.shape[0]
    #print("n: %d:" % n)
    k = x.shape[1]+1
    #print("k: %d:" % k)
    ar2 = 1 - \
        (sum((model.predict(x) - y) ** 2)/(n-k))/(sum((y-meanY) ** 2)/(n-1))
    #print("Adjusted R-squared: %.4f:" % ar2)
    

    # The mean squared error
    mse = np.mean((model.predict(x) - y) ** 2)
    # i.e. mse = np.sum((model.predict(x) - y) ** 2)/x.shape[0]
    #print("\nOther stats: \nMean squared error: %.5f" % mse)
    
    # The square root of the mean squared error
    rmse = mse ** 0.5
    #print("Root mean squared error: %.5f" % rmse)

    # The square of the standard error
    sse = np.sum((model.predict(x) - y) ** 2)/(x.shape[0]-x.shape[1])    
    #print("The OLS estimate of the conditional variance of the errors and response variable y: %.5f" % sse)
    
    # The standard error
    se = sse** 0.5
    #print("Standard error of the estimate: %.5f" % se)
    
    
    '''
    Parsimony and using the adjusted R-squared 
    ------------------------------------------
    
    The value of the adjusted R-squared is a reasonable way to competing linear 
    regression models with differing numbers of regressors. We do this below.
    
    Parsimony is the idea that it is best to try and keep the number of 
    regressors in a model small. 
    
    We will overwrite parameters.csv and update the maximum adjusted R-squared 
    (max_ar2) with ar2 and return the new associated value for k (k_max_ar2) 
    if a different set of parameters produces a higher adjusted R-squared score
    if k is unchanged 
    
    But, for reasons of parsimony, we will only use a model with more 
    parameters if the adjusted R-squared is higher and by a factor of at least 
    min_improvement for each additional parameter. 
    '''
    if (ar2 > max_ar2) and ar2 > max_ar2*(1 + min_improvement*(k-k_max_ar2)):
        # Open the file to which we will output the parameters.
        f = open(filename,"w")
        # Write the headings
        f.write ("R-2,AR-2,n,k,mse,rmse,sse,se,intercept")
        for i in range(len(coefficients)):
            f.write(","+x.columns[i])
        f.write ("\n")
        # Write the parameters
        f.write(repr(r2) + "," + repr(ar2) + "," + repr(n) + "," + repr(k) + \
                "," + repr(mse) + "," + repr(rmse) + "," + repr(sse) + "," + \
                repr(se) + "," + repr(model.intercept_))
        for i in range(len(coefficients)):
            f.write("," + repr(coefficients[i]))
        f.write ("\n")
        f.close()
        
        # Print the scores
        print("\nCurrent best model")
        print("R-squared: %.4f\tAdj. R-squared: %.4f\tn: %d\tk: %d" \
              % (r2,ar2,n,k))
        print("mse: %.4f\trmse: %.4f\tsse: %.4f\tse: %.4f" \
              % (mse,rmse,sse,se))
        #Print intercept
        print("intercept: %f" % model.intercept_)
        #Print the model headings and parameters
        for i in range(len(coefficients)):
            print(x.columns[i],": ",coefficients[i])
        max_ar2 = ar2
        k_max_ar2 = k
    return(max_ar2,k_max_ar2)

'''
very_negative_Watson
--------------------

The idea here was to create a new variable for the amount by which Watson is 
more negative than a cutoff. E.g., 
    
cutoff = -0.4
x["V_neg_Watson"] = \
    x.apply (lambda row: very_negative_Watson(row,cutoff),axis=1)

The code has become somewhat redundant due to the new simpler approach to 
splitting sentiment variables below and is currently unused.

If it were reused, the code to write the parameters would have to be augmented 
to include the cutoff score and label.
'''
def very_negative_Watson(row, cutoff):
    if row["watson_score"] < cutoff:
        return(row["watson_score"]-cutoff)
    return(0)
 
'''
powerset_subsets
----------------

This returns all the unique subsets up to length max_length of a given input 
set. It is a modified version of code to produce the entire powerset found 
here: https://docs.python.org/2/library/itertools.html#recipes 
'''
def powerset_subsets(iterable,max_length):
    #powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in \
                               range(min(max_length,len(s))+1))

def main():
    model = LinearRegression(fit_intercept=True)
    df = pd.read_csv("regression_train_data.csv", index_col=0)
   
    #Drop mean and, if they exist, unused columns  
    x = df.drop(["mean"], axis=1)
    for col in ("random", "stdev values", "count"):
        if col in x.columns:
            x = df.drop([col], axis=1)

    '''
    Missing values
    --------------
    
    The input file should have no missing values. We recommended that this be 
    managed by separate code to clean the data. If obtaining values is 
    difficult for some input parameters, then they can be dropped from the 
    input file and from the code that creates new and orthogonal regressors.
    
    We do some basic filling of missing values here for Aylien scores only
    '''   
    x["aylien_polarity"] = x["aylien_polarity"].fillna("none")
    
    x["aylien_confidence"] = x["aylien_confidence"].fillna(0)
    
    x["aylien_title_sentiment"] = x["aylien_title_sentiment"].fillna("none")
    
    x["aylien_title_confidence"] = x["aylien_title_confidence"].fillna(0)
    
    x["aylien_paragraph1_sentiment"] = \
        x["aylien_paragraph1_sentiment"].fillna("none")
    
    x["aylien_paragraph1_confidence"] = \
        x["aylien_paragraph1_confidence"].fillna(0)
    
    x["aylien_paragraph2_sentiment"] = \
        x["aylien_paragraph2_sentiment"].fillna("none")
    
    x["aylien_paragraph2_confidence"] = \
        x["aylien_paragraph2_confidence"].fillna(0)
    
    '''
    New regressors 
    ---------------
    It has been observed that some sentiment tools may be better at predicting 
    negative sentiments than they are at predicting positive sentiments, and 
    vice-versa. Therefore, we will split each sentiment regressor into positive 
    and negative, so that they can have different weights.
    '''
    x["watson_positive"] = np.where(x["watson_score"] > 0, \
        x["watson_score"],0)

    x["watson_negative"] = np.where(x["watson_score"] < 0, \
        x["watson_score"],0)
    
    x["nltk_combined_positive"] = np.where(x["nltk_combined_sentiment"] > 0, \
        x["nltk_combined_sentiment"],0)
    
    x["nltk_combined_negative"] = np.where(x["nltk_combined_sentiment"] < 0, \
        x["nltk_combined_sentiment"],0)
    
    x["nltk_title_positive"] = np.where(x["nltk_title_sentiment"] > 0, \
        x["nltk_title_sentiment"],0)
    
    x["nltk_title_negative"] = np.where(x["nltk_title_sentiment"] < 0, \
        x["nltk_title_sentiment"],0)

    x["nltk_paragraph1_positive"] = np.where(x["nltk_paragraph1_sentiment"] \
        > 0, x["nltk_paragraph1_sentiment"],0)
    
    x["nltk_paragraph1_negative"] = np.where(x["nltk_paragraph1_sentiment"] \
        < 0, x["nltk_paragraph1_sentiment"],0)
    
    x["nltk_paragraph2_positive"] = np.where(x["nltk_paragraph2_sentiment"] \
        > 0, x["nltk_paragraph2_sentiment"],0)
    
    x["nltk_paragraph2_negative"] = np.where(x["nltk_paragraph2_sentiment"] \
        < 0, x["nltk_paragraph2_sentiment"],0)
    
    '''
    Alyien scores are quite different to those produced by NLTK and Watson. 
    There is a large band of neutral scores and the positive and negative
    scores are measures of confidence.
    
    We convert Aylien sentiment scores into ratings, split into positive and
    negative and ignore neutral. Assume neutral is equivalent to -0.2 to 0.2, 
    positive to > 0.2 and negative to < -0.2.
    '''
    
    #Adjust non-zero positve score from the range 0-1 to the range 0.2-1.
    x["aylien_positive"] = np.where(x["aylien_polarity"]=="positive", \
        x["aylien_confidence"]/0.8 + 0.2,0)
    
    # Adjust non-zero negative score from the range 0 to -1 to -0.2 to -1.
    x["aylien_negative"] = np.where(x["aylien_polarity"]=="negative", \
        x["aylien_confidence"]/-0.8 - 0.2,0)

    # As above for other Alyien scores
    x["aylien_title_positive"] = np.where(x["aylien_title_sentiment"]== \
        "positive", x["aylien_title_confidence"]/0.8 + 0.2,0)
    
    x["aylien_title_negative"] = np.where(x["aylien_title_sentiment"]== \
        "negative", x["aylien_title_confidence"]/-0.8 - 0.2,0)

    x["aylien_paragraph1_positive"] = \
        np.where(x["aylien_paragraph1_sentiment"]=="positive", \
        x["aylien_paragraph1_confidence"]/0.8 + 0.2,0)
    
    x["aylien_paragraph1_negative"] = \
        np.where(x["aylien_paragraph1_sentiment"]=="negative", \
        x["aylien_paragraph1_confidence"]/-0.8 - 0.2,0)

    x["aylien_paragraph2_positive"] = \
        np.where(x["aylien_paragraph2_sentiment"]=="positive", \
        x["aylien_paragraph2_confidence"]/0.8 + 0.2,0)
    
    x["aylien_paragraph2_negative"] = \
        np.where(x["aylien_paragraph2_sentiment"]=="negative", \
        x["aylien_paragraph2_confidence"]/-0.8 - 0.2,0)

    #print(x.head())
    
    '''
    Drop intermediary regressors
    ----------------------------
    
    Drop intermediary regressors used to create new regressors above, since
    they are no longer needed. 
    '''
    x =x.drop(["watson_score","nltk_combined_sentiment", \
    "nltk_title_sentiment","nltk_paragraph1_sentiment", \
    "nltk_paragraph2_sentiment", 
    "aylien_polarity", "aylien_confidence", \
    "aylien_paragraph1_sentiment", "aylien_paragraph1_confidence", \
    "aylien_paragraph2_sentiment", "aylien_paragraph2_confidence", \
    "aylien_title_sentiment", "aylien_title_confidence"], axis=1)

    '''
    Make regressors orthogonal
    --------------------------
    
    We can make the regressors orthogonal in order to make the regression more 
    robust and also to handle multi-collinearity. For example, two correlated 
    regressors x and y can be transformed into the pair x and x-y. On the 
    assumption that the various sentiment and emotion scores that input into 
    the model should be somewhat correlated to the ground truth sentiment of 
    the original article, we have decided to take one sentiment tool and make 
    all other independent variables orthogonal to it – either to its positive 
    or negative characterisation, depending on the independent variable. Since 
    NLTK is free, and therefore is likely to remain in the model despite any 
    changes in price or availability of other tools, we use its whole article 
    score for this purpose.

    Making regressors orthogonal does not improve R-squared or the standard 
    error (because the resulting linear model is equivalent to a linear model 
    using the original regressors, just with different parameters). However, 
    making the regressors independent does reduce their standard errors, and 
    is therefore more robust.
    
    We drop non-orthogonal regressors as we go along.    
    ''' 
    x["nc_watson_positive"] = x["nltk_combined_positive"]-x["watson_positive"]
    x =x.drop(["watson_positive"], axis=1)

    x["nc_nltk_title_positive"] = \
        x["nltk_combined_positive"]-x["nltk_title_positive"]
    x =x.drop(["nltk_title_positive"], axis=1)
    
    x["nc_nltk_paragraph1_positive"] = \
        x["nltk_combined_positive"]-x["nltk_paragraph1_positive"]
    x =x.drop(["nltk_paragraph1_positive"], axis=1)

    x["nc_nltk_paragraph2_positive"] = \
        x["nltk_combined_positive"]-x["nltk_paragraph2_positive"]
    x =x.drop(["nltk_paragraph2_positive"], axis=1)
    
    x["nc_aylien_positive"] = \
        x["nltk_combined_positive"]-x["aylien_positive"]
    x =x.drop(["aylien_positive"], axis=1)

    x["nc_aylien_title_positive"] = \
        x["nltk_combined_positive"]-x["aylien_title_positive"]
    x =x.drop(["aylien_title_positive"], axis=1)

    x["nc_aylien_paragraph1_positive"] = \
        x["nltk_combined_positive"]-x["aylien_paragraph1_positive"]
    x =x.drop(["aylien_paragraph1_positive"], axis=1)

    x["nc_aylien_paragraph2_positive"] = \
        x["nltk_combined_positive"]-x["aylien_paragraph2_positive"]
    x =x.drop(["aylien_paragraph2_positive"], axis=1)
    
    x["nc_joy"] = x["nltk_combined_positive"]-x["joy"]
    x =x.drop(["joy"], axis=1)

    x["nc_watson_negative"] = x["nltk_combined_negative"]-x["watson_negative"]
    x =x.drop(["watson_negative"], axis=1)

    x["nc_nltk_title_negative"] = \
        x["nltk_combined_negative"]-x["nltk_title_negative"]
    x =x.drop(["nltk_title_negative"], axis=1)
    
    x["nc_nltk_paragraph1_negative"] = \
        x["nltk_combined_negative"]-x["nltk_paragraph1_negative"]
    x =x.drop(["nltk_paragraph1_negative"], axis=1)

    x["nc_nltk_paragraph2_negative"] = \
        x["nltk_combined_negative"]-x["nltk_paragraph2_negative"]
    x =x.drop(["nltk_paragraph2_negative"], axis=1)
    
    x["nc_aylien_negative"] = x["nltk_combined_negative"]-x["aylien_negative"]
    x =x.drop(["aylien_negative"], axis=1)

    x["nc_aylien_title_negative"] = \
        x["nltk_combined_negative"]-x["aylien_title_negative"]
    x =x.drop(["aylien_title_negative"], axis=1)

    x["nc_aylien_paragraph1_negative"] = \
        x["nltk_combined_negative"]-x["aylien_paragraph1_negative"]
    x =x.drop(["aylien_paragraph1_negative"], axis=1)

    x["nc_aylien_paragraph2_negative"] = \
        x["nltk_combined_negative"]-x["aylien_paragraph2_negative"]
    x =x.drop(["aylien_paragraph2_negative"], axis=1)
    
    x["nc_anger"] = x["nltk_combined_negative"]-x["anger"]
    x =x.drop(["anger"], axis=1)

    x["nc_fear"] = x["nltk_combined_negative"]-x["fear"]
    x =x.drop(["fear"], axis=1)

    x["nc_sadness"] = x["nltk_combined_negative"]-x["sadness"]
    x =x.drop(["sadness"], axis=1)

    x["nc_disgust"] = x["nltk_combined_negative"]-x["disgust"]
    x =x.drop(["disgust"], axis=1)
  
    #print(x.head())
    
    y = df["mean"]
   
    '''
    Create set of possible regressor combinations
    ---------------------------------------------

    1. Parsimony in the maximum number of regressors
       ---------------------------------------------
    
    For the set of regressors, we take the union of k-combinations of the set, 
    where 1 <= k <= max(max_num_regressors,len(regressors))

    Parsimony is the idea that it is best to try and keep the number of 
    regressors in a model small. 
    
    Limiting the number of regressor combinations is parsimonious and speeds 
    up the code. With 23 regressors, there are about 8 million possible unique 
    subsets. If we limit subsets to length 11 or less, that cuts it to 
    4 million. With a limit of 8, it is 881,000 – nearly 10 times quicker.

    2. Exlcude specific regressors
       ---------------------------
    
    One may exclude regressors here, if any, with a lot of missing values
    We didn't include "nc_aylien_positive" and "nc_aylien_negative" for
    that reason but they could be included in the future depending on the 
    available data
    ''' 
    max_num_regressors = 10

    regressors = powerset_subsets(["nltk_combined_positive",\
    "nltk_combined_negative", "nc_watson_positive", "nc_nltk_title_positive", \
    "nc_nltk_paragraph1_positive", "nc_nltk_paragraph2_positive", \
    "nc_aylien_title_positive", \
    "nc_aylien_paragraph1_positive", "nc_aylien_paragraph2_positive", \
    "nc_joy", "nc_watson_negative","nc_nltk_title_negative", \
    "nc_nltk_paragraph1_negative", "nc_nltk_paragraph2_negative", \
    "nc_aylien_title_negative", "nc_aylien_paragraph1_negative", \
    "nc_aylien_paragraph2_negative","nc_anger", "nc_fear","nc_sadness", \
    "nc_disgust"],max_num_regressors)
    
    # Initialise adjusted R-squared and number of parameters for the current
    # best linear regression model.
    max_adjusted_r2 = 0
    k_max_adjusted_r2 = 0
    min_improvement_per_regressor = 0.01 # 0.01 is 1%
    for s in regressors:
        if s: 
            z = x.loc[:,s]
            max_adjusted_r2,k_max_adjusted_r2 = \
                calc_model_score("parameters.csv",z,y,model,max_adjusted_r2, \
                            k_max_adjusted_r2, min_improvement_per_regressor)
    
    '''
    Normality test for the dependent variable
    -----------------------------------------
    
    This test combines skew and kurtosis to produce an omnibus test of 
    normality. [D’Agostino, R. B. (1971), “An omnibus test of normality for 
    moderate and large sample size”, Biometrika, 58, 341-348]
    It is suitable for samples sizes > 50.
    '''
    omnibus_skew_kurtosis, p = normaltest(y)
    if p < 0.05:
        print('Ground truths not normally distributed.'); 
    
main()