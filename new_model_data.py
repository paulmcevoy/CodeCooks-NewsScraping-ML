#!/usr/bin/python
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info
from get_model_data import get_mod_data
from get_table_data import get_art_table, get_ent_table, get_ent_norm_table, get_url_table

#get the table data
df_art_table = get_art_table()
df_mod_table, df_mod_table_simple_mean = get_mod_data()
df_mod_table_full = df_art_table[['uniqueid', 'watson_score', 'nltk_combined_sentiment', 'nltk_title_sentiment']]
df_mod_table_simple_mean['new_score'] = 0

#add weight spread
text_weights = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
#set defaults
best_rmse = 1
best_text_weight = 0

#loop through all weight ranges
for text_weight in text_weights:
	title_weight = 1 - 	text_weight

	for index, row in df_mod_table_simple_mean.iterrows():
	    if (row['nltk_combined_sentiment'] < 0 and row['watson_score'] < 0):
	        new_score = (text_weight*row['watson_score'] + title_weight*row['nltk_title_sentiment'])/2
	    elif (row['nltk_combined_sentiment'] > 0 and row['watson_score'] > 0):
	        new_score = (text_weight*row['nltk_combined_sentiment'] + title_weight*row['nltk_title_sentiment'])/2
	    else:
	        new_score = (text_weight* ((row['watson_score'] + row['nltk_title_sentiment'])/2)    +   title_weight*row['nltk_title_sentiment']) /2
	    df_mod_table_simple_mean.loc[index,'new_score'] = new_score

	new_rmse = ((df_mod_table_simple_mean.new_score - df_mod_table_simple_mean.userscore) ** 2).mean() ** .5
	#print(text_weight, title_weight, new_rmse)
	if new_rmse < best_rmse:
		best_text_weight = text_weight
		best_rmse = new_rmse

watson_score_rmse 				= ((df_mod_table_simple_mean.watson_score - df_mod_table_simple_mean.userscore) ** 2).mean() ** .5
nltk_combined_sentiment_rmse 	= ((df_mod_table_simple_mean.nltk_combined_sentiment - df_mod_table_simple_mean.userscore) ** 2).mean() ** .5
nltk_title_sentiment_rmse 		= ((df_mod_table_simple_mean.nltk_title_sentiment - df_mod_table_simple_mean.userscore) ** 2).mean() ** .5

print("Best RMSE {0:.3f} with text_weight {0:.1f}".format(best_rmse, best_text_weight))
print("Watson RMSE {0:.3f}".format(watson_score_rmse))
print("NLTK text RMSE {0:.3f}".format(nltk_combined_sentiment_rmse))
print("NLTK title RMSE {0:.3f}".format(nltk_title_sentiment_rmse))

best_title_weight = 1 - best_text_weight
conn, cursor = get_conn_info()
print("Sending (Weighted) model data to DB... ")
for index, row in df_mod_table_full.iterrows():
    if (row['nltk_combined_sentiment'] < 0 and row['watson_score'] < 0):
        new_score = (best_text_weight*row['watson_score'] + best_title_weight*row['nltk_title_sentiment'])/2
    elif (row['nltk_combined_sentiment'] > 0 and row['watson_score'] > 0):
        new_score = (best_text_weight*row['nltk_combined_sentiment'] + best_title_weight*row['nltk_title_sentiment'])/2
    else:
        new_score = (best_text_weight* ((row['watson_score'] + row['nltk_title_sentiment'])/2)    +   best_title_weight*row['nltk_title_sentiment']) /2

    cursor.execute(" update backend_sentiment set model_score = (%s) where article =  (%s) ;", (new_score, row['uniqueid'],))
    cursor.execute(" update backend_article set modelprocessed = (%s) where uniqueid =  (%s) ;", (True, row['uniqueid'],))

print("Model data done")
conn.commit()
cursor.close()
conn.close()
