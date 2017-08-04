
# coding: utf-8

# In[ ]:


from datetime import datetime
import pandas as pd
from get_table_data import get_table_data
import psycopg2
import pandas as pd
import pandas.io.sql as sql
from get_conn_info import get_conn_info
from get_model_data import get_mod_data
from sentiment_process_aylien import get_aylien

conn, cursor = get_conn_info()
df_rand_table = sql.read_sql("SELECT backend_article.uniqueid, \
                            backend_article.publishedat, \
                            backend_article.url, \
                            backend_article.title, \
                            backend_article.source, \
                            backend_article.aylien_sentiment_adv, \
                            backend_sentiment.watson_score, \
                            backend_sentiment.aylien_polarity, \
                            backend_sentiment.aylien_confidence, \
                            backend_sentiment.aylien_paragraph1_sentiment, \
                            backend_sentiment.aylien_paragraph2_sentiment, \
                            backend_sentiment.aylien_paragraph1_confidence, \
                            backend_sentiment.aylien_paragraph2_confidence, \
                            backend_sentiment.aylien_title_confidence, \
                            backend_sentiment.aylien_title_sentiment, \
                            backend_sentiment.nltk_paragraph1_sentiment, \
                            backend_sentiment.nltk_paragraph2_sentiment, \
                            backend_sentiment.nltk_title_sentiment, \
                            backend_sentiment.nltk_combined_sentiment, \
                            backend_article.text \
                            FROM \
                            backend_article \
                            INNER JOIN \
                            backend_sentiment \
                            ON backend_article.uniqueid = backend_sentiment.article;"\
                            , conn) 
conn.commit()
cursor.close()    

"""
uniqueid_list = ['4091936264','3663702422','3477021980','2492906808','3759153076',
'1443587108','4268280762','3644997503','472468008','674348952',
'2068681099','2773781345','1591193490','494229333','1682990526',
'3671399665','519745677','2985015841','750820626','4131172770',
'666837715','3626661759','1319004404','2480785593', '557164040',
'1945789018','3948403476','2391760743','4083377876','300775108',
'733514295','2736777008','1414045729','2635073589','1891083304', 
'2041568154', '1620552689', '3534750741', 
'1127862072', '1903043327', '2305194430', '3284907296', 
'42737988', '2186751004', '3195102008']
"""
uniqueid_list = ['1891083304', 
'2041568154', '1620552689', '3534750741', 
'1127862072', '1903043327', '2305194430', '3284907296', 
'42737988', '2186751004', '3195102008']

#uniqueid_list = [4091936264, 3663702422,3477021980,2492906808,3759153076,1443587108,4268280762,3644997503, \
#                472468008,674348952,2068681099,2773781345,1591193490,494229333,1682990526, \
#                 3671399665,519745677,2985015841,750820626,4131172770,666837715,3626661759, \
#                 1319004404,773454877,2480785593]

df_mod_table = get_mod_data()

#uniqueid_list = df_mod_table.articleid.tolist()
#uniqueid_list_set = set(uniqueid_list)

df_rand_table_sample_full = pd.DataFrame()

for eachid in uniqueid_list:
    #print(eachid)
    df_rand_table_sample = df_rand_table[(df_rand_table.uniqueid == str(eachid)) ] 
    df_rand_table_sample_full = df_rand_table_sample_full.append(df_rand_table_sample)

df_rand_table_sample_full_cut = df_rand_table_sample_full[df_rand_table_sample_full.aylien_sentiment_adv == False]
#df_rand_table_sample_full_cut = df_rand_table_sample_full

get_aylien(df_rand_table_sample_full_cut)

#df_rand_table_for_csv = df_rand_table.drop('text',1)
#df_rand_table_for_csv.to_csv('df_rand_table_for_csv.csv')
    
#df_rand_table_sample_full_for_csv = df_rand_table_sample_full.drop('text',1)
#df_rand_table_sample_full_for_csv.to_csv('df_rand_table_sample_full_for_csv.csv')

