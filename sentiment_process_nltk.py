#!/usr/bin/python3.6

import re
import nltk
import math
from get_conn_info import get_conn_info

from get_table_data import get_table_data

df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()
#df_table_date = df_art_table[df_art_table.addedon == '18_07_2017']
#df_table_date = df_table_date[50:150]
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

sid = SIA()
import unicodedata
#for index, row in df_table_date.iterrows():
    #df_table_date.loc[index, 'cleaned_text'] = re.sub('\\','', row['text']) 
    #print( df_table_date.loc[index, 'cleaned_text'])
    #df_table_date.loc[index, 'cleaned_text'] = row['text'] 
    #df_table_date.loc[index, 'cleaned_text'] = unicodedata.normalize("NFKD", row['text'])
    #print("Old str:\n {}\n".format(row['text']))
    #print("New str:\n {}\n".format(new_str))


df_table_date_nltk = df_art_table[df_art_table.nltk_sentiment == False]
df_table_date_nltk_set = set(df_table_date_nltk['addedon'])

print(len(df_art_table))
total_articles = len(df_table_date_nltk)
print("Num articles to process: {}".format(total_articles))
loop_count = 0
for date in df_table_date_nltk_set:
    conn, cursor = get_conn_info()
    df_table_date_nltk_one_day = df_table_date_nltk[(df_table_date_nltk.addedon == date) ] 
    
    for index, row in df_table_date_nltk_one_day.iterrows():
        if(loop_count%10 == 0 | loop_count == total_articles):
            print("{} of {} articles processed".format(loop_count, total_articles))
            
        lines_list = tokenizer.tokenize(row['text'])
        title = tokenizer.tokenize(row['title'])
        num_sents = len(lines_list)
        section_length = math.floor(num_sents/2)
        section1 = lines_list[:len(lines_list)//2]    
        section2 = lines_list[(-len(lines_list)//2):] 
        sfull = 0
        stitle = 0
        s1 = 0
        s2 = 0
        #print("section1 {}\n section2{}\n length{}\n\n".format(section1, section2,section_length ))
        for sentence in lines_list:
            sfull += sid.polarity_scores(sentence)['compound']
        sfull_norm = sfull/num_sents
        df_table_date_nltk.loc[index, 'nltk_polarity'] = sfull_norm
        for sentence in title:
            stitle+= sid.polarity_scores(sentence)['compound']
        df_table_date_nltk.loc[index, 'nltk_polarity_title'] = stitle
        #print("--------Section1--------")
        for sentence in section1:
            s1 += sid.polarity_scores(sentence)['compound']
        #    print("{}\nPolarity {}\n\n".format(sentence, sid.polarity_scores(sentence)['compound']))
        s1_norm = s1/section_length
        df_table_date_nltk.loc[index, 'nltk_polarity_section1'] = s1_norm
        #print("--------Section2--------")
        for sentence in section2:
            s2 += sid.polarity_scores(sentence)['compound']
        #    print("{}\nPolarity {}\n".format(sentence, sid.polarity_scores(sentence)['compound']))
        s2_norm = s2/section_length
        df_table_date_nltk.loc[index, 'nltk_polarity_section2'] = s2_norm
        #print("\n--------TITLE--------")
        #print("{}\n".format(title))
        #print("NLTK:\n{}\nTitle {}\nTotal: {}\nSection1: {}\nSection2: {}\n\n"\
        #      .format(row['uniqueid'], stitle, sfull_norm, s1_norm, s2_norm))            
        
        uniqueid = row['uniqueid']
        cursor.execute(" update backend_sentiment set nltk_combined_sentiment = (%s) where article =  (%s) ;", (sfull_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set nltk_paragraph1_sentiment = (%s) where article =  (%s) ;", (s1_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set nltk_paragraph2_sentiment = (%s) where article =  (%s) ;", (s2_norm, uniqueid,))
        cursor.execute(" update backend_sentiment set nltk_title_sentiment = (%s) where article =  (%s) ;", (stitle, uniqueid,))
        cursor.execute(" update backend_article set nltk_sentiment = (%s) where uniqueid =  (%s) ;", (True, uniqueid,))
        loop_count+=1
    
    conn.commit()
    cursor.close()



#df_short =  df_table_date_nltk.loc[:,['uniqueid','url','score','nltk_polarity', 'nltk_polarity_title', 'nltk_polarity_section1', 'nltk_polarity_section2', 'aylien_polarity']]
#df_short.to_csv('df_short_nltk.csv')



