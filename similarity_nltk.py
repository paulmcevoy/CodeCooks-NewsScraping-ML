#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:58:42 2017

@author: paul
"""
import pandas as pd
import nltk 
import json
from get_table_data import get_art_table, get_ent_table, get_ent_norm_table, get_url_table
from get_conn_info import get_conn_info
import datetime
def extract_entity_names(t):
    entity_names = []

    if hasattr(t, 'label') and t.label:
        if t.label() == 'NE':
            entity_names.append(' '.join([child[0] for child in t]))
        else:
            for child in t:
                entity_names.extend(extract_entity_names(child))

    return entity_names

def renameEntitiesNoScore(entities, common_entities):
    if (entities is not None) and (common_entities is not None):
        for i, entity in enumerate(entities):
            for key, value in common_entities.items():
                if entity in value:
                    entities[i] = key
                    break
    return(entities)            

def removeDuplicateEntitiesNoScore(entities, common_entities):
    #print(entities)
    sorted_entities = sorted(entities)
    #print(sorted_entities)
    no_duplicate_entities = []
    
    if (sorted_entities is not None):
        for entity in sorted_entities:
            match = False
            for unique_entity in no_duplicate_entities:
                if unique_entity == entity: 
                    match = True                  
                elif unique_entity in entity and not (
                entity.isupper()) and (                
                not (entity in common_entities.keys())):
                    match = True
            if not (match):
                no_duplicate_entities.append(entity)

    return(no_duplicate_entities)
 
with open('common_entities.json', 'r') as f:
    common_entities = json.load(f)

df_art_table = get_art_table()

todays_date =  datetime.date.today()
df_ent_table_date = df_art_table[df_art_table.addedondt == todays_date]

df_ents_data_set = set(df_ent_table_date['addedon'])
df_art_titles =  df_ent_table_date.loc[:,['uniqueid','title','addedon']]

#df_art_titles = df_art_titles[:200]
from collections import Counter
from collections import  defaultdict
df_ents_sim_dict = defaultdict(dict)
ents_top_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
#df_art_ent = df_art_total[df_art_total.entitynormalized == False]

for date in df_ents_data_set:
    sim_each_list = []
    each_ent_list = []
    df_art_titles_date = df_art_titles[df_art_titles.addedon == date]
    ents_dict = defaultdict(dict)

    for index, row in df_art_titles_date.iterrows():
        ent_list = []
        each_ent_noscore = []
        sentences = nltk.sent_tokenize(row['title'])
        tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
        tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
        chunked_sentences = nltk.ne_chunk_sents(tagged_sentences, binary=True)
      
        entity_names = []
        for tree in chunked_sentences:
            entity_names.extend(extract_entity_names(tree))
            
        entities_rename = renameEntitiesNoScore(entity_names, common_entities)
        entities_rename_no_dups = removeDuplicateEntitiesNoScore(entities_rename, common_entities)    
        #entities_rename_no_dups = set(entities_rename)    
        uniqueid = row['uniqueid']
        if len(entities_rename_no_dups) != 0:
            ents_dict[uniqueid]= entities_rename_no_dups
            for each_ent in entities_rename_no_dups:
                each_ent_list.append(each_ent)   
    """
    for key1, value1 in ents_dict.items():
        for key2, value2 in ents_dict.items():
            sim_num = len(set(value1) & set(value2))
            sim_vals = (set(value1) & set(value2))
            if sim_num > 0 and key1 != key2:
                #print(sim_num, sim_vals)
                df_ents_sim_dict[key1][key2] = sim_num
                df_ents_sim = df_ents_sim.append(pd.DataFrame({'article1': key1, 'article2': key2, 'sim_count': sim_num, 'entities': sim_vals}, index=[0]), ignore_index=True)
                for sim_each in sim_vals:
                    sim_each_list.append(sim_each)    
    """                
    ent_id_most_common_dict = defaultdict(dict)                
    ents_count = Counter(each_ent_list)
    ents_count_most_common = ents_count.most_common(10)
    most_common_list = [i[0] for i in ents_count_most_common]
    #for ent in most_common_list:
    for idx, ent in enumerate(most_common_list):
        ent_id_most_common_dict[ent] = idx +1
        
    ents_top_dict = defaultdict(dict)
    ents_id_dict = defaultdict(dict)

    for each_top_ent in reversed(most_common_list):
        #print("Doing ent {}".format(each_top_ent))
        for article, article_ents in ents_dict.items():
            #print("Doing article {}".format(article))
            if each_top_ent in article_ents:
                #print(article, article_ents, each_top_ent)
                ents_top_dict[article] = each_top_ent
   
    #print("Most common entities for: {}\n{}".format(date, ents_count_most_common))
    for art, ent in ents_top_dict.items():
        ents_id_dict[art] = ent_id_most_common_dict[ent]

    conn, cursor = get_conn_info()
     
    
    loop_count = 0
    total_articles = len(df_art_titles_date)
    articles_grouped = 0
    for article, id_val in ents_id_dict.items():
    #for index, row in df_art_titles_date.iterrows():

        #print(article)
        #if(loop_count%100 == 0):
        #    print("{} of {} articles grouped".format(loop_count, total_articles))
        #df_ents_current = df_ents_full[df_ents_full.article == article]
            #print("Normalising article {} ent {} score {}".format(article, name, score))
        cursor.execute(" update backend_article set similaritygroupid = (%s) where uniqueid =  (%s) ;", (id_val , article,))
        articles_grouped+=1
        loop_count+=1
    
    print("{} articles grouped from total {}".format(articles_grouped, len(df_art_titles_date) ))
    conn.commit()
    cursor.close()
