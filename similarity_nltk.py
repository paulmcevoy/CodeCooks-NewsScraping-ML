# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:58:42 2017

@author: paul
"""
import pandas as pd
import nltk 
import json
from get_clean_table_data import get_clean_table_data
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

df_art_table, df_ent_table, df_ent_table_norm = get_clean_table_data()

df_ent_table_date = df_art_table[(df_art_table.publishedat == '19_07_2017') | (df_art_table.publishedat == '18_07_2017') | (df_art_table.publishedat == '17_07_2017') ]
df_ents_data_set = set(df_ent_table_date['publishedat'])
df_art_titles =  df_ent_table_date.loc[:,['uniqueid','title','publishedat']]

#df_art_titles = df_art_titles[:200]

from collections import  defaultdict
df_ents_sim_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
for date in df_ents_data_set:
    df_art_titles_date = df_art_titles[df_art_titles.publishedat == date]
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
    
    for key1, value1 in ents_dict.items():
        for key2, value2 in ents_dict.items():
            sim_num = len(set(value1) & set(value2))
            sim_vals = (set(value1) & set(value2))
            if sim_num > 0 and key1 != key2:
                print(sim_num, sim_vals)
                df_ents_sim_dict[key1][key2] = sim_num
                df_ents_sim = df_ents_sim.append(pd.DataFrame({'article1': key1, 'article2': key2, 'sim_count': sim_num, 'entities': sim_vals}, index=[0]), ignore_index=True)
