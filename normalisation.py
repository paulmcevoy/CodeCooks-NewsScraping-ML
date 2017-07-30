#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 16 20:23:14 2017

@author: Paul
"""

from get_table_data import get_table_data
import json
import pandas as pd

df_art_table, df_ent_table, df_ent_table_norm, get_url_table = get_table_data()
from get_conn_info import get_conn_info

# Replaces the key in each (key, value) member of entities by a key in
# common_entities if the entities key is a member of the values corresponding
# to the common_entities key.
def renameEntities(entities, common_entities):
    if (entities is not None) and (common_entities is not None):
        for entity in entities:
            for key, value in common_entities.items():
                if entity[0] in value:
                    entity[0] = key
                    break
    return(entities)            

# Takes a list of entities as input.
# Remove enitities with duplicate names and adjust scores using the formula:
#   old scores: x, y
#   new score: 1 - (1-x)(1-y)
# If one entity name contains (but doesn't equal) another, then:
#   If the longer name is a key in common_entities is all upper case, 
#       then keep both. 
#   Otherwise: 
#       keep the shorter one only and adjust its score. 
#
# Returns a list of entities, ordered from shortest to longest name with
# no duplicates and no pair of entities where the name of one is contained in
# the name of the other unless the other is a key of common_entities or all
# upper case and different (i.e., possibly a different acronym)
def removeDuplicateEntities(entities, common_entities):
    #print(entities)
    sorted_entities = sorted(entities, key=lambda x: len(x[0]))
    #print(sorted_entities)
    no_duplicate_entities = []
    
    if (sorted_entities is not None):
        for entity in sorted_entities:
            match = False
            for unique_entity in no_duplicate_entities:
                if unique_entity[0] == entity[0]: 
                    unique_entity[1] = (1-(1-entity[1])*(1-unique_entity[1]))
                    match = True                  
                elif unique_entity[0] in entity[0] and not (
                entity[0].isupper()) and (                
                not (entity[0] in common_entities.keys())):
                    unique_entity[1] = (1-(1-entity[1])*(1-unique_entity[1]))
                    match = True
            if not (match):
                no_duplicate_entities.append(entity)

    return(no_duplicate_entities)
 
with open('common_entities.json', 'r') as f:
    common_entities = json.load(f)

from collections import  defaultdict
df_ents_current_dict = defaultdict(dict)
df_ents_cut_current_article_dict = defaultdict(dict)
df_ents_full = pd.DataFrame()
df_ents_current = pd.DataFrame()
 
#df_ent_table_date = df_ent_table[(df_ent_table.addedon == '15_07_2017') ]
df_ent_table_date = df_ent_table

df_ents_cut =  df_ent_table_date.loc[:,['article','score','name']]
for article in df_ents_cut.article.unique():
    #get it into a format that renameEntities and removeDuplicateEntities need
    df_ents_cut_current_article = df_ents_cut[(df_ents_cut.article == article) ]
    df_ents_cut_current_article_two = df_ents_cut_current_article.loc[:,['name', 'score']]
    entities = df_ents_cut_current_article_two.values.tolist()

    entities_rename = renameEntities(entities, common_entities)
    entities_rename_no_dups = removeDuplicateEntities(entities_rename, common_entities)
    df_ents = pd.DataFrame(entities_rename_no_dups)
    df_ents.columns = ['name', 'score']
    df_ents['article'] = article
    df_ents_full = df_ents_full.append(df_ents)

df_art_total =  df_art_table.loc[:,['uniqueid','entitynormalized']]
df_art_ent = df_art_total[df_art_total.entitynormalized == False]

conn, cursor = get_conn_info()
#df_art_ent = df_art_ent[:50]
loop_count = 0
total_articles = len(df_art_ent)
articles_normed = 0
for index, row in df_art_ent.iterrows():
    article = row['uniqueid']
    if(loop_count%100 == 0):
        print("{} of {} articles processed".format(loop_count, total_articles))
    df_ents_current = df_ents_full[df_ents_full.article == article]

    for index, ent_row in df_ents_current.iterrows():
        name = ent_row['name'] 
        score = ent_row['score'] 
        #print("Normalising article {} ent {} score {}".format(article, name, score))
        cursor.execute("INSERT INTO backend_entitiesnormalized (article, name, score) VALUES (%s, %s, %s)", (article, name, score))
    cursor.execute(" update backend_article set entitynormalized = (%s) where uniqueid =  (%s) ;", (True, article,))
    articles_normed+=1
    loop_count+=1

print("{} articles normalised".format(articles_normed))
        
conn.commit()
cursor.close()

