# -*- coding: utf-8 -*-
"""
Created on Sun Jul 16 20:23:14 2017

@author: Frank
"""
from get_clean_table_data import get_clean_table_data
import json
import pandas as pd
import psycopg2
df_art_table, df_ent_table = get_clean_table_data()

# Replaces the key in each (key, value) member of entities by a key in
# common_entities if the entities key is a member of the values corresponding
# to the common_entities key.
def renameEntities(entities, common_entities):
    if (entities is not None) and (common_entities is not None):
        for entity in entities:
            for key, value in common_entities.items():
                if entity[0] in value:
                    entity[0] = key
                    print(value, key)
                    #print("Did rename on:\n{}\n".format(entity[0]) )
                    break
    return(entities)            

# Takes a list of entities as input.
# If one entity name contains (or equals) another, then:
# if the longer name is a key in common_entities, then keep both
# otherwise: 
#    keep one (the shorter, if different) and adjust the score (upwards) 
#    for the entity kept. 
#
# Returns a list of entities, ordered from shortest to longest name with
# no duplicates and no pair of entities where the name of one is contained in
# the name of the other unless the other is a key of common_entities
def removeDuplicateEntities(entities, common_entities):
    #print(entities)
    sorted_entities = sorted(entities, key=lambda x: len(x[0]))
    #print(sorted_entities)
    no_duplicate_entities = []
    
    if (sorted_entities is not None):
        for entity in sorted_entities:
            match = False
            for unique_entity in no_duplicate_entities:
                if unique_entity[0] in entity[0] and (
                (unique_entity[0] in common_entities.keys()) or 
                not (entity[0] in common_entities.keys())):
                    unique_entity[1] = (1-(1-entity[1])*(1-unique_entity[1]))
                    match = True
                    #print("Found duplicate of:\n{}\n{}\n".format(entity[0], unique_entity[0]) )
            if not (match):
                no_duplicate_entities.append(entity)
    #else:
    #    print("sorted_entities is None")
    return(no_duplicate_entities)
 
with open('common_entities.json', 'r') as f:
    common_entities = json.load(f)

from collections import  defaultdict
df_ents_current_dict = defaultdict(dict)
df_ents_cut_current_article_dict = defaultdict(dict)
df_ents_full = pd.DataFrame()

df_ent_table_date = df_ent_table[(df_ent_table.publishedat == '15_07_2017') ]
#df_ent_table_date = df_ent_table

df_ents_cut =  df_ent_table_date.loc[:,['article','score','name']]
for article in df_ents_cut.article.unique():
    df_ents_cut_current_article = df_ents_cut[(df_ents_cut.article == article) ]
    df_ents_cut_current_article_two = df_ents_cut_current_article.loc[:,['name', 'score']]
    entities = df_ents_cut_current_article_two.values.tolist()

    entities_rename = renameEntities(entities, common_entities)
    entities_rename_no_dups = removeDuplicateEntities(entities_rename, common_entities)
    table = [[1 , 2], [3, 4]]
    df_ents = pd.DataFrame(entities_rename_no_dups)
    #df = df.transpose()
    df_ents.columns = ['name', 'score']
    df_ents['article'] = article
    df_ents_full = df_ents_full.append(df_ents)

#df_ents_data_set = set(df_ents_full['publishedat'])
# Sample Data. To do: replace sample data with iteration through each 
# article in the DB; read entities; and write back after processing with
# renameEntities and removeDuplicateEntities.
#entities = [["President Trump",0.5], ["Mr Jones",0.4], ["Donald Trump",0.5], 
#            [u"Jones",0.4], ["Jones",0.4], ["Merkel",0.4], 
#            ["Chancellor Merkel",0.4], ["Angela",0.4]]

#insert into backend_entitiesnormalized (article,name,score) values (‘1010917238’,‘Trump’,‘0.80’);

conn_string = "host='codecooks.ftp.sh' dbname='newsapp' user='postgres' password='codepostgrescook'"
print ("Connecting to database\n    ->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

df_ents_full_short = df_ents_full[:20]
for index, row in df_ents_full_short.iterrows():
    #print(row['sumanalyzed'])
    #if(row['sumanalyzed']):
    #this means we have already sum'd it       
    #    print("Already analysed article {}".format(uniqueid))
    #    continue
    #else:
    article = row['article']
    name = row['name'] 
    score = row['score'] 

    cursor.execute("INSERT INTO backend_entitiesnormalized (article, name, score) VALUES (%s, %s, %s)", (article, name, score))
    
conn.commit()
cursor.close()