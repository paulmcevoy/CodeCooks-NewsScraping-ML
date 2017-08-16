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
from pympler import tracker
import datetime
#debug to track memory leaks
#tr = tracker.SummaryTracker()


"""
This file does the comparison of articles and assigns an ID when they form a group

This is not a deep comparison, it just aims to keep the stories that are mostly likely similar apart.

This is done as follows:
1. Clear all the group previous group IDs from the DB. New stories will have appeared and we want to discover new groups, if they exist
1. A day's worth of articles is extracted
2. A list is created of all of the entities in the article titles for that day
3. The entities are normalised for each title to ensure no duplicates in the title
4. This list counts the top 10 entities with most occurences
5. This top 10 list is given an ID 0-9
6. Each article title (for that day) is then checked if it has an entity from that list in its title, if so it gets the corresponding ID
7. The loop continues until all articles for that day have been checked, it then moves onto the next day but the new ID values are offset by 10

The result at the end of this have a UNIQUE ID to indicate they form part of a "group". If they have no idea they belong to no group (other 
than themselves)

The fact that the IDs are unique across all days is important. Originally each day had articles with IDs 0-9. However if the sentiment slider
was used in such a way that stories from multiple days appeared on the front-page we had an issue where ID3 from day 4 would be kept away from 
an article that had ID3 but from day 5, when in reality they were completely separate stories, as they were from separate days and could be safely
included side-by-side

Keeping the group IDs completely across all dates means we are completely agnostic to what day they are from, but we still know that articles with
the same ID are similar and need to be kept apart.

A number of approaches to the problem were made but this was the simplest. For instance we considered (and tried) doing some clustering on the articles 
based on the text or entities. However this clustering was overkill since we weren't looking for really robust article similarity but more for way to 
keep the really similar articles apart. Instead the decision to move to titles only was made for the following reasons:
    1. Just checking the titles is much quicker than checking all of the text for each article
    2. The solution worked really well in general use.

These 3 functions are simplification of what is in normalisation.py

extract_entity_names - Extracts the names of the entities from 

renameEntitiesNoScore - Same as in normalisation.py, but without the score. Replaces the key in each (key, value) member of entities by a key in
common_entities if the entities key is a member of the values corresponding to the common_entities key.

removeDuplicateEntitiesNoScore -  Returns a list of entities, ordered from shortest to longest name with
no duplicates and no pair of entities where the name of one is contained in
the name of the other unless the other is a key of common_entities or all
upper case and different (i.e., possibly a different acronym)

"""


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

df_ent_table_date = df_art_table
df_ents_data_set = sorted(set(df_ent_table_date['addedondt']))
df_art_titles =  df_ent_table_date.loc[:,['uniqueid','title','addedondt']]

#df_art_titles = df_art_titles[:200]
from collections import Counter
from collections import  defaultdict
df_ents_sim_dict = defaultdict(dict)
ents_top_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])


#global offset is added to every ID to ensure they are unique across
#each iteration of the the loop
global_offset = 0

#clean the groups
conn, cursor = get_conn_info()
cursor.execute(" update backend_article set similaritygroupid = null;")
conn.commit()
cursor.close()
conn.close()

#start the log file
print(datetime.date.today(), file=open("groups.txt", "w"))


#This loop runs for each set of articles in the DB from the same day
#We are interested in looking at stories from each day and grouping accordingly
for date in df_ents_data_set:
    each_ent_list = []
    #get the articles for the date we are interested in
    df_art_titles_date = df_art_titles[df_art_titles.addedondt == date]
    ents_dict = defaultdict(dict)

    #process the titles
    for index, row in df_art_titles_date.iterrows():
        sentences = nltk.sent_tokenize(row['title'])
        tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
        tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
        chunked_sentences = nltk.ne_chunk_sents(tagged_sentences, binary=True)
      
        entity_names = []
        for tree in chunked_sentences:
            entity_names.extend(extract_entity_names(tree))
        
        #Rename the articles in case they have a variations from the JSON dictionary    
        entities_rename = renameEntitiesNoScore(entity_names, common_entities)
        entities_rename_no_dups = removeDuplicateEntitiesNoScore(entities_rename, common_entities)    
 
        uniqueid = row['uniqueid']
        if len(entities_rename_no_dups) != 0:
            ents_dict[uniqueid]= entities_rename_no_dups
            #add each normalised ent to a list
            for each_ent in entities_rename_no_dups:
                each_ent_list.append(each_ent)   
    

    ent_id_most_common_dict = defaultdict(dict)                
    #Count the entities in the list
    ents_count = Counter(each_ent_list)
    #take the top 10
    ents_count_most_common = ents_count.most_common(10)
    #ceate a list from that top 10
    most_common_list = [i[0] for i in ents_count_most_common]
    
    for idx, ent in enumerate(most_common_list):
        #create a dict with the ID value plus some global_offset
        ent_id_most_common_dict[ent] = idx + global_offset

    #update the global offset by the lenght of the amount we have taken
    global_offset+=len(most_common_list)

    ents_top_dict = defaultdict(dict)
    ents_id_dict = defaultdict(dict)

    for each_top_ent in reversed(most_common_list):
        #print("Doing ent {}".format(each_top_ent))
        for article, article_ents in ents_dict.items():
            #print("Doing article {}".format(article))
            #If we find one of the top ents in this article's title then we assign that article with the ID
            if each_top_ent in article_ents:
                #print(article, article_ents, each_top_ent)
                ents_top_dict[article] = each_top_ent
   
    print(date, ents_count_most_common, file=open("groups.txt", "a"))

    for art, ent in ents_top_dict.items():
        ents_id_dict[art] = ent_id_most_common_dict[ent]

    #Just for debug, sometimes its nice to see what it grouped
    group_list = []
    for k,v in ents_id_dict.items():
        #print(k,v)
        group_list.append(v)
    
    conn, cursor = get_conn_info()

    loop_count = 0
    total_articles = len(df_art_titles_date)
    articles_grouped = 0

    #Finally, set the ID for any articles that were awarded one
    for article, id_val in ents_id_dict.items():
        cursor.execute(" update backend_article set similaritygroupid = (%s) where uniqueid =  (%s) ;", (id_val , article,))       
        articles_grouped+=1
        loop_count+=1
    
    #debug to track memory leaks
    #tr.print_diff()
    conn.commit()
    cursor.close()
    conn.close()

print("Grouping Done. Group details have been output to groups.txt")
