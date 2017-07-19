# -*- coding: utf-8 -*-
"""
Created on Sun Jul 16 20:23:14 2017

@author: Frank
"""

import json
#import psycopg2

# Replaces the key in each (key, value) member of entities by a key in
# common_entities if the entities key is a member of the values corresponding
# to the common_entities key.
def renameEntities(entities, common_entities):
    if (entities is not None) and (common_entities is not None):
        for entity in entities:
            for key, value in common_entities.iteritems():
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
    #else:
    #    print("sorted_entities is None")
    return(no_duplicate_entities)
 
def main():
    with open('common_entities.json', 'r') as f:
        common_entities = json.load(f)

    # Sample Data. To do: replace sample data with iteration through each 
    # article in the DB; read entities; and write back after processing with
    # renameEntities and removeDuplicateEntities.
    entities = [["President Trump",0.5], ["Mr Jones",0.4], ["Donald Trump",0.5], 
                [u"Jones",0.4], ["Jones",0.4], ["Merkel",0.4], 
                ["Chancellor Merkel",0.4], ["Angela",0.4]]
    print(entities)
    entities = renameEntities(entities, common_entities)
    print(entities)
    entities = removeDuplicateEntities(entities, common_entities)
    print(entities)
main()
