normalisation.py takes article entities and a common_entities.json file and does the following:

For all articles: 
If an entity name is a value in common_entities.json, it is replaced with the key.

For each article:
If an entity name is a substring of another entity name for the same article, the longer name is replaced by the shorter name unless the longer name is a key in common_entities.json.

If an article has two or more entities with the same name, then the duplicates are replaced and the score of the remaining entity is adjusted.
(These 2 tasks are done together because entities names that are the same are substrings of one another.)

When replacing two entities with a single entity representing both, the following adjustment is made to the score:
original scores: x, y
new scores: 1 - (1 - x)*(1-y).

Notes:
This formula is commutative in the case of triplicates, etc. I.e., it does not matter in which order the scores are adjusted.
if y = 0, the new score is x.
if y = 1, the new score is 1, i.e, y.