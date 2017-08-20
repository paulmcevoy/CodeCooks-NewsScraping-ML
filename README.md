# CodeCooks-Repo-ML

##### What this repo is for:

This repo handles most of the post-processing (including model scoring) done on the articles after they have been scraped

##### Why do we need it:

Gettting the article text and initial sentiment data is only the first step in a sentiment solution for the user.
We need advanced sentiment information. We also need to find article similarity, summarise and normalise entities.
Finally we need to determine a new sentiment value using a model

##### What other project resources are important to it:

* PostgreSQL is the main source and destination for the data used in this repo.
* The data it uses comes from the information [codecooks-watsonapi Repo](https://github.com/ucd-nlmsc-teamproject/codecooks-watsonapi) has sent to the DB.
* The data it updates to the DB is primarily used by [CodeCooks-DJango Repo](https://github.com/ucd-nlmsc-teamproject/CodeCooks-DJango)

### Resources use for this repo:

 * [NLTK-Vader](https://github.com/cjhutto/vaderSentiment) for NLTK sentiment analysis
 * [Sumy](https://pypi.python.org/pypi/sumy) for article summarisation
 * [Aylien API](https://developer.aylien.com/getting-started/python) for sentiment more analysis
 * [Celery](http://www.celeryproject.org/) for job scheduling
 * [psycopg]( http://initd.org/psycopg/) for SQL requests, updates and inserts

In summary the following steps are carried out at configurable intervals (defaulted to 30 minutes):

	1. Entity Normalisation (normalisation.py)
	2. NLTK Sentiment  (sentiment_process_nltk.py)
	3. Aylien Sentiment (sentiment_process_aylien.py)
	4. Article Similarity (similarity_nltk.py)
	5. Summarisation (summ.py)
	6. Top Entities (top_ents.py)
	7. Parse flags (parse_flags.py)
	8. Update model scores (new_model_data_weighted_mean.py)


1. **Entity Normalisation** (normalisation.py)
    1. Extracts all the articles from today, just to reduce overhead
    2. For each article it extracts the (un-normalised) entities already set from WatsonAPI
    3. These entities are then renamed if they have a 'key' value from the JSON list
    4. Any duplicates are removed and scores are combined where necessary, more detail below
    5. Finally the normalised entities are sent back to the DB with linked to the article they relate to

2. **NLTK Sentiment** (sentiment_process_nltk.py)
    1. Gets the article data from the DB
    2. Gets articles that have not been analyzed yet
    3. Splits the articles into 1st and 2nd half using NLTK tokenizer
    4. Gets the sentiment using NLTK for Title, 1st and 2nd sections
    5. Sends the sentiments back to the DB and sets the nltk_sentiment flag

3. **Aylien Sentiment** (sentiment_process_aylien.py)
    1. Gets the article data from the DB
    2. Gets articles after 31st July, due to Unit limits
    3. Gets articles that have not been analyzed yet
    4. Splits the articles into 1st and 2nd half using NLTK tokenizer
    5. Gets the sentiment polarity and confidence from Aylien for Title, 1st and 2nd sections

4. **Article Similarity** (similarity_nltk.py)
    1. Clear all the group previous group IDs from the DB. New stories will have appeared and we want to discover new groups, if they exist
    2. A day's worth of articles is extracted
    3. A list is created of all of the entities in the article titles for that day
    4. The entities are normalised for each title to ensure no duplicates in the title
    5. This list counts the top 10 entities with most occurences
    6. This top 10 list is given an ID 0-9
    7. Each article title (for that day) is then checked if it has an entity from that list in its title, if so it gets the corresponding ID
    8. The loop continues until all articles for that day have been checked, it then moves onto the next day but the new ID values are offset by 10

5. **Summarisation** (summ.py)

	This file uses the Python package 'sumy' to extract the top 5 sentences that represent
  a summary of the article. These are then used as a tooltip when the article is displayed
  on the site
  This program only analyses the articles that have not been summarized by checking the 
  'sumanalyzed' flag. This is to reduce runtime as the summarization can take a few seconds 
  for each article

6. **Top Entities** (top_ents.py)
    1. Gets all of the entities from the last 24 hours
    2. Sum all of the scores for that entity as an indication of entity strength
    3. Multiply the scores by the sentiment of the articles they appear in
    4. Gets a top 5 negative and positive from that and sends it to the DB

7. **Parse flags** (parse_flags.py)

	This program checks the flags set for the sentiment scores and sets an overall flag 'sentiment_analyzed' that
    indicates that article has been fully analyzed and is ready to get a new model score

8.  **Update model scores** (new_model_data_weighted_mean.py)

	This file gets all the user ratings from the evaluation site and calculates the best weighting
    for the headline and content based on the RMSE, then sends the new model score to the DB
    
    The new (weighted) model works as follows:
    NLTK has far better accuracy in determing if a story is positive
    Watson has far better accuracy in determing if a story is negative
    
    1. If there is positive agreement NLTK score is used
       If there is negative agreement Watson score is used
       If there is no agreement then the mean of the scores is used
    2. Final score is the mean of 30% the NLTK on the Title and 70% the score from #2


```

Using this REPO (to test)
    git clone https://github.com/ucd-nlmsc-teamproject/CodeCooks-Repo-ML.git

Using this REPO (for development)
    From https://github.com/ucd-nlmsc-teamproject/CodeCooks-Repo-ML fork this project
    Then cd your_project_directory
    git clone git@github.com:<your_user_name>/CodeCooks-Repo-ML.git
    cd CodeCooks-Repo-ML
    git remote -v
    git remote add --track master upstream git@github.com:ucd-nlmsc-teamproject/CodeCooks-Repo-ML.git
    git remote -v 
    git remote set-url --push upstream DISABLED 
    git remote -v 
```
