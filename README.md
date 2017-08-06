# CodeCooks-Repo-ML

Machine Learning Code
```

1. Entity Normalisation
2. Article Similarity 
3. NLTK Sentiment 
4. Aylien Sentiment
5. Summarisation
6. Parse flags 
7. Update model scores

The new (weighted) model works as follows:

NLTK has far better accuracy in determing if a story is positive
Watson has far better accuracy in determing if a story is negative

1. If there is positive agreement NLTK score is used
   If there is negative agreement Watson score is used
   If there is no agreement then the mean of the scores is used
2. Final score is the mean of 30% the NLTK on the Title and 70% the score from #2



Using this REPO 

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
