from celery.task import task
import os
@task
def post_process():
    os.system("./sentiment_process_nltk.py")
    os.system("./sentiment_process_aylien.py")
    os.system("./similarity_nltk.py")
    os.system("./normalisation.py")
    os.system("./summ.py")
