from celery.task import task
import os
@task
def post_process():
    os.system("python3.6 ./sentiment_process_nltk.py")
    os.system("python3.6 ./sentiment_process_aylien.py")
    os.system("python3.6 ./similarity_nltk.py")
    os.system("python3.6 ./normalisation.py")
    os.system("python3.6 ./summ.py")
    os.system("python3.6 ./parse_flags.py")
