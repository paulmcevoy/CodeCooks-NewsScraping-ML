from celery.task import task
import os
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

"""
This is the Python file that calls all of the individual post process scripts
"""

@task
def post_process():
    logger.info("Post processing start")
    os.system("python3.6 ./normalisation.py")
    os.system("python3.6 ./sentiment_process_nltk.py")
    os.system("python3.6 ./sentiment_process_aylien.py")
    os.system("python3.6 ./similarity_nltk.py")
    os.system("python3.6 ./summ.py")
    os.system("python3.6 ./top_ents.py")
    os.system("python3.6 ./parse_flags.py")
    os.system("python3.6 ./new_model_data_weighted_mean.py")
    logger.info("Post processing end")
