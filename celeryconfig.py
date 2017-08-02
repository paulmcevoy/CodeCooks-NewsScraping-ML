CELERY_IMPORTS = ('tasks')
CELERY_IGNORE_RESULT = False
BROKER_HOST = "127.0.0.1" #IP address of the server running RabbitMQ and Celery
BROKER_PORT = 5672
BROKER_URL='amqp://'
CELERY_RESULT_BACKEND = "amqp"
CELERY_IMPORTS=("tasks",)

from celery.schedules import crontab
 
CELERYBEAT_SCHEDULE = {
    'every-10mins': {
        'task': 'tasks.post_process',
        'schedule': crontab(minute='*/10'),
        #'args': (1,2),
    },
}
