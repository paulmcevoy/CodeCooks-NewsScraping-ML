# -*- coding: utf-8 -*-
from celery import Celery
celery = Celery()
celery.config_from_object('celeryconfig')

celery.send_task("tasks.post_process",0])
