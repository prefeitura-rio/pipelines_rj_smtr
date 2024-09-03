# -*- coding: utf-8 -*-
import os
from time import sleep

from prefect import task

from pipelines.utils.jdbc import JDBC

@task
def wait_sleeping(interval_seconds: int = 54000, wait=None):
    sleep(interval_seconds)


@task
def get_db_object(secret_path='radar_serpro', environment:str='dev'):
    return JDBC(db_params_secret_path=secret_path, environment=environment)
