from prefect import task
from time import sleep

@task
def wait_sleeping(interval_seconds:int=3600):
    sleep(interval_seconds)