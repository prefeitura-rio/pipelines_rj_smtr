# -*- coding: utf-8 -*-
from time import sleep

from prefect import task


@task
def wait_sleeping(interval_seconds: int = 3600):
    sleep(interval_seconds)
