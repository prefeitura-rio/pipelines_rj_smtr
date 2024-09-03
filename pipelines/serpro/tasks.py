# -*- coding: utf-8 -*-
from time import sleep
from prefect import task
import os

from pipelines.utils.secret import get_secret  
from pipelines.utils.utils import log


@task
def wait_sleeping(interval_seconds: int = 54000, wait=None):
    sleep(interval_seconds)


@task
def setup_serpro(secret_path:str='radar_serpro'):
    data = get_secret(secret_path=secret_path)['setup.sh']
    log('Got Secret')
    os.popen('touch setup.sh')
    with open('setup.sh','w') as f:
        f.write(data)
    return os.popen("sh setup.sh")
