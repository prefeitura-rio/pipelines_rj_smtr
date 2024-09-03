# -*- coding: utf-8 -*-
import os
from pathlib import Path
from time import sleep

from prefect import task

from pipelines.utils.secret import get_secret


@task
def wait_sleeping(interval_seconds: int = 3600, wait=None):
    sleep(interval_seconds)


@task
def setup_serpro(secret_path: str = "radar_serpro"):
    data = get_secret(secret_path=secret_path)["setup.sh"]
    with open("setup.sh", "w") as f:
        f.write(data)
    return os.popen("sh setup.sh")
