# -*- coding: utf-8 -*-
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

from prefect import task

from pipelines.serpro.constants import constants
from pipelines.utils.jdbc import JDBC
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log


@task
def wait_sleeping(interval_seconds: int = 54000):
    sleep(interval_seconds)


@task(checkpoint=False, max_retries=10, retry_delay=timedelta(seconds=300))
def get_db_object(secret_path="radar_serpro", environment: str = "dev"):
    jar_path = get_secret(secret_path=secret_path, environment=environment)["jars"]

    if not os.path.exists(jar_path):
        raise Exception(f"Arquivo JAR '{jar_path}' não encontrado.")

    return JDBC(db_params_secret_path=secret_path, environment=environment)


@task(checkpoint=False, nout=2)
def get_raw_serpro(jdbc: JDBC, timestamp: datetime, local_filepath: str) -> str:
    date = timestamp.date()
    raw_filepath = local_filepath.format(mode="raw", filetype="csv")
    Path(raw_filepath).parent.mkdir(parents=True, exist_ok=True)

    query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(date=date.strftime("%Y-%m-%d"))

    jdbc.execute_query(query)
    columns = jdbc.get_columns()

    rows = jdbc.fetch_all()

    with open(raw_filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)

    log(f"Raw data saved to: {raw_filepath}")
    return raw_filepath
