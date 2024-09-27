# -*- coding: utf-8 -*-
import csv
import os
from time import sleep
from typing import List

from prefect import task

from pipelines.utils.jdbc import JDBC
from pipelines.utils.utils import log


@task
def wait_sleeping(interval_seconds: int = 54000, wait=None):
    sleep(interval_seconds)


@task
def get_db_object(secret_path="radar_serpro", environment: str = "dev"):
    return JDBC(db_params_secret_path=secret_path, environment=environment)


@task
def dump_serpro(jdbc: JDBC, batch_size: int) -> List[str]:

    index = 0
    data_folder = os.getenv("DATA_FOLDER", "data")
    file_path = f"{os.getcwd()}/{data_folder}/raw/radar_serpro/tb_infracao_view"
    csv_files = []

    query = "SELECT * FROM dbpro_radar_view_SMTR_VBL.tb_infracao_view"

    jdbc.execute_query(query)

    columns = jdbc.get_columns()

    while True:
        rows = jdbc.fetch_batch(batch_size)

        if not rows:
            break

        output_file = file_path + f"dados_infracao_{index}.csv"

        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)

        csv_files.append(output_file)
        index += 1

    jdbc.close_connection()

    return csv_files


@task
def list_files():

    try:
        files = os.listdir("/app")
        log(f"Files: {files}")
    except Exception as e:
        log(f"Erro: {e}")
