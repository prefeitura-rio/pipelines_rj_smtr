# -*- coding: utf-8 -*-
import csv
from pathlib import Path
from time import sleep

from prefect import task

from pipelines.serpro.constants import constants
from pipelines.utils.jdbc import JDBC
from pipelines.utils.utils import log


@task
def wait_sleeping(interval_seconds: int = 54000):
    sleep(interval_seconds)


@task(checkpoint=False)
def get_db_object(secret_path="radar_serpro", environment: str = "dev"):
    return JDBC(db_params_secret_path=secret_path, environment=environment)


@task(checkpoint=False, nout=2)
def get_raw_serpro(
    jdbc: JDBC, start_date: str, end_date: str, local_filepath: str, batch_size: int = 50000
) -> str:
    """
    Task para capturar dados brutos do SERPRO com base em um intervalo de datas.

    Args:
        jdbc (JDBC): Instância para execução de queries via JDBC.
        start_date (str): Data de início no formato "YYYY-MM-DD".
        end_date (str): Data de fim no formato "YYYY-MM-DD".
        local_filepath (str): Local onde o arquivo será salvo.
        batch_size (int): Tamanho do lote.

    Returns:
        str: Caminho do arquivo salvo.
    """

    raw_filepath = local_filepath.format(mode="raw", filetype="csv")
    Path(raw_filepath).parent.mkdir(parents=True, exist_ok=True)

    query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
        start_date=start_date, end_date=end_date
    )

    jdbc.execute_query(query)
    columns = jdbc.get_columns()

    rows = jdbc.fetch_batch(batch_size=batch_size)

    with open(raw_filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)

    log(f"Raw data saved to: {raw_filepath}")
    return raw_filepath
