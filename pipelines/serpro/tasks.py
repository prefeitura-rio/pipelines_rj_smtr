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
    """
    Creates a JDBC object.

    Args:
        secret_path (str): The path to the secret containing database credentials.
                           Defaults to "radar_serpro".
        environment (str): The environment for the connection. Defaults to "dev".

    Returns:
        JDBC: A JDBC connection object.
    """
    return JDBC(db_params_secret_path=secret_path, environment=environment)


@task(checkpoint=False, nout=2)
def get_raw_serpro(
    jdbc: JDBC, start_date: str, end_date: str, local_filepath: str, batch_size: int = 100000
) -> str:
    """
    Task to fetch raw data from SERPRO based on a date range.

    Args:
        jdbc (JDBC): Instance for executing queries via JDBC.
        start_date (str): Start date in the format "YYYY-MM-DD".
        end_date (str): End date in the format "YYYY-MM-DD".
        local_filepath (str): Path where the file will be saved.
        batch_size (int): Batch size.

    Returns:
        str: Path of the saved file.
    """

    raw_filepath = local_filepath.format(mode="raw", filetype="csv")
    Path(raw_filepath).parent.mkdir(parents=True, exist_ok=True)

    query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
        start_date=start_date, end_date=end_date
    )

    jdbc.execute_query(query)
    columns = jdbc.get_columns()

    with open(raw_filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        while True:
            rows = jdbc.fetch_batch(batch_size=batch_size)
            if not rows:
                break
            writer.writerows(rows)

    log(f"Raw data saved to: {raw_filepath}")
    return raw_filepath
