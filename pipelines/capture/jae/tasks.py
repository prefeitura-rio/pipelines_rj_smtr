# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Jaé"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task
from pytz import timezone

from pipelines.capture.jae.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.database import test_database_connection
from pipelines.utils.extractors.db import get_raw_db
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_jae_general_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas da Jaé"""

    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]

    start = (
        source.get_last_scheduled_timestamp(timestamp=timestamp)
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    end = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    query = params["query"].format(start=start, end=end)
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]

    return partial(
        get_raw_db,
        query=query,
        engine=database["engine"],
        host=database["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database_name,
    )


@task(nout=2)
def test_jae_databases_connections() -> tuple[bool, list[str]]:
    """
    Testa a conexão com os bancos de dados da Jaé

    Returns:
        bool: Se todas as conexões foram bem-sucedidas ou não
        list[str]: Lista com os nomes dos bancos de dados com falha de conexão
    """
    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    failed_connections = []
    for database_name, database in constants.JAE_DATABASE_SETTINGS.value.items():
        success, _ = test_database_connection(
            engine=database["engine"],
            host=database["host"],
            user=credentials["user"],
            password=credentials["password"],
            database=database_name,
        )
        if not success:
            failed_connections.append(database_name)

    return len(failed_connections) == 0, failed_connections


@task
def create_database_error_discord_message(failed_connections: list[str]) -> str:
    """
    Cria a mensagem para ser enviada no Discord caso haja
    problemas de conexão com os bancos da Jaé

    Args:
        failed_connections (list[str]): Lista com os nomes dos bancos de dados com falha de conexão
    Returns:
        str: Mensagem
    """
    message = "Falha de conexão com o(s) banco(s) de dados:\n"
    failed_connections = "\n".join(failed_connections)
    message += failed_connections
    return message
