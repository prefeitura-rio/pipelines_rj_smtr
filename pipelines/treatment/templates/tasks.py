# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
from typing import Union

import pandas as pd
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.dbt import run_dbt_model
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.treatment.templates.utils import (
    create_dataplex_log_message,
    send_dataplex_discord_message,
)
from pipelines.utils.dataplex import DataQuality
from pipelines.utils.gcp import BQTable
from pipelines.utils.prefect import flow_is_running_local
from pipelines.utils.utils import get_last_materialization_redis_key


@task
def get_last_materialization_datetime(
    env: str,
    dataset_id: str,
    table_id: str,
    datetime_column_name: str,
) -> tuple[datetime, str]:
    """
    Busca no Redis o último datetime materializado. Caso não exista no Redis,
    consulta a tabela no BigQuery.

    Args:
        env (str): dev ou prod
        dataset_id (str): dataset_id no DBT
        table_id (str): table_id no DBT

    Returns:
        datetime: A última data e hora materializada
    """
    key = get_last_materialization_redis_key(env=env, dataset_id=dataset_id, table_id=table_id)

    redis_client = get_redis_client()
    runs = redis_client.get(key)
    try:
        last_run_timestamp = runs[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value]
    except (KeyError, TypeError):
        last_run_timestamp = None

    if last_run_timestamp is None:
        log("Failed to fetch key from Redis...\n Querying tables for last suceeded run")
        table = BQTable(env=env, dataset_id=dataset_id, table_id=table_id)
        if table.exists() and datetime_column_name is not None:
            log("Table exists, getting max datetime")
            last_run_timestamp = table.get_table_min_max_value(
                field_name=datetime_column_name, kind="max"
            )
        else:
            log(
                "datetime_column_name is None"
                if datetime_column_name is None
                else "Table does not exist"
            )
    else:
        last_run_timestamp = datetime.strptime(
            last_run_timestamp,
            constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
        )

    if (not isinstance(last_run_timestamp, datetime)) and (isinstance(last_run_timestamp, date)):
        last_run_timestamp = datetime(
            last_run_timestamp.year,
            last_run_timestamp.month,
            last_run_timestamp.day,
        )

    if not isinstance(last_run_timestamp, datetime):
        raise ValueError(
            f"last_run_timestamp must be datetime. Received: {type(last_run_timestamp)}"
        )

    log(f"Got value {last_run_timestamp}")
    return last_run_timestamp


@task
def get_repo_version() -> str:
    """
    Pega o SHA do último commit do repositório no GITHUB

    Returns:
        str: SHA do último commit do repositório no GITHUB
    """
    response = requests.get(
        f"{constants.REPO_URL.value}/commits",
        timeout=constants.MAX_TIMEOUT_SECONDS.value,
    )

    response.raise_for_status()

    return response.json()[0]["sha"]


@task
def create_dbt_run_vars(
    datetime_vars: Union[list[dict[datetime]], dict[datetime]],
    repo_version: str,
) -> list[dict[str]]:
    """
    Cria a lista de variaveis para rodar o modelo DBT,
    unindo a versão do repositório com as variaveis de datetime

    Args:
        datetime_vars (Union[list[dict[datetime]], dict[datetime]]): Variáveis de datetime
            usadas para limitar as execuções incrementais do modelo
        repo_version (str): SHA do último commit do repositorio no GITHUB

    Returns:
        list[dict[str]]: Variáveis para executar o modelo DBT
    """
    datetime_vars = datetime_vars or [{}]
    datetime_vars = [datetime_vars] if not isinstance(datetime_vars, list) else datetime_vars
    var_list = []
    for datetime_variable in datetime_vars:
        var_list.append(datetime_variable | {"version": repo_version})

    return datetime_variable


@task
def run_dbt_model_task(
    dataset_id: str,
    table_id: str,
    upstream: bool,
    downstream: bool,
    exclude: str,
    rebuild: bool,
    dbt_run_vars: list[dict[str]],
):
    """
    Executa o modelo DBT

    Args:
        dataset_id (str): dataset_id no DBT
        table_id (str): table_id no DBT
        upstream (bool): Se verdadeiro, irá executar os modelos anteriores
        downstream (bool): Se verdadeiro, irá executar os modelos posteriores
        exclude (str): Modelos para excluir da execução
        rebuild (bool): Se True, irá executar com a flag --full-refresh
        dbt_run_vars (list[dict[str]]): Lista de variáveis para executar o modelo
    """
    if rebuild and len(dbt_run_vars) > 1:
        raise ValueError(
            f"Rebuild = True with multiple model runs: len(dbt_run_vars)={len(dbt_run_vars)}"
        )
    flags = "--full-refresh" if rebuild else None
    for variable in dbt_run_vars:
        run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=upstream,
            downstream=downstream,
            exclude=exclude,
            flags=flags,
            _vars=variable,
        )


@task
def save_materialization_datetime_redis(redis_key: str, value: datetime):
    value = value.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
    log(f"Saving timestamp {value} on key: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    if not content:
        content = {}
    redis_client.set(redis_key, content)


@task
def run_data_quality_checks(
    check_ids: list[str],
    partition_column_name: str,
    initial_partition: datetime,
    final_partition: datetime,
):
    if flow_is_running_local():
        return

    if not isinstance(check_ids, list):
        raise ValueError(
            f"O argumento check_ids precisa receber uma lista. Tipo passado: {type(check_ids)}"
        )
    for check_id in check_ids:
        dataplex = DataQuality(data_scan_id=check_id)
        initial_partition = initial_partition.strftime("%Y-%m-%d")
        final_partition = final_partition.strftime("%Y-%m-%d")
        row_filters = (
            f"{partition_column_name} BETWEEN '{initial_partition}' AND '{final_partition}'"
        )
        run = dataplex.run_parameterized(
            row_filters=row_filters,
            wait_run_completion=True,
        )

        log(create_dataplex_log_message(dataplex_run=run))

        if not run.data_quality_result.passed:
            send_dataplex_discord_message(
                dataplex_check_id=check_id,
                dataplex_run=run,
                timestamp=datetime.now(tz=timezone(constants.TIMEZONE.value)),
                initial_partition=initial_partition,
                final_partition=final_partition,
            )


@task(nout=2)
def create_date_range_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,
    overwrite_initial_datetime: datetime,
) -> tuple[dict, datetime]:
    log("Creating daterange DBT variables")
    pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN.value
    date_range_start = overwrite_initial_datetime or last_materialization_datetime
    if last_materialization_datetime is not None:
        date_range_start = last_materialization_datetime.strftime(pattern)
    else:
        log("last_materialization_datetime é Nulo")

    date_range_end = timestamp - timedelta(hours=incremental_delay_hours)

    date_range = {
        "date_range_start": date_range_start,
        "date_range_end": date_range_end.strftime(pattern),
    }
    log(f"Got date_range as: {date_range}")

    return date_range, date_range_end


@task(nout=2)
def create_run_date_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,  # pylint: disable=W0613
    overwrite_initial_datetime: datetime,
) -> tuple[list[dict], datetime]:
    log("Creating run_date DBT variable")
    if last_materialization_datetime is None:
        log("last_materialization_datetime é Nulo")
        return None, timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    date_range_start = overwrite_initial_datetime or last_materialization_datetime
    date_range = pd.date_range(start=date_range_start, end=timestamp)
    dates = [{"run_date": d.strftime("%Y-%m-%d")} for d in date_range]

    log(f"Created the following dates: {dates}")
    return dates, date_range[-1].to_pydatetime()


@task(nout=2)
def create_run_date_hour_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,
    overwrite_initial_datetime: datetime,
) -> tuple[list[dict], datetime]:
    log("Creating run_date_hour DBT variable")
    if last_materialization_datetime is None:
        log("last_materialization_datetime é Nulo")
        return None, timestamp.replace(minute=0, second=0, microsecond=0)
    date_range_start = overwrite_initial_datetime or last_materialization_datetime
    date_range_end = timestamp - timedelta(hours=incremental_delay_hours)
    date_range = pd.date_range(start=date_range_start, end=date_range_end, freq="H")
    dates = [{"run_date_hour": d.strftime("%Y-%m-%d %H:%M:%S")} for d in date_range]

    log(f"Created the following dates: {dates}")
    return dates, date_range[-1].to_pydatetime()
