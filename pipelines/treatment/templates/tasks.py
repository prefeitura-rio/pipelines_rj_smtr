# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from typing import Union

import basedosdados as bd
import pandas as pd
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.treatment.templates.utils import (
    DBTSelector,
    create_dataplex_log_message,
    send_dataplex_discord_message,
)
from pipelines.utils.dataplex import DataQuality, DataQualityCheckArgs
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.prefect import flow_is_running_local, rename_current_flow_run

# from pipelines.utils.utils import get_last_materialization_redis_key

try:
    from prefect.tasks.dbt.dbt import DbtShellTask
except ImportError:
    from prefeitura_rio.utils import base_assert_dependencies

    base_assert_dependencies(["prefect"], extras=["pipelines"])

from prefeitura_rio.pipelines_utils.io import get_root_path


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def rename_materialization_flow(
    selector: DBTSelector,
    timestamp: datetime,
    datetime_start: datetime,
    datetime_end: datetime,
) -> bool:
    """
    Renomeia a run atual do Flow de materialização com o formato:
    [<timestamp>] <selector_name>: from <valor inicial> to <valor final>

    Args:
        dataset_id (str): dataset_id no DBT
        table_id (str): table_id no DBT
        timestamp (datetime): timestamp de execução do Flow
        datetime_start (datetime): Partição inicial da materialização
        datetime_end (datetime): Partição final da materialização

    Returns:
        bool: Se o flow foi renomeado
    """
    name = f"[{timestamp.astimezone(tz=timezone(constants.TIMEZONE.value))}] \
{selector.name}: from {datetime_start} to {datetime_end}"
    return rename_current_flow_run(name=name)


@task
def get_datetime_start(
    env: str,
    selector: DBTSelector,
    datetime_start: Union[str, datetime, None],
) -> datetime:
    if datetime_start is not None:
        if isinstance(datetime_start, str):
            datetime_start = datetime.fromisoformat(datetime_start)
        return datetime_start
    return selector.get_last_materialized_datetime(env=env)


@task
def get_datetime_end(
    selector: DBTSelector,
    timestamp: datetime,
    datetime_end: Union[str, datetime, None],
) -> datetime:
    if datetime_end is not None:
        if isinstance(datetime_end, str):
            datetime_end = datetime.fromisoformat(datetime_end)
        return datetime_end
    return timestamp - timedelta(hours=selector.incremental_delay_hours)


@task
def wait_data_sources(data_sources: list[Union[SourceTable, DBTSelector]]):
    pass


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

    return var_list


@task
def run_dbt_selector(
    selector_name: str,
    flags: str = None,
    _vars: dict | list[dict] = None,
):
    """
    Runs a DBT selector.

    Args:
        selector_name (str): The name of the DBT selector to run.
        flags (str, optional): Flags to pass to the dbt run command.
        _vars (Union[dict, list[dict]], optional): Variables to pass to dbt. Defaults to None.
    """
    # Build the dbt command
    run_command = f"dbt run --selector {selector_name}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    log(f"Running dbt with command: {run_command}")
    root_path = get_root_path()
    queries_dir = str(root_path / "queries")
    dbt_task = DbtShellTask(
        profiles_dir=queries_dir,
        helper_script=f"cd {queries_dir}",
        log_stderr=True,
        return_all=True,
        command=run_command,
    )
    dbt_logs = dbt_task.run()

    log("\n".join(dbt_logs))


@task
def save_materialization_datetime_redis(redis_key: str, value: datetime):
    """
    Salva o datetime de materialização do Redis

    Args:
        redis_key (str): Key do Redis para salvar o valor
        value (datetime): Datetime a ser salvo
    """
    value = value.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
    log(f"Saving timestamp {value} on key: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    if not content:
        content = {}
    redis_client.set(redis_key, content)


@task
def run_data_quality_checks(
    data_quality_checks: list[DataQualityCheckArgs],
    initial_timestamp: datetime,
):
    """
    Executa os testes de qualidade de dados no Dataplex

    Args:
        data_quality_checks (list[DataQualityCheckArgs]): Lista de testes para executar
        initial_timestamp (datetime): Data inicial para filtrar as partições modificadas
    """
    if flow_is_running_local():
        return

    if not isinstance(data_quality_checks, list):
        raise ValueError(
            f"data_quality_checks precisa ser uma lista. Recebeu: {type(data_quality_checks)}"
        )

    log("Executando testes de qualidade de dados")

    for check in data_quality_checks:
        dataplex = DataQuality(
            data_scan_id=check.check_id,
            project_id="rj-smtr",
        )
        partition_column_name = check.table_partition_column_name
        partitions = "Sem filtro de partições"
        if partition_column_name is None:
            row_filters = "1=1"
        else:
            partitions = bd.read_sql(
                f"""
            SELECT
                PARSE_DATE('%Y%m%d', partition_id) AS partition_date
            FROM
                `rj-smtr.{check.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE
                table_name = "{check.table_id}"
                AND partition_id != "__NULL__"
                AND
                    DATE(last_modified_time, "America/Sao_Paulo") >=
                    DATE('{initial_timestamp.date().isoformat()}')
            """,
                billing_project_id="rj-smtr-dev",
            )["partition_date"].to_list()

            partitions = [f"'{p}'" for p in partitions]
            row_filters = f"{partition_column_name} IN ({', '.join(partitions)})"

        log(f"Executando check de qualidade de dados {dataplex.id} com o filtro: {row_filters}")
        run = dataplex.run_parameterized(
            row_filters=row_filters,
            wait_run_completion=True,
        )

        log(create_dataplex_log_message(dataplex_run=run))

        if not run.data_quality_result.passed:
            send_dataplex_discord_message(
                dataplex_check_id=check.check_id,
                dataplex_run=run,
                timestamp=datetime.now(tz=timezone(constants.TIMEZONE.value)),
                partitions=partitions,
            )


@task(nout=3)
def create_date_range_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,
    overwrite_initial_datetime: datetime,
) -> tuple[dict, datetime, datetime]:
    """
    Cria as variáveis date_range_start e data_range_end

    Args:
        timestamp (datetime): Timestamp de execução do Flow
        last_materialization_datetime (datetime): Timestamp da última materialização
        incremental_delay_hours (int): Quantidade de horas a ser subtraído do date_range_end
        overwrite_initial_datetime (datetime): Valor para sobrescrever o date_range_start

    Returns:
        dict: Variáveis para serem usadas do DBT
        datetime: datetime inicial
        datetime: datetime final
    """
    log("Creating daterange DBT variables")
    log(
        f"""Parâmetros recebidos:
        timestamp = {timestamp}
        last_materialization_datetime = {last_materialization_datetime}
        incremental_delay_hours = {incremental_delay_hours}
        overwrite_initial_datetime = {overwrite_initial_datetime}
        """
    )
    pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN.value
    date_range_start = overwrite_initial_datetime or last_materialization_datetime

    date_range_end = timestamp - timedelta(hours=incremental_delay_hours)

    date_range = {
        "date_range_start": (
            date_range_start if date_range_start is None else date_range_start.strftime(pattern)
        ),
        "date_range_end": date_range_end.strftime(pattern),
    }
    log(f"Got date_range as: {date_range}")

    return date_range, date_range_start, date_range_end


@task(nout=3)
def create_run_date_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,  # pylint: disable=W0613
    overwrite_initial_datetime: datetime,
) -> tuple[list[dict], datetime, datetime]:
    """
    Cria uma lista de variáveis run_date

    Args:
        timestamp (datetime): Timestamp de execução do Flow
        last_materialization_datetime (datetime): Timestamp da última materialização
        overwrite_initial_datetime (datetime): Valor para sobrescrever a data inicial

    Returns:
        list[dict]: Variáveis para serem usadas do DBT
        datetime: datetime inicial
        datetime: datetime final
    """

    log("Creating run_date DBT variable")
    log(
        f"""Parâmetros recebidos:
        timestamp = {timestamp}
        last_materialization_datetime = {last_materialization_datetime}
        overwrite_initial_datetime = {overwrite_initial_datetime}
        """
    )
    if last_materialization_datetime is None:
        log("last_materialization_datetime é Nulo")
        return None, None, timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    date_range_start = overwrite_initial_datetime or last_materialization_datetime
    date_range = pd.date_range(start=date_range_start, end=timestamp)
    dates = [{"run_date": d.strftime("%Y-%m-%d")} for d in date_range]

    log(f"Created the following dates: {dates}")
    return dates, date_range[0].to_pydatetime(), date_range[-1].to_pydatetime()


@task(nout=3)
def create_run_date_hour_variable(
    timestamp: datetime,
    last_materialization_datetime: datetime,
    incremental_delay_hours: int,
    overwrite_initial_datetime: datetime,
) -> tuple[list[dict], datetime, datetime]:
    """
    Cria uma lista de variáveis run_date_hour

    Args:
        timestamp (datetime): Timestamp de execução do Flow
        last_materialization_datetime (datetime): Timestamp da última materialização
        incremental_delay_hours (int): Quantidade de horas a ser subtraído da data final
        overwrite_initial_datetime (datetime): Valor para sobrescrever a data inicial

    Returns:
        list[dict]: Variáveis para serem usadas do DBT
        datetime: datetime inicial
        datetime: datetime final
    """

    log("Creating run_date_hour DBT variable")
    log(
        f"""Parâmetros recebidos:
        timestamp = {timestamp}
        last_materialization_datetime = {last_materialization_datetime}
        overwrite_initial_datetime = {overwrite_initial_datetime}
        """
    )
    if last_materialization_datetime is None:
        log("last_materialization_datetime é Nulo")
        return None, timestamp.replace(minute=0, second=0, microsecond=0)

    date_range_start = overwrite_initial_datetime or last_materialization_datetime
    date_range_end = timestamp - timedelta(hours=incremental_delay_hours)
    date_range = pd.date_range(start=date_range_start, end=date_range_end, freq="H")
    dates = [{"run_date_hour": d.strftime("%Y-%m-%d %H:%M:%S")} for d in date_range]

    log(f"Created the following dates: {dates}")
    return dates, date_range[0].to_pydatetime(), date_range[-1].to_pydatetime()
