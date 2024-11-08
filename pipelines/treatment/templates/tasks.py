# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta
from typing import Dict, List, Union

import basedosdados as bd
import prefect
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.treatment.templates.utils import (
    DBTSelector,
    IncompleteDataError,
    create_dataplex_log_message,
    parse_dbt_test_output,
    send_dataplex_discord_message,
)
from pipelines.utils.dataplex import DataQuality, DataQualityCheckArgs
from pipelines.utils.discord import format_send_discord_message
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.prefect import flow_is_running_local, rename_current_flow_run
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone, cron_get_last_date

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
    """
    Task que retorna o datetime de inicio da materialização

    Args:
        env (str): prod ou dev
        selector (DBTSelector): Objeto que representa o selector do DBT
        datetime_start (Union[str, datetime, None]): Força um valor no datetime_start

    Returns:
        datetime: datetime de inicio da materialização
    """
    if datetime_start is not None:
        if isinstance(datetime_start, str):
            datetime_start = datetime.fromisoformat(datetime_start)
    else:
        datetime_start = selector.get_last_materialized_datetime(env=env)

    return convert_timezone(timestamp=datetime_start)


@task
def get_datetime_end(
    selector: DBTSelector,
    timestamp: datetime,
    datetime_end: Union[str, datetime, None],
) -> datetime:
    """
    Task que retorna o datetime de fim da materialização

    Args:
        selector (DBTSelector): Objeto que representa o selector do DBT
        timestamp (datetime): Timestamp de execução do flow
        datetime_end (Union[str, datetime, None]): Força um valor no datetime_end

    Returns:
        datetime: datetime de fim da materialização
    """
    if datetime_end is not None:
        if isinstance(datetime_end, str):
            datetime_end = datetime.fromisoformat(datetime_end)
    else:
        datetime_end = selector.get_datetime_end(timestamp=timestamp)

    return convert_timezone(timestamp=datetime_end)


@task
def wait_data_sources(
    env: str,
    datetime_start: datetime,
    datetime_end: datetime,
    data_sources: list[Union[SourceTable, DBTSelector, dict]],
    skip: bool,
):
    """
    Espera os dados fonte estarem completos

    Args:
        env (str): prod ou dev
        datetime_start (datetime): Datetime inicial da materialização
        datetime_end (datetime): Datetime final da materialização
        data_sources (list[Union[SourceTable, DBTSelector, dict]]): Fontes de dados para esperar
        skip (bool): se a verificação deve ser pulada ou não
    """
    if skip:
        log("Pulando verificação de completude dos dados")
        return
    count = 0
    for ds in data_sources:
        log("Checando completude dos dados")
        complete = False
        while not complete:
            if isinstance(ds, SourceTable):
                name = f"{ds.source_name}.{ds.table_id}"
                uncaptured_timestamps = ds.set_env(env=env).get_uncaptured_timestamps(
                    timestamp=datetime_end,
                    retroactive_days=max(2, (datetime_end - datetime_start).days),
                )
                complete = len(uncaptured_timestamps) == 0
            elif isinstance(ds, DBTSelector):
                name = f"{ds.name}"
                complete = ds.is_up_to_date(env=env, timestamp=datetime_end)
            elif isinstance(ds, dict):
                # source dicionário utilizado para compatibilização com flows antigos
                name = ds["redis_key"]
                redis_client = get_redis_client()
                last_materialization = datetime.strptime(
                    redis_client.get(name)[ds["dict_key"]],
                    ds["datetime_format"],
                )
                last_schedule = cron_get_last_date(
                    cron_expr=ds["schedule_cron"],
                    timestamp=datetime_end,
                )
                complete = convert_timezone(
                    timestamp=last_materialization
                ) >= last_schedule - timedelta(hours=ds.get("delay_hours", 0))

            else:
                raise NotImplementedError(f"Espera por fontes do tipo {type(ds)} não implementada")

            log(f"Checando dados do {type(ds)} {name}")
            if not complete:
                if count < 10:
                    log("Dados incompletos, tentando novamente")
                    time.sleep(60)
                    count += 1
                else:
                    log("Tempo de espera esgotado")
                    raise IncompleteDataError(f"{type(ds)} {name} incompleto")
            else:
                log("Dados completos")


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
    datetime_start: datetime,
    datetime_end: datetime,
    repo_version: str,
) -> dict:
    """
    Cria a lista de variaveis para rodar o modelo DBT,
    unindo a versão do repositório com as variaveis de datetime

    Args:
        datetime_start (datetime): Datetime inicial da materialização
        datetime_end (datetime): Datetime final da materialização
        repo_version (str): SHA do último commit do repositorio no GITHUB

    Returns:
        dict[str]: Variáveis para executar o modelo DBT
    """
    pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN.value
    return {
        "date_range_start": datetime_start.strftime(pattern),
        "date_range_end": datetime_end.strftime(pattern),
        "version": repo_version,
    }


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

    root_path = get_root_path()
    queries_dir = str(root_path / "queries")

    if flow_is_running_local():
        run_command += f' --profiles-dir "{queries_dir}/dev"'

    log(f"Running dbt with command: {run_command}")
    dbt_task = DbtShellTask(
        profiles_dir=queries_dir,
        helper_script=f'cd "{queries_dir}"',
        log_stderr=True,
        return_all=True,
        command=run_command,
    )
    dbt_logs = dbt_task.run()

    log("\n".join(dbt_logs))


@task
def save_materialization_datetime_redis(env: str, selector: DBTSelector, value: datetime):
    """
    Salva o datetime de materialização do Redis

    Args:
        redis_key (str): Key do Redis para salvar o valor
        value (datetime): Datetime a ser salvo
    """
    selector.set_redis_materialized_datetime(env=env, timestamp=value)


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
def check_dbt_test_run(
    date_range_start: str, date_range_end: str, run_time: str
) -> tuple[bool, str, str]:
    """
    Compares the specified run time with the start date's time component.
    If they match, it calculates and returns the start and end date strings
    for the previous day in ISO format.

    Args:
        date_range_start (str): The start date of the range.
        date_range_end (str): The end date of the range.
        run_time (str): The time to check against in the format "HH:MM:SS".

    Returns:
        Tuple[bool, str, str]: A tuple containing the following elements:
            - bool: True if the run time matches the start date's time; otherwise, False.
            - str: The start date of the previous day in ISO format if the time matches.
            - str: The end date of the previous day in ISO format if the time matches.
    """

    datetime_start = datetime.fromisoformat(date_range_start)
    datetime_end = datetime.fromisoformat(date_range_end)

    run_time = datetime.strptime(run_time, "%H:%M:%S").time()

    if datetime_start.time() == run_time:
        datetime_start_str = (datetime_start - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        datetime_end_str = (datetime_end - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
        return True, datetime_start_str, datetime_end_str
    return False, None, None


@task
def run_dbt_tests(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    test_name: str = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
) -> str:
    """
    Runs a DBT test

    Args:
        dataset_id (str, optional): Dataset ID of the dbt model. Defaults to None.
        table_id (str, optional): Table ID of the dbt model. Defaults to None.
        model (str, optional): model to be tested. Defaults to None.
        upstream (bool, optional): If True, includes upstream models. Defaults to None.
        downstream (bool, optional): If True, includes downstream models. Defaults to None.
        test_name (str, optional): The name of the specific test to be executed. Defaults to None.
        exclude (str, optional): Models to be excluded from the test execution. Defaults to None.
        flags (str, optional): Additional flags for the `dbt test` command. Defaults to None.
        _vars (Union[dict, List[Dict]], optional): Variables to pass to dbt. Defaults to None.

    Returns:
        str: Logs resulting from the execution of the `dbt test` command.
    """
    run_command = "dbt test"

    if not model:
        model = dataset_id
        if table_id:
            model += f".{table_id}"

    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += model
        if downstream:
            run_command += "+"
        if test_name:
            model += f",test_name:{test_name}"

    if exclude:
        run_command += f" --exclude {exclude}"

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

    root_path = get_root_path()
    queries_dir = str(root_path / "queries")

    if flow_is_running_local():
        run_command += f' --profiles-dir "{queries_dir}/dev"'

    log(f"Running dbt with command: {run_command}")
    dbt_task = DbtShellTask(
        profiles_dir=queries_dir,
        helper_script=f'cd "{queries_dir}"',
        log_stderr=True,
        return_all=True,
        command=run_command,
    )
    dbt_logs = dbt_task.run()

    log("\n".join(dbt_logs))
    dbt_logs = "\n".join(dbt_logs)
    return dbt_logs


@task
def dbt_data_quality_checks(dbt_logs: str, checks_list: dict, params: dict):
    """
    Extracts the results of DBT tests and sends a message with the information to Discord.

    Args:
        dbt_logs (str): Logs from DBT containing the test results.
        checks_list (dict): Dictionary with the names of the tests and their descriptions.
        date_range (dict): Dictionary representing a date range.
    """

    checks_results = parse_dbt_test_output(dbt_logs)

    webhook_url = get_secret(secret_path=constants.WEBHOOKS_SECRET_PATH.value)["dataplex"]

    dados_tag = f" - <@&{constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>\n"

    test_check = all(test["result"] == "PASS" for test in checks_results.values())

    date_range = (
        params["date_range_start"]
        if params["date_range_start"] == params["date_range_end"]
        else f'{params["date_range_start"]} a {params["date_range_end"]}'
    )

    if "(target='dev')" in dbt_logs or "(target='hmg')" in dbt_logs:
        formatted_messages = [
            ":green_circle: " if test_check else ":red_circle: ",
            f"**[DEV] Data Quality Checks - {prefect.context.get('flow_name')} - {date_range}**\n\n",  # noqa
        ]
    else:
        formatted_messages = [
            ":green_circle: " if test_check else ":red_circle: ",
            f"**Data Quality Checks - {prefect.context.get('flow_name')} - {date_range}**\n\n",
        ]

    for table_id, tests in checks_list.items():
        formatted_messages.append(
            f"*{table_id}:*\n"
            + "\n".join(
                f'{":white_check_mark:" if checks_results[test_id]["result"] == "PASS" else ":x:"} '
                f'{test["description"]}'
                for test_id, test in tests.items()
            )
        )

    formatted_messages.append("\n\n")
    formatted_messages.append(
        ":tada: **Status:** Sucesso"
        if test_check
        else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
    )

    formatted_messages.append(dados_tag)
    try:
        format_send_discord_message(formatted_messages, webhook_url)
    except Exception as e:
        log(f"Falha ao enviar mensagem para o Discord: {e}", level="error")
        raise
