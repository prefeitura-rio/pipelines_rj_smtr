# -*- coding: utf-8 -*-
"""Module containing general purpose tasks"""
from datetime import datetime, timedelta
from typing import Any, Union

import prefect
from prefect import task
from prefect.engine.signals import FAIL
from prefect.triggers import all_finished
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.discord import send_discord_message
from pipelines.utils.prefect import FailedSubFlow, create_subflow_run, wait_subflow_run
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


@task(trigger=all_finished)
def task_value_is_none(task_value: Union[Any, None]) -> bool:
    """Testa se o valor retornado por uma Task é None

    Args:
        task_value (Union[Any, None]): Valor retornado por uma Task

    Returns:
        bool: Se o valor é None ou não
    """
    return task_value is None


@task
def get_current_timestamp(
    truncate_minute: bool = True,
) -> datetime:
    """
    Retorna a timestamp atual em UTC

    Args:
        truncate_minute: Se for True, substitui os segundos e os microssegundos por 0

    Returns:
        Union[datetime, str]: A timestamp atual
    """

    timestamp = datetime.now(tz=timezone("UTC"))
    if truncate_minute:
        timestamp = timestamp.replace(second=0, microsecond=0)

    return timestamp


@task
def get_scheduled_timestamp(timestamp: str = None) -> datetime:
    """
    Retorna a timestamp do agendamento da run atual

    Returns:
        datetime: A data e hora do agendamento
    """
    if timestamp is not None:
        timestamp = datetime.fromisoformat(timestamp)
    else:
        timestamp = prefect.context["scheduled_start_time"]

    timestamp = convert_timezone(timestamp=timestamp).replace(second=0, microsecond=0)

    log(f"Created timestamp: {timestamp}")
    return timestamp


@task
def parse_timestamp_to_string(timestamp: datetime, pattern: str = "%Y-%m-%d-%H-%M-%S") -> str:
    """
    Converte um datetime em string

    Args:
        timestamp (datetime): O datetime a ser convertido
        pattern (str): O formato da string de data retornado

    """
    if pattern.lower() == "iso":
        return timestamp.isoformat()
    return timestamp.strftime(pattern)


@task
def parse_string_to_timestamp(
    timestamp_str: Union[None, str],
    pattern: str = "iso",
    tz: str = constants.TIMEZONE.value,
) -> Union[None, datetime]:
    """
    Converte uma string para um datetime

    Args:
        timestamp_str (Union[None, str]): String para converter
        pattern (str, optional): Formato de data da string. Caso seja "iso", aplica a função
            fromisoformat
        tz (str, optional): Nome da timezone
    """
    if timestamp_str is None:
        return timestamp_str
    if pattern.lower() == "iso":
        timestamp = datetime.fromisoformat(timestamp_str)
    else:
        timestamp = datetime.strptime(timestamp_str, pattern)

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone(tz))
    else:
        timestamp = timestamp.astimezone(tz=timezone(tz))

    return timestamp


@task
def get_run_env() -> str:
    """
    Retorna o ambiente de execução atual com base no projeto do Prefect

    Returns:
        str: "dev" ou "prod"
    """
    try:
        run_mode = get_flow_run_mode()
        if run_mode == "staging":
            return "dev"
        return run_mode
    except ValueError as err:
        if "Invalid project name: None" in str(err):
            return "dev"
        raise err


@task
def flow_log(msg, level: str = "info"):
    """
    Task para Debug, executa a função log no nível do flow

    Args:
        msg: Texto para exibir no log
        level (str): Level do log do Prefect
    """
    log(msg, level=level)


@task
def run_subflow(
    flow_name: str,
    parameters: Union[list[dict], dict],
    project_name: str = None,
    labels: list[str] = None,
    maximum_parallelism: int = None,
):
    """
    Executa e espera a execução de um flow.

    Args:
        flow_name (str): Nome do flow a ser executado.
        parameters (Union[list[dict], dict]): Parâmetros para executar o flow. Caso seja uma lista,
            irá executar o flow uma vez para cada dict de parâmetros
        project_name (str, optional): Nome do projeto no Prefect para executar o flow,
            se não for especificado, é utilizado o nome do projeto do flow atual
        labels (list[str]): Labels para executar o flow,
            se não for especificado, são utilizadas as labels do flow atual
        maximum_parallelism (int): Número máximo de runs a serem executadas de uma vez
    """

    if not isinstance(parameters, (dict, list)):
        raise ValueError("parameters must be a list or a dict")

    if isinstance(parameters, dict):
        parameters = [parameters]

    if maximum_parallelism is not None:
        parameters = [
            parameters[i : i + maximum_parallelism]  # noqa
            for i in range(0, len(parameters), maximum_parallelism)
        ]

    idempotency_key = prefect.context.get("task_run_id")
    map_index = prefect.context.get("map_index")
    if idempotency_key and map_index is not None:
        idempotency_key += f"-{map_index}"

    flow_run_results = []

    for idx, param_list in enumerate(parameters):
        if not isinstance(param_list, list):
            param_list = [param_list]

        runs_ids = [
            create_subflow_run(
                flow_name=flow_name,
                parameters=params,
                idempotency_key=idempotency_key + f"-{idx}-{sub_idx}",
                project_name=project_name,
                labels=labels,
            )
            for sub_idx, params in enumerate(param_list)
        ]

        for run_id in runs_ids:
            result = wait_subflow_run(flow_run_id=run_id)
            flow_run_results.append(result)

    failed_message = "The following runs failed:"
    flag_failed_runs = False
    for res in flow_run_results:
        if res.state.is_failed():
            flag_failed_runs = True
            failed_message += "\n" + constants.FLOW_RUN_URL_PATTERN.value.format(
                run_id=res.flow_run_id
            )

    if flag_failed_runs:
        raise FailedSubFlow(failed_message)


@task(trigger=all_finished)
def transform_task_state(results: Union[list, str]):
    """
    Transforms the task state to success and returns the results
    """
    return results


@task(trigger=all_finished)
def check_fail(results: Union[list, str]):
    """
    Checks if any task result indicates failure.

    Args:
        results (Union[list, str]): A result or list of results to check.

    Returns:
        bool: True if any result is an instance of `FAIL`, otherwise False.
    """
    if isinstance(results, list):
        return any(isinstance(result, FAIL) for result in results)
    else:
        return isinstance(results, FAIL)


@task
def log_discord(message: str, key: str, dados_tag: bool = False):
    """Logs message to discord channel specified

    Args:
        message (str): Message to post on the channel
        key (str): Key to secret path storing the webhook to channel.
        dados_tag (bool): Indicates whether the message will tag the data team
    """
    if dados_tag:
        message = (
            message + f" - <@&{constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>\n"
        )
    url = get_secret(secret_path=constants.WEBHOOKS_SECRET_PATH.value)[key]
    send_discord_message(message=message, webhook_url=url)


@task
def remove_key_from_dict(data: dict, key: str) -> dict:
    """Removes a specific key from a dictionary.

    Args:
        data (dict): The original dictionary.
        key (str): The key to be removed.

    Returns:
        dict: The dictionary without the specified key.
    """
    data_copy = data.copy()
    data_copy.pop(key, None)
    return data_copy


@task
def add_days_to_date(
    date_str: str, days: int = 1, pattern: str = constants.DATE_PATTERN.value
) -> str:
    """
    Adiciona `days` a uma data em formato string.

    Args:
        date_str (str): Data no formato especificado (default: "YYYY-MM-DD").
        days (int): Número de dias a adicionar (pode ser negativo).
        pattern (str): Formato da data (default: "%Y-%m-%d").

    Returns:
        str: Nova data como string.
    """
    if date_str is None:
        return None

    dt = datetime.strptime(date_str, pattern)
    new_dt = dt + timedelta(days=days)
    return new_dt.strftime(pattern)
