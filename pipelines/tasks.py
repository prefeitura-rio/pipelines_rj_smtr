# -*- coding: utf-8 -*-
"""Module containing general purpose tasks"""
from datetime import datetime
from typing import Any, Union

import prefect
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from pytz import timezone

from pipelines.utils.prefect import create_subflow_run, wait_subflow_run


@task
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
    Executa e espera a execução de um flow

    Args:
        flow_name (str): Nome do flow a ser executado.
        parameters (dict): Parâmetros para executar o flow
        project_name (str, optional): Nome do projeto no Prefect para executar o flow,
            se não for especificado, é utilizado o nome do projeto do flow atual
        labels (list[str]): Labels para executar o flow,
            se não for especificado, são utilizadas as labels do flow atual
    """

    if not isinstance(parameters, (dict, list)):
        raise ValueError("parameters must be a list or a dict")

    if maximum_parallelism is not None and isinstance(parameters, list):
        execution_list = [
            parameters[i : i + maximum_parallelism]  # noqa
            for i in range(0, len(parameters), maximum_parallelism)
        ]

    idempotency_key = prefect.context.get("task_run_id")
    map_index = prefect.context.get("map_index")
    if idempotency_key and map_index is not None:
        idempotency_key += f"-{map_index}"

    for idx, param_list in enumerate(execution_list):

        if not isinstance(param_list, list):
            param_list = [param_list]

        runs_ids = [
            create_subflow_run(
                flow_name=flow_name,
                parameters=params,
                idempotency_key=idempotency_key + f"-{idx}" + f"-{sub_idx}",
                project_name=project_name,
                labels=labels,
            )
            for sub_idx, params in enumerate(param_list)
        ]

        for run_id in runs_ids:
            wait_subflow_run(flow_run_id=run_id)
