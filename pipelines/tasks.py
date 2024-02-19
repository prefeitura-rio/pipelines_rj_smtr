# -*- coding: utf-8 -*-
"""Module containing general purpose tasks"""
from datetime import datetime
from typing import Any, Union

import prefect
from prefect import task, unmapped
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from pytz import timezone

from pipelines.utils.prefect import get_current_flow_labels


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
def run_flow(
    flow_name: str,
    parameters: dict,
    project_name: str = None,
    labels: list[str] = None,
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

    Returns:
        FunctionTask: retorno da task wait_for_flow_run
    """
    if project_name is None:
        project_name = prefect.context.get("project_name")

    if labels is None:
        labels = get_current_flow_labels()

    subflow_run = create_flow_run.run(
        flow_name=flow_name,
        project_name=project_name,
        labels=labels,
        parameters=parameters,
    )

    wait_for_flow_run.run(
        subflow_run,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )


@task
def run_flow_mapped(
    flow_name: str,
    parameters: list[dict],
    project_name: str = None,
    labels: list[str] = None,
    maximum_parallelism: int = None,
):
    """
    Executa e espera várias execuções de um mesmo flow em paralelo
    com diferentes argumentos

    Args:
        flow_name (str): Nome do flow a ser executado.
        parameters (list[dict]): Lista de parâmetros para cada execução do flow.
        project_name (str, optional): Nome do projeto no Prefect para executar o flow,
            se não for especificado, é utilizado o nome do projeto do flow atual
        labels (list[str]): Labels para executar o flow,
            se não for especificado, são utilizadas as labels do flow atual

    Returns:
        FunctionTask: retorno da task wait_for_flow_run
    """
    if project_name is None:
        project_name = prefect.context.get("project_name")

    if labels is None:
        labels = get_current_flow_labels()

    if maximum_parallelism is None:
        execution_list = [parameters]
    else:
        execution_list = [
            parameters[i : i + maximum_parallelism]  # noqa
            for i in range(0, len(parameters), maximum_parallelism)
        ]

    for params in execution_list:

        subflow_run = create_flow_run.map(
            flow_name=unmapped(flow_name),
            project_name=unmapped(project_name),
            labels=unmapped(labels),
            parameters=params,
        ).run()

        wait_for_flow_run.map(
            subflow_run,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        ).run()
