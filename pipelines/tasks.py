# -*- coding: utf-8 -*-
"""Module containing general purpose tasks"""
from datetime import datetime
from typing import Any, Union

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from pytz import timezone


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
