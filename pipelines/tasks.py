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
    return task_value is None


@task
def get_current_timestamp(
    truncate_minute: bool = True,
    return_str: bool = False,
) -> Union[datetime, str]:
    """
    Get current timestamp for flow run.

    Args:
        truncate_minute: whether to truncate the timestamp to the minute or not
        return_str: if True, the return will be an isoformatted datetime string
                    otherwise it returns a datetime object

    Returns:
        Union[datetime, str]: timestamp for flow run
    """

    timestamp = datetime.now(tz=timezone("UTC"))
    if truncate_minute:
        timestamp = timestamp.replace(second=0, microsecond=0)
    if return_str:
        timestamp = timestamp.isoformat()

    return timestamp


@task
def parse_timestamp_to_string(timestamp: datetime, pattern="%Y-%m-%d-%H-%M-%S") -> str:
    """
    Parse timestamp to string pattern.
    """
    if pattern.lower() == "iso":
        return timestamp.isoformat()
    return timestamp.strftime(pattern)


@task
def get_run_env():
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
    log(msg, level=level)
