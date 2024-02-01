# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
import traceback
from datetime import datetime, timedelta
from typing import Any, Callable, Union

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.capture.base import DataExtractor
from pipelines.utils.fs import read_raw_data, save_local_file
from pipelines.utils.gcp import BQTable
from pipelines.utils.incremental_strategy import (
    IncrementalInfo,
    IncrementalStrategy,
    incremental_strategy_from_dict,
)
from pipelines.utils.prefect import flow_is_running_local, rename_current_flow_run
from pipelines.utils.pretreatment import transform_to_nested_structure
from pipelines.utils.utils import create_timestamp_captura, data_info_str

############################
# Flow Configuration Tasks #
############################


@task
def create_source_dataset_id(project: str, source_name: str):
    """Creates the BQ source table dataset_id according to the standard

    Args:
        project (str): The project that the table fits in (eg.: bilhetagem, subsidio)
        source_name (str): The data source name (eg.: jae)
    """
    return constants.SOURCE_DATASET_ID_PATTERN.value.format(
        project=project,
        source_name=source_name,
    )


@task
def create_table_object(
    env: str,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    timestamp: datetime,
    partition_date_only: bool,
    partition_date_name: str,
    raw_filetype: str,
) -> BQTable:
    """
    Creates basedosdados Table object

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery

    Returns:
        BQTable: Table object
    """

    return BQTable(
        env=env,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        timestamp=timestamp,
        partition_date_only=partition_date_only,
        partition_date_name=partition_date_name,
        raw_filetype=raw_filetype,
    )


# @task
# def create_source_redis_key(
#     env: str,
#     dataset_id: str,
#     table_id: str,
# ):
#     assert env in ("staging", "prod"), f"env must be staging or prod, received: {env}"
#     return f"{env}.{dataset_id}.{table_id}"


@task
def rename_capture_flow(
    dataset_id: str,
    table_id: str,
    timestamp: datetime,
    execution_mode: str,
    start_value: Union[datetime, str],
    end_value: Union[datetime, str],
) -> bool:
    """
    Rename the current capture flow run.
    """
    name = f"[{timestamp.astimezone(tz=timezone(constants.TIMEZONE.value))} | \
{execution_mode.upper()}] {dataset_id}.{table_id}: from {start_value} to {end_value}"
    return rename_current_flow_run(name=name)


#####################
# Raw Capture Tasks #
#####################


@task
def get_raw_data(data_extractor: DataExtractor) -> tuple[str, str]:
    error = None
    try:
        data_extractor.extract()
        data_extractor.save_raw_local()
        log(f"Data saved to: {data_extractor.save_path}")
    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


################
# Upload Tasks #
################


@task
def upload_raw_file_to_gcs(error: str, table: BQTable):
    if error is None:
        try:
            log(f"Uploading file to: {table.raw_filepath}")
            table.upload_raw_file()
        except Exception:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


@task
def upload_source_data_to_gcs(error: str, table: BQTable):
    """Conditionally create table or append data to its relative GCS folder.

    Args:
        error (str): Upstream errors
        table (BQTable): BigQuery table object to create or append
    """
    log_table = table.get_log_table(generate_logs=True, error=error)

    if error is None:
        if not table.exists():
            log("Staging Table does not exist, creating table...")
            table.create()
            log("Table created")
        else:
            log("Staging Table already exists, appending to it...")
            table.append()
            log("Appended to staging table successfully.")

    if log_table.exists():
        log("Log Table does not exist, creating table...")
        log_table.create()
        log("Table created")
    else:
        log("Log Table already exists, appending to it...")
        log_table.append()
        log("Appended to log table successfully.")

    if error is not None:
        raise Exception(error)


######################
# Pretreatment Tasks #
######################


@task
def transform_raw_to_nested_structure(
    pretreatment_steps: list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]],
    error: str,
    raw_filepath: str,
    source_filepath: str,
    timestamp: datetime,
    primary_key: Union[list, str] = None,
    print_inputs: bool = False,
    reader_args: dict = None,
) -> str:
    """
    Task to transform raw data to nested structure

    Args:
        pretreatment_steps (list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]]):
            List of pretreatment steps to be executed before the structure nesting.
            A pretreatment step is a function the can receive the following args:
                data (pd.DataFrame): the dataframe to treat
                timestamp (datetime): the timestamp argument
                primary_key (list): the list of primary keys
            and returns the treated pandas dataframe

        raw_filepath (str): Path to the saved raw .json file
        source_filepath (str): Path to the saved treated .csv file
        error (str): Error catched from upstream tasks
        timestamp (datetime): timestamp for flow run
        primary_key (list, optional): Primary key to be used on nested structure
        print_inputs (bool, optional): Flag to indicate if the task should log the data
        reader_args (dict): arguments to pass to pandas.read_csv or read_json

    Returns:
        str: Error traceback
        str: Path to the saved treated .csv file
    """
    if error is None:
        try:
            data = read_raw_data(filepath=raw_filepath, reader_args=reader_args)

            if print_inputs:
                log(
                    f"""
                    Received inputs:
                    - timestamp:\n{timestamp}
                    - data:\n{data.head()}"""
                )

            if data.empty:
                log("Empty dataframe, skipping transformation...")
                return

            log(f"Raw data:\n{data_info_str(data)}", level="info")

            primary_key = primary_key if isinstance(primary_key, list) else [primary_key]

            for step in pretreatment_steps:
                log(f"Starting treatment step: {step.__name__}...")
                data = step(data=data, timestamp=timestamp, primary_key=primary_key)
                log(f"Step {step.__name__} finished")

            log("Creating nested structure...", level="info")

            data = transform_to_nested_structure(data=data, primary_key=primary_key)

            timestamp = create_timestamp_captura(timestamp=timestamp)
            data["timestamp_captura"] = timestamp
            log(f"timestamp column = {timestamp}", level="info")

            log(
                f"Finished nested structure! Data:\n{data_info_str(data)}",
                level="info",
            )

            save_local_file(filepath=source_filepath, data=data)
            log(f"Data saved in {source_filepath}")

        except Exception:  # pylint: disable=W0703
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


#####################
# Incremental Tasks #
#####################


# @task(
#     nout=4,
#     max_retries=constants.MAX_RETRIES.value,
#     retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
# )
# def create_execution_strategy(
#     skip_if_running: bool,
#     redis_key: str,
#     timestamp: datetime,
#     incremental_type: Union[None, str],
#     max_incremental_window: Union[str, dict],
#     overwrite_start_value: Union[None, str, int],
#     overwrite_end_value: Union[None, str, int],
#     force_full: bool,
# ) -> tuple[Union[int, datetime, None], Union[int, datetime, None], str, bool]:
#     if incremental_type is None:
#         log("Incremental type is None, ending task...")
#         return None, None, "full", False

#     flag_save_redis = None
#     local_run = flow_is_running_local()
#     if local_run:
#         last_captured_value = overwrite_start_value
#     else:
#         log("Getting last captured value from Redis...")
#         redis_client = get_redis_client()
#         last_captured_value = redis_client.get(redis_key)

#     execution_mode = "full" if last_captured_value is None or force_full else "incr"
#     incremental_type = incremental_type.strip().lower()
#     log(f"Executing: {execution_mode}\nType: {incremental_type}")

#     log("Getting start and end values...")
#     match incremental_type:
#         case "datetime":
#             start, end, last_captured_value = datetime_type_incremental(
#                 last_captured_value=last_captured_value,
#                 timestamp=timestamp,
#                 max_incremental_window=max_incremental_window,
#                 overwrite_start_value=overwrite_start_value,
#                 overwrite_end_value=overwrite_end_value,
#                 execution_mode=execution_mode,
#             )
#         case "id":
#             start, end, last_captured_value = id_type_incremental(
#                 last_captured_value=last_captured_value,
#                 max_incremental_window=max_incremental_window,
#                 overwrite_start_value=overwrite_start_value,
#                 overwrite_end_value=overwrite_end_value,
#             )
#         case _:
#             raise NotImplementedError(
#                 f"incremental_type must be datetime or id, received {incremental_type}"
#             )

#     if start is not None and start > end:
#         raise ValueError(f"Start value is bigger than end value.\n\tstart = {start}\n
# \tend = {end}")

#     flag_save_redis = (
#         (last_captured_value is None or last_captured_value < end)
#         and skip_if_running
#         and not local_run
#     )

#     log(
#         f"""
#         Execution Strategy Creating finished:
#             Last Captured Value: {last_captured_value}
#             Start: {start}
#             End: {end}
#             Will save on Redis: {flag_save_redis}
#         """
#     )

#     return start, end, execution_mode, flag_save_redis


@task
def create_incremental_strategy(
    strategy_dict: dict,
    table: BQTable,
    force_full: bool,
    overwrite_start_value: Any,
    overwrite_end_value: Any,
) -> Union[dict, IncrementalStrategy]:
    if strategy_dict:
        incremental_strategy = incremental_strategy_from_dict(strategy_dict=strategy_dict)
        incremental_strategy.initialize(
            table=table,
            force_full=force_full,
            overwrite_start_value=overwrite_start_value,
            overwrite_end_value=overwrite_end_value,
        )

        log(
            f"""Incremental Strategy created:
            Mode: {incremental_strategy.incremental_info.execution_mode}
            Start Value: {incremental_strategy.incremental_info.start_value}
            End Value: {incremental_strategy.incremental_info.end_value}
            """
        )

        return incremental_strategy

    log(
        f"""Empty incremental:
            Mode: {constants.MODE_FULL.value}
            Start Value: {overwrite_start_value}
            End Value: {overwrite_end_value}
            """
    )
    return {
        "incremental_info": IncrementalInfo(
            start_value=overwrite_start_value,
            end_value=overwrite_end_value,
            execution_mode=constants.MODE_FULL.value,
        )
    }


# @task
# def get_last_value(
#     end_value: Union[datetime, str],
#     raw_filepath: str,
#     incremental_type: str,
#     incremental_reference_column: str,
# ) -> Union[datetime, int]:
#     match incremental_type.strip().lower():
#         case "datetime":
#             last_value = end_value
#         case "id":
#             df = read_raw_data(filepath=raw_filepath)
#             last_value = (
#                 df[incremental_reference_column]
#                 .dropna()
#                 .astype(str)
#                 .str.replace(".0", "")
#                 .astype(int)
#                 .max()
#             )
#         case _:
#             raise NotImplementedError(
#                 f"incremental_type must be datetime or id, received {incremental_type}"
#             )

#     return last_value


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def save_incremental_redis(
    incremental_strategy: Union[dict, IncrementalStrategy], raw_filepath: str
):
    if isinstance(incremental_strategy, IncrementalStrategy) and not flow_is_running_local():
        last_value = incremental_strategy.get_value_to_save(raw_filepath=raw_filepath)

        log(f"Last captured value: {last_value}")

        log("Saving new value on Redis")
        msg = incremental_strategy.save_on_redis(value_to_save=last_value)
        log(msg)
    else:
        log("Incremental_strategy parameter is None, save on Redis skipped")
