# -*- coding: utf-8 -*-
from datetime import datetime
from types import NoneType
from typing import Callable

import pandas as pd
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.core.function import FunctionTask
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.capture.templates.tasks import (
    create_incremental_strategy,
    create_source_dataset_id,
    create_table_object,
    get_raw_data,
    rename_capture_flow,
    save_incremental_redis,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
    validate_default_capture_flow_params,
)
from pipelines.constants import constants
from pipelines.tasks import get_current_timestamp, get_run_env, task_value_is_none
from pipelines.utils.prefect import TypedParameter
from pipelines.utils.pretreatment import strip_string_columns


def create_default_capture_flow(
    flow_name: str,
    create_extractor_task: FunctionTask,
    default_params: dict,
    agent_label: str,
    pretreatment_steps: list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]] = None,
):  # pylint: disable=R0914
    """
    Create capture flows

    Args:
        flow_name (str): Flow's name
        create_extractor_task (FunctionTask):
            The task to create the data extractor
            Can receive the arguments below:
                env: dev or prod
                category: parameter
                table_id: parameter
                save_filepath: local path to save raw data
                extract_params: parameter
                execution_mode: full or incr
                start_value: start value to filter the capture
                end_value: end value to filter the capture
            And must return a DataExtractor
        default_params (dict): Dict to overwrite params default values
        agent_label (str): Flow label
        pretreatment_steps (list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]], optional):
            List of pretreatment steps to be executed before the structure nesting.
            A pretreatment step is a function the can receive the following args:
                data (pd.DataFrame): the dataframe to treat
                timestamp (datetime): the timestamp argument
                primary_key (list): the list of primary keys
            and returns the treated pandas dataframe
            (default [strip_string_columns])


    Flow Parameters:
        source_name (str): Source system's name (eg.: jae)
        category (str): Target dataset_id (eg.: bilhetagem, subsidio)
        table_id (str): Table name on BigQuery
        force_full (bool, optional): Executes the flow to capture all data
        incremental_type (str, optional): id or datetime. If None will always execute full
        incremental_reference_column (str, optional): ID column name. Id incremental type only
        max_incremental_window (Union[int, dict], optional): Max window to capture:
            id type: the number of ids to capture
            datatime type: dict containing timedelta arguments (eg.: {"days": 1})
        start_value (Union[int, str], optional): Manually set the capture start value
        end_value (Union[int, str], optional): Manually set the capture end value
        extract_params (dict): Dict containing all extra information needed to make the capture
        raw_filetype (str, optional): The raw file type. default 'json')
        primary_key (Union[list, str], optional): Primary key name or list of primary keys
        partition_date_only (bool, optional): If True data will be partitioned only by date.
        partition_date_name (str, optional): Partition name (default 'data')
        save_bucket_name (str, optional): Bucket that the data will be saved

    Returns:
        Flow: The capture flow
    """
    if pretreatment_steps is None:
        pretreatment_steps = [strip_string_columns]

    with Flow(flow_name) as capture_flow:
        # Parâmetros Gerais #
        source_name = TypedParameter(
            name="source_name",
            default=default_params.get("source_name"),
            accepted_types=str,
        )
        category = TypedParameter(
            name="category",
            default=default_params.get("category"),
            accepted_types=str,
        )
        table_id = TypedParameter(
            name="table_id",
            default=default_params.get("table_id"),
            accepted_types=str,
        )

        # Parâmetros Incremental #
        force_full = TypedParameter(
            name="force_full",
            default=default_params.get("force_full", False),
            accepted_types=bool,
        )
        incremental_strategy = TypedParameter(
            name="incremental_strategy",
            default=default_params.get("incremental_strategy"),
            accepted_types=(dict, NoneType),
        )
        start_value = TypedParameter(
            name="start_value",
            default=default_params.get("start_value"),
            accepted_types=(str, int, NoneType),
        )
        end_value = TypedParameter(
            name="end_value",
            default=default_params.get("end_value"),
            accepted_types=(str, int, NoneType),
        )

        # Parâmetros para Captura #
        extract_params = Parameter("extract_params", default=default_params.get("extract_params"))
        raw_filetype = TypedParameter(
            name="raw_filetype",
            default=default_params.get("raw_filetype", "json"),
            accepted_types=str,
        )

        # Parâmetros para Pré-tratamento #
        primary_key = TypedParameter(
            name="primary_key",
            default=default_params.get("primary_key"),
            accepted_types=(list, str, NoneType),
        )
        pretreatment_reader_args = TypedParameter(
            name="pretreatment_reader_args",
            default=default_params.get("pretreatment_reader_args"),
            accepted_types=(dict, NoneType),
        )

        # Parâmetros para Carregamento de Dados #
        partition_date_only = TypedParameter(
            name="partition_date_only",
            default=default_params.get("partition_date_only", False),
            accepted_types=bool,
        )
        partition_date_name = TypedParameter(
            name="partition_date_name",
            default=default_params.get("partition_date_name", "data"),
            accepted_types=str,
        )
        save_bucket_name = TypedParameter(
            name="save_bucket_name",
            default=default_params.get("save_bucket_name"),
            accepted_types=(str, NoneType),
        )

        # Preparar execução #

        param_validation = validate_default_capture_flow_params()

        timestamp = get_current_timestamp(upstream_tasks=[param_validation])

        dataset_id = create_source_dataset_id(
            source_name=source_name,
            category=category,
            upstream_tasks=[param_validation],
        )

        env = get_run_env(upstream_tasks=[param_validation])

        table = create_table_object(
            env=env,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name=save_bucket_name,
            timestamp=timestamp,
            partition_date_name=partition_date_name,
            partition_date_only=partition_date_only,
            raw_filetype=raw_filetype,
        )

        incremental_strategy = create_incremental_strategy(
            strategy_dict=incremental_strategy,
            table=table,
            force_full=force_full,
            overwrite_start_value=start_value,
            overwrite_end_value=end_value,
        )

        incremental_values = incremental_strategy["incremental_data"]

        rename_flow_run = rename_capture_flow(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=timestamp,
            execution_mode=incremental_values["execution_mode"],
            start_value=start_value,
            end_value=end_value,
        )

        # Extração #

        data_extractor = create_extractor_task(
            env=env,
            category=category,
            table_id=table_id,
            save_filepath=table["raw_filepath"],
            extract_params=extract_params,
            incremental_data=incremental_values,
        )

        data_extractor.set_upstream(rename_flow_run)

        error = get_raw_data(data_extractor=data_extractor)

        error = upload_raw_file_to_gcs(error=error, table=table)

        # Pré-tratamento #

        error = transform_raw_to_nested_structure(
            pretreatment_steps=pretreatment_steps,
            error=error,
            raw_filepath=table["raw_filepath"],
            source_filepath=table["source_filepath"],
            timestamp=timestamp,
            primary_key=primary_key,
            print_inputs=task_value_is_none(task_value=save_bucket_name),
            reader_args=pretreatment_reader_args,
        )

        upload_source_gcs = upload_source_data_to_gcs(error=error, table=table)

        # Finalizar Flow #

        save_incremental_redis(
            incremental_strategy=incremental_strategy,
            raw_filepath=table["raw_filepath"],
            upstream_tasks=[upload_source_gcs],
        )

    capture_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    capture_flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[agent_label],
    )
    capture_flow.state_handlers = [
        handler_inject_bd_credentials,
        handler_skip_if_running,
    ]

    return capture_flow
