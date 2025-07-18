# -*- coding: utf-8 -*-
# pylint: disable=W0703, W0511
"""
Tasks for rj_smtr
"""
import io
import json
import os
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import basedosdados as bd
import pandas as pd
import pendulum
import prefect
import requests
from basedosdados import Storage, Table
from pandas_gbq.exceptions import GenericGBQException
from prefect import Client, task
from prefect.backend import FlowRunView
from prefeitura_rio.pipelines_utils.dbt import run_dbt_model as run_dbt_model_func
from prefeitura_rio.pipelines_utils.infisical import inject_bd_credentials
from prefeitura_rio.pipelines_utils.io import get_root_path
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.migration.utils import (
    bq_project,
    create_or_append_table,
    data_info_str,
    dict_contains_keys,
    get_datetime_range,
    get_last_run_timestamp,
    get_raw_data_api,
    get_raw_data_db,
    get_raw_data_gcs,
    get_raw_recursos,
    get_table_min_max_value,
    log_critical,
    read_raw_data,
    save_raw_local_func,
    save_treated_local_func,
    upload_run_logs_to_bq,
)
from pipelines.utils.pretreatment import transform_to_nested_structure
from pipelines.utils.secret import get_secret


###############
#
# SETUP
#
###############
@task
def setup_task():
    return inject_bd_credentials()


@task
def get_current_flow_labels() -> List[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


###############
#
# DBT
#
###############


@task
def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    dbt_alias: bool = False,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: dict | List[Dict] = None,
):
    if not _vars:
        _vars = {}
    if isinstance(_vars, dict):
        _vars["flow_name"] = prefect.context.flow_name
    else:
        for a in _vars:
            a["flow_name"] = prefect.context.flow_name

    return run_dbt_model_func(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        upstream=upstream,
        downstream=downstream,
        exclude=exclude,
        flags=flags,
        _vars=_vars,
    )


# @task(max_retries=3, retry_delay=timedelta(seconds=10))
# def build_incremental_model(  # pylint: disable=too-many-arguments
#     dataset_id: str,
#     base_table_id: str,
#     mat_table_id: str,
#     field_name: str = "data_versao",
#     refresh: bool = False,
#     wait=None,  # pylint: disable=unused-argument
# ):
#     """
#         Utility task for backfilling table in predetermined steps.
#         Assumes the step sizes will be defined on the .sql file.

#     Args:
#         dbt_client (DbtClient): DBT interface object
#         dataset_id (str): Dataset id on BigQuery
#         base_table_id (str): Base table from which to materialize (usually, an external table)
#         mat_table_id (str): Target table id for materialization
#         field_name (str, optional): Key field (column) for dbt incremental filters.
#         Defaults to "data_versao".
#         refresh (bool, optional): If True, rebuild the table from scratch. Defaults to False.
#         wait (NoneType, optional): Placeholder parameter, used to wait previous tasks finish.
#         Defaults to None.

#     Returns:
#         bool: whether the table was fully built or not.
#     """

#     query_project_id = bq_project()
#     last_mat_date = get_table_min_max_value(
#         query_project_id, dataset_id, mat_table_id, field_name, "max"
#     )
#     last_base_date = get_table_min_max_value(
#         query_project_id, dataset_id, base_table_id, field_name, "max"
#     )
#     log(
#         f"""
#     Base table last version: {last_base_date}
#     Materialized table last version: {last_mat_date}
#     """
#     )
#     run_command = f"run --select models/{dataset_id}/{mat_table_id}.sql"

#     if refresh:
#         log("Running in full refresh mode")
#         log(f"DBT will run the following command:\n{run_command+' --full-refresh'}")
#         run_dbt_model_func(dataset_id=dataset_id, table_id=mat_table_id, flags="--full-refresh")
#         last_mat_date = get_table_min_max_value(
#             query_project_id, dataset_id, mat_table_id, field_name, "max"
#         )

#     if last_base_date > last_mat_date:
#         log("Running interval step materialization")
#         log(f"DBT will run the following command:\n{run_command}")
#         while last_base_date > last_mat_date:
#             running = run_dbt_model_func(dataset_id=dataset_id, table_id=mat_table_id)
#             # running = dbt_client.cli(run_command, sync=True)
#             last_mat_date = get_table_min_max_value(
#                 query_project_id,
#                 dataset_id,
#                 mat_table_id,
#                 field_name,
#                 "max",
#                 wait=running,
#             )
#             log(f"After this step, materialized table last version is: {last_mat_date}")
#             if last_mat_date == last_base_date:
#                 log("Materialized table reached base table version!")
#                 return True
#     log("Did not run interval step materialization...")
#     return False


@task(checkpoint=False, nout=3)
def create_dbt_run_vars(
    dataset_id: str,
    dbt_vars: dict,
    table_id: str,
    raw_dataset_id: str,
    raw_table_id: str,
    mode: str,
    timestamp: datetime,
) -> tuple[list[dict], Union[list[dict], dict, None], bool]:
    """
    Create the variables to be used in dbt materialization based on a dict

    Args:
        dataset_id (str): the dataset_id to get the variables
        dbt_vars (dict): dict containing the parameters
        table_id (str): the table_id get the date_range variable
        raw_dataset_id (str): the raw_dataset_id get the date_range variable
        raw_table_id (str): the raw_table_id get the date_range variable
        mode (str): the mode to get the date_range variable

    Returns:
        list[dict]: the variables to be used in DBT
        Union[list[dict], dict, None]: the date variable (date_range or run_date)
        bool: a flag that indicates if the date_range variable came from Redis
    """

    log(f"Creating DBT variables. Parameter received: {dbt_vars}")

    if not dbt_vars:
        log("dbt_vars are blank. Skiping task...")
        return [None], None, False

    final_vars = []
    date_var = None
    flag_date_range = False

    if "date_range" in dbt_vars.keys():
        log("Creating date_range variable")

        # Set date_range variable manually
        if dict_contains_keys(dbt_vars["date_range"], ["date_range_start", "date_range_end"]):
            date_var = {
                "date_range_start": dbt_vars["date_range"]["date_range_start"],
                "date_range_end": dbt_vars["date_range"]["date_range_end"],
            }
        # Create date_range using Redis
        else:
            if not table_id:
                log("table_id is blank. Skiping task...")
                return [None], None, False

            raw_table_id = raw_table_id or table_id

            date_var = get_materialization_date_range.run(
                dataset_id=dataset_id,
                table_id=dbt_vars["date_range"].get("table_alias", table_id),
                raw_dataset_id=raw_dataset_id,
                raw_table_id=raw_table_id,
                table_run_datetime_column_name=dbt_vars["date_range"].get(
                    "table_run_datetime_column_name"
                ),
                mode=mode,
                delay_hours=dbt_vars["date_range"].get("delay_hours", 0),
                end_ts=timestamp,
                truncate_minutes=dbt_vars["date_range"].get("truncate_minutes", True),
            )

            flag_date_range = True

        final_vars.append(date_var.copy())

        log(f"date_range created: {date_var}")

    elif "run_date" in dbt_vars.keys():
        log("Creating run_date variable")

        date_var = get_run_dates.run(
            date_range_start=dbt_vars["run_date"].get("date_range_start", False),
            date_range_end=dbt_vars["run_date"].get("date_range_end", False),
            day_datetime=timestamp,
        )

        final_vars += [d.copy() for d in date_var]

        log(f"run_date created: {date_var}")

    elif "data_versao_gtfs" in dbt_vars.keys():
        log("Creating data_versao_gtfs variable")

        date_var = {"data_versao_gtfs": dbt_vars["data_versao_gtfs"]}

        final_vars.append(date_var.copy())

    if "version" in dbt_vars.keys():
        log("Creating version variable")
        dataset_sha = fetch_dataset_sha.run(dataset_id=dataset_id)

        # if there are other variables inside the list, update each item adding the version variable
        if final_vars:
            final_vars = get_join_dict.run(dict_list=final_vars, new_dict=dataset_sha)
        else:
            final_vars.append(dataset_sha)

        log(f"version created: {dataset_sha}")

    log(f"All variables was created, final value is: {final_vars}")

    return final_vars, date_var, flag_date_range


###############
#
# Local file management
#
###############


@task
def get_rounded_timestamp(
    timestamp: Union[str, datetime, None] = None,
    interval_minutes: Union[int, None] = None,
) -> datetime:
    """
    Calculate rounded timestamp for flow run.

    Args:
        timestamp (Union[str, datetime, None]): timestamp to be used as reference
        interval_minutes (Union[int, None], optional): interval in minutes between each recapture

    Returns:
        datetime: timestamp for flow run
    """
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)

    if not timestamp:
        timestamp = datetime.now(tz=timezone(constants.TIMEZONE.value))

    timestamp = timestamp.replace(second=0, microsecond=0)

    if interval_minutes:
        if interval_minutes >= 60:
            hours = interval_minutes / 60
            interval_minutes = round(((hours) % 1) * 60)

        if interval_minutes == 0:
            rounded_minutes = interval_minutes
        else:
            rounded_minutes = (timestamp.minute // interval_minutes) * interval_minutes

        timestamp = timestamp.replace(minute=rounded_minutes)

    return timestamp


@task
def get_current_timestamp(
    timestamp=None, truncate_minute: bool = True, return_str: bool = False
) -> Union[datetime, str]:
    """
    Get current timestamp for flow run.

    Args:
        timestamp: timestamp to be used as reference (optionally, it can be a string)
        truncate_minute: whether to truncate the timestamp to the minute or not
        return_str: if True, the return will be an isoformatted datetime string
                    otherwise it returns a datetime object

    Returns:
        Union[datetime, str]: timestamp for flow run
    """
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    if not timestamp:
        timestamp = datetime.now(tz=timezone(constants.TIMEZONE.value))
    if truncate_minute:
        timestamp = timestamp.replace(second=0, microsecond=0)
    if return_str:
        timestamp = timestamp.isoformat()

    return timestamp


@task
def create_date_hour_partition(
    timestamp: datetime,
    partition_date_name: str = "data",
    partition_date_only: bool = False,
) -> str:
    """
    Create a date (and hour) Hive partition structure from timestamp.

    Args:
        timestamp (datetime): timestamp to be used as reference
        partition_date_name (str, optional): partition name. Defaults to "data".
        partition_date_only (bool, optional): whether to add hour partition or not

    Returns:
        str: partition string
    """
    partition = f"{partition_date_name}={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition += f"/hora={timestamp.strftime('%H')}"
    return partition


@task
def parse_timestamp_to_string(timestamp: datetime, pattern="%Y-%m-%d-%H-%M-%S") -> str:
    """
    Parse timestamp to string pattern.
    """
    return timestamp.strftime(pattern)


@task
def create_local_partition_path(
    dataset_id: str, table_id: str, filename: str, partitions: str = None
) -> str:
    """
    Create the full path sctructure which to save data locally before
    upload.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filename (str, optional): Single csv name
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01"
    Returns:
        str: String path having `mode` and `filetype` to be replaced afterwards,
    either to save raw or staging files.
    """
    data_folder = os.getenv("DATA_FOLDER", "data")
    root = str(get_root_path())
    file_path = f"{root}/{data_folder}/{{mode}}/{dataset_id}/{table_id}"
    file_path += f"/{partitions}/{filename}.{{filetype}}"
    log(f"Creating file path: {file_path}")
    return file_path


@task
def save_raw_local(file_path: str, status: dict, mode: str = "raw") -> str:
    """
    Saves json response from API to .json file.
    Args:
        file_path (str): Path which to save raw file
        status (dict): Must contain keys
          * data: json returned from API
          * error: error catched from API request
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.
    Returns:
        str: Path to the saved file
    """
    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        json.dump(status["data"], Path(_file_path).open("w", encoding="utf-8"))
        log(f"Raw data saved to: {_file_path}")
    return _file_path


@task
def save_treated_local(file_path: str, status: dict, mode: str = "staging") -> str:
    """
    Save treated file to CSV.

    Args:
        file_path (str): Path which to save treated file
        status (dict): Must contain keys
          * `data`: dataframe returned from treatement
          * `error`: error catched from data treatement
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """

    log(f"Saving treated data to: {file_path}, {status}")

    _file_path = file_path.format(mode=mode, filetype="csv")

    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        status["data"].to_csv(_file_path, index=False)
        log(f"Treated data saved to: {_file_path}")

    return _file_path


###############
#
# Extract data
#
###############
@task(nout=3, max_retries=3, retry_delay=timedelta(seconds=5))
def query_logs(
    dataset_id: str,
    table_id: str,
    datetime_filter=None,
    max_recaptures: int = 60,
    interval_minutes: int = 1,
    recapture_window_days: int = 1,
):
    """
    Queries capture logs to check for errors

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        datetime_filter (pendulum.datetime.DateTime, optional):
        filter passed to query. This task will query the logs table
        for the last n (n = recapture_window_days) days before datetime_filter
        max_recaptures (int, optional): maximum number of recaptures to be done
        interval_minutes (int, optional): interval in minutes between each recapture
        recapture_window_days (int, optional): Number of days to query for erros

    Returns:
        lists: errors (bool),
        timestamps (list of pendulum.datetime.DateTime),
        previous_errors (list of previous errors)
    """

    if not datetime_filter:
        datetime_filter = pendulum.now(constants.TIMEZONE.value).replace(second=0, microsecond=0)
    elif isinstance(datetime_filter, str):
        datetime_filter = datetime.fromisoformat(datetime_filter).replace(second=0, microsecond=0)

    datetime_filter = datetime_filter.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
    WITH
        t AS (
        SELECT
            DATETIME(timestamp_array) AS timestamp_array
        FROM
            UNNEST(
                GENERATE_TIMESTAMP_ARRAY(
                    TIMESTAMP_SUB('{datetime_filter}', INTERVAL {recapture_window_days} day),
                    TIMESTAMP('{datetime_filter}'),
                    INTERVAL {interval_minutes} minute) )
            AS timestamp_array
        WHERE
            timestamp_array < '{datetime_filter}' ),
        logs_table AS (
            SELECT
                SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura),
                        "America/Sao_Paulo") AS DATETIME) timestamp_captura,
                SAFE_CAST(sucesso AS BOOLEAN) sucesso,
                SAFE_CAST(erro AS STRING) erro,
                SAFE_CAST(DATA AS DATE) DATA
            FROM
                {bq_project(kind='bigquery_staging')}.{dataset_id}_staging.{table_id}_logs AS t
        ),
        logs AS (
            SELECT
                *,
                TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
            FROM
                logs_table
            WHERE
                DATA BETWEEN DATE(DATETIME_SUB('{datetime_filter}',
                                INTERVAL {recapture_window_days} day))
                AND DATE('{datetime_filter}')
                AND timestamp_captura BETWEEN
                    DATETIME_SUB('{datetime_filter}', INTERVAL {recapture_window_days} day)
                AND '{datetime_filter}'
        )
    SELECT
        CASE
            WHEN logs.timestamp_captura IS NOT NULL THEN logs.timestamp_captura
        ELSE
            t.timestamp_array
        END
            AS timestamp_captura,
            logs.erro
    FROM
        t
    LEFT JOIN
        logs
    ON
        logs.timestamp_array = t.timestamp_array
    WHERE
        logs.sucesso IS NOT TRUE
    """
    log(f"Run query to check logs:\n{query}")
    results = bd.read_sql(query=query, billing_project_id=bq_project())

    if len(results) > 0:
        results = results.sort_values(["timestamp_captura"])
        results["timestamp_captura"] = (
            pd.to_datetime(results["timestamp_captura"])
            .dt.tz_localize(constants.TIMEZONE.value)
            .to_list()
        )
        log(f"Recapture data for the following {len(results)} timestamps:\n{results}")
        if len(results) > max_recaptures:
            message = f"""
            [SPPO - Recaptures]
            Encontradas {len(results)} timestamps para serem recapturadas.
            Essa run processará as seguintes:
            #####
            {results[:max_recaptures]}
            #####
            Sobraram as seguintes para serem recapturadas na próxima run:
            #####
            {results[max_recaptures:]}
            #####
            """
            log_critical(message)

            results = results[:max_recaptures]
        return True, results["timestamp_captura"].to_list(), results["erro"].to_list()
    return False, [], []


@task
def get_raw(  # pylint: disable=R0912
    url: str,
    headers: str = None,
    filetype: str = "json",
    csv_args: dict = None,
    params: dict = None,
) -> Dict:
    """
    Request data from URL API

    Args:
        url (str): URL to send request
        headers (str, optional): Path to headers guardeded on Vault, if needed.
        filetype (str, optional): Filetype to be formatted (supported only: json, csv and txt)
        csv_args (dict, optional): Arguments for read_csv, if needed
        params (dict, optional): Params to be sent on request

    Returns:
        dict: Containing keys
          * `data` (json): data result
          * `error` (str): catched error, if any. Otherwise, returns None
    """
    data = None
    error = None

    try:
        if headers is not None:
            headers = get_secret(secret_path=headers)
            # remove from headers, if present
            remove_headers = ["host", "databases"]
            for remove_header in remove_headers:
                if remove_header in list(headers.keys()):
                    del headers[remove_header]

        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS.value,
            params=params,
        )

        if response.ok:  # status code is less than 400
            if not response.content and url in [
                constants.GPS_SPPO_API_BASE_URL_V2.value,
                constants.GPS_SPPO_API_BASE_URL.value,
            ]:
                error = "Dados de GPS vazios"

            if filetype == "json":
                data = response.json()

                # todo: move to data check on specfic API # pylint: disable=W0102
                if isinstance(data, dict) and "DescricaoErro" in data.keys():
                    error = data["DescricaoErro"]

            elif filetype in ("txt", "csv"):
                if csv_args is None:
                    csv_args = {}
                data = pd.read_csv(io.StringIO(response.text), **csv_args).to_dict(orient="records")
            else:
                error = "Unsupported raw file extension. Supported only: json, csv and txt"

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}


@task(checkpoint=False, nout=2)
def create_request_params(
    extract_params: dict,
    table_id: str,
    dataset_id: str,
    timestamp: datetime,
    interval_minutes: int,
) -> tuple[str, str]:
    """
    Task to create request params

    Args:
        extract_params (dict): extract parameters
        table_id (str): table_id on BigQuery
        dataset_id (str): dataset_id on BigQuery
        timestamp (datetime): timestamp for flow run
        interval_minutes (int): interval in minutes between each capture

    Returns:
        request_params: host, database and query to request data
        request_url: url to request data
    """
    request_params = None
    request_url = None

    if dataset_id == constants.BILHETAGEM_DATASET_ID.value:
        database = constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["databases"][
            extract_params["database"]
        ]
        request_url = database["host"]

        request_params = {
            "database": extract_params["database"],
            "engine": database["engine"],
            "query": extract_params["query"],
        }

        if table_id == constants.BILHETAGEM_TRACKING_CAPTURE_PARAMS.value["table_id"]:
            project = bq_project(kind="bigquery_staging")
            log(f"project = {project}")

            logs_query = f"""
            SELECT
                timestamp_captura
            FROM
                `{project}.{dataset_id}_staging.{table_id}_logs`
            WHERE
                DATE(data) BETWEEN
                    DATE_SUB('{timestamp.strftime("%Y-%m-%d")}', INTERVAL 7 DAY)
                    AND DATE('{timestamp.strftime("%Y-%m-%d")}')
                AND sucesso = "True"
            ORDER BY
                timestamp_captura DESC
            """
            last_success_dates = bd.read_sql(query=logs_query, billing_project_id=project)
            last_success_dates = last_success_dates.iloc[:, 0].to_list()
            for success_ts in last_success_dates:
                success_ts = datetime.fromisoformat(success_ts)
                last_id_query = f"""
                SELECT
                    CAST(MAX(CAST(id AS INTEGER)) AS STRING)
                FROM
                    `{project}.{dataset_id}_staging.{table_id}`
                WHERE
                    data = '{success_ts.strftime("%Y-%m-%d")}'
                    and hora = "{success_ts.strftime("%H")}";
                """

                last_captured_id = bd.read_sql(query=last_id_query, billing_project_id=project)
                last_captured_id = last_captured_id.iloc[0][0]
                if last_captured_id is None:
                    print("ID is None, trying next timestamp")
                else:
                    log(f"last_captured_id = {last_captured_id}")
                    break

            request_params["query"] = request_params["query"].format(
                last_id=last_captured_id,
                max_id=int(last_captured_id)
                + extract_params["page_size"] * extract_params["max_pages"],
            )
            request_params["page_size"] = extract_params["page_size"]
            request_params["max_pages"] = extract_params["max_pages"]
        else:
            if "get_updates" in extract_params.keys():
                project = bq_project()
                log(f"project = {project}")
                columns_to_concat_bq = [c.split(".")[-1] for c in extract_params["get_updates"]]
                concat_arg = ",'_',"

                try:
                    query = f"""
                    SELECT
                        CONCAT("'", {concat_arg.join(columns_to_concat_bq)}, "'")
                    FROM
                        `{project}.{dataset_id}_staging.{table_id}`
                    """
                    log(query)
                    last_values = bd.read_sql(query=query, billing_project_id=project)

                    last_values = last_values.iloc[:, 0].to_list()
                    last_values = ", ".join(last_values)
                    update_condition = f"""CONCAT(
                            {concat_arg.join(extract_params['get_updates'])}
                        ) NOT IN ({last_values})
                    """

                except GenericGBQException as err:
                    if "404 Not found" in str(err):
                        log("table not found, setting updates to 1=1")
                        update_condition = "1=1"

                request_params["query"] = request_params["query"].format(update=update_condition)

            datetime_range = get_datetime_range(
                timestamp=timestamp, interval=timedelta(minutes=interval_minutes)
            )

            request_params["query"] = request_params["query"].format(**datetime_range)

    elif dataset_id == constants.GTFS_DATASET_ID.value:
        request_params = {"zip_filename": extract_params["filename"]}

    elif dataset_id == constants.SUBSIDIO_SPPO_RECURSOS_DATASET_ID.value:
        request_params = {}
        data_recurso = extract_params.get("data_recurso", timestamp)
        if isinstance(data_recurso, str):
            data_recurso = datetime.fromisoformat(data_recurso)
        extract_params["token"] = get_secret(constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value)[
            "token"
        ]
        start = datetime.strftime(
            data_recurso - timedelta(minutes=interval_minutes), "%Y-%m-%dT%H:%M:%S.%MZ"
        )
        end = datetime.strftime(data_recurso, "%Y-%m-%dT%H:%M:%S.%MZ")
        log(f" Start date {start}, end date {end}")

        service = constants.SUBSIDIO_SPPO_RECURSO_TABLE_CAPTURE_PARAMS.value[table_id]

        recurso_params = {
            "start": start,
            "end": end,
            "service": service,
        }

        extract_params["$filter"] = extract_params["$filter"].format(**recurso_params)

        request_params = extract_params

        request_url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value

    elif dataset_id == constants.STU_DATASET_ID.value:
        request_params = {"bucket_name": constants.STU_BUCKET_NAME.value}

    elif dataset_id == constants.VEICULO_DATASET_ID.value:
        request_url = get_secret(extract_params["secret_path"])["request_url"]

    elif dataset_id == constants.VIAGEM_ZIRIX_RAW_DATASET_ID.value:
        request_url = f"{constants.ZIRIX_BASE_URL.value}/EnvioViagensConsolidadas"
        delay_minutes = extract_params["delay_minutes"]
        token = get_secret(constants.ZIRIX_API_SECRET_PATH.value)
        token_key = list(token)[0]
        request_params = {
            "data_inicial": (
                timestamp - timedelta(minutes=delay_minutes + interval_minutes)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "data_final": (timestamp - timedelta(minutes=delay_minutes)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            token_key: get_secret(constants.ZIRIX_API_SECRET_PATH.value)[token_key],
        }
        log(
            f"""Params:
            data_inicial: {request_params['data_inicial']}
            data_final: {request_params['data_final']}"""
        )

    elif dataset_id == constants.CONTROLE_FINANCEIRO_DATASET_ID.value:
        request_url = extract_params["base_url"] + extract_params["sheet_id"]

    return request_params, request_url


# @task(checkpoint=False, nout=2)
# def create_request_params(
#     extract_params: dict,
#     table_id: str,
#     dataset_id: str,
#     timestamp: datetime,
#     interval_minutes: int,
# ) -> tuple[str, str]:
#     """
#     Task to create request params

#     Args:
#         extract_params (dict): extract parameters
#         table_id (str): table_id on BigQuery
#         dataset_id (str): dataset_id on BigQuery
#         timestamp (datetime): timestamp for flow run
#         interval_minutes (int): interval in minutes between each capture

#     Returns:
#         request_params: host, database and query to request data
#         request_url: url to request data
#     """
#     request_params = None
#     request_url = None

#     if dataset_id == constants.BILHETAGEM_DATASET_ID.value:
#         database = constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["databases"][
#             extract_params["database"]
#         ]
#         request_url = database["host"]

#         datetime_range = get_datetime_range(
#             timestamp=timestamp, interval=timedelta(minutes=interval_minutes)
#         )

#         request_params = {
#             "database": extract_params["database"],
#             "engine": database["engine"],
#             "query": extract_params["query"].format(**datetime_range),
#         }

#     elif dataset_id == constants.GTFS_DATASET_ID.value:
#         request_params = extract_params["filename"]

#     elif dataset_id == constants.SUBSIDIO_SPPO_RECURSOS_DATASET_ID.value:
#         extract_params["token"] = get_secret(
#             secret_path=constants.SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH.value
#         )["token"]
#         start = datetime.strftime(
#             timestamp - timedelta(minutes=interval_minutes), "%Y-%m-%dT%H:%M:%S.%MZ"
#         )
#         end = datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%MZ")
#         log(f" Start date {start}, end date {end}")
#         recurso_params = {
#             "dates": f"createdDate ge {start} and createdDate le {end}",
#             "service": constants.SUBSIDIO_SPPO_RECURSO_SERVICE.value,
#         }
#         extract_params["$filter"] = extract_params["$filter"].format(**recurso_params)
#         request_params = extract_params

#         request_url = constants.SUBSIDIO_SPPO_RECURSO_API_BASE_URL.value

#     return request_params, request_url


@task(checkpoint=False, nout=2)
def get_raw_from_sources(
    source_type: str,
    local_filepath: str,
    source_path: str = None,
    dataset_id: str = None,
    table_id: str = None,
    secret_path: str = None,
    request_params: dict = None,
) -> tuple[str, str]:
    """
    Task to get raw data from sources

    Args:
        source_type (str): source type
        local_filepath (str): local filepath
        source_path (str, optional): source path. Defaults to None.
        dataset_id (str, optional): dataset_id on BigQuery. Defaults to None.
        table_id (str, optional): table_id on BigQuery. Defaults to None.
        secret_path (str, optional): secret path. Defaults to None.
        request_params (dict, optional): request parameters. Defaults to None.

    Returns:
        error: error catched from upstream tasks
        filepath: filepath to raw data
    """
    error = None
    filepath = None
    data = None

    source_values = source_type.split("-", 1)

    source_type, filetype = source_values if len(source_values) == 2 else (source_values[0], None)

    log(f"Getting raw data from source type: {source_type}")

    try:
        if source_type == "api":
            error, data, filetype = get_raw_data_api(
                url=source_path,
                secret_path=secret_path,
                api_params=request_params,
                filetype=filetype,
            )
        elif source_type == "gcs":
            error, data, filetype = get_raw_data_gcs(
                dataset_id=dataset_id, table_id=table_id, zip_filename=request_params
            )
        elif source_type == "db":
            error, data, filetype = get_raw_data_db(
                host=source_path, secret_path=secret_path, **request_params
            )
        elif source_type == "movidesk":
            error, data, filetype = get_raw_recursos(
                request_url=source_path, request_params=request_params
            )
        else:
            raise NotImplementedError(f"{source_type} not supported")

        filepath = save_raw_local_func(data=data, filepath=local_filepath, filetype=filetype)

    except NotImplementedError:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    log(f"Raw extraction ended returned values: {error}, {filepath}")
    return error, filepath


###############
#
# Load data
#
###############


@task
def bq_upload(
    dataset_id: str,
    table_id: str,
    filepath: str,
    raw_filepath: str = None,
    partitions: str = None,
    status: dict = None,
):  # pylint: disable=R0913
    """
    Upload raw and treated data to GCS and BigQuery.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filepath (str): Path to the saved treated .csv file
        raw_filepath (str, optional): Path to raw .json file. Defaults to None.
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01".
        Defaults to None.
        status (dict, optional): Dict containing `error` key from
        upstream tasks.

    Returns:
        None
    """
    log(
        f"""
    Received inputs:
    raw_filepath = {raw_filepath}, type = {type(raw_filepath)}
    treated_filepath = {filepath}, type = {type(filepath)}
    dataset_id = {dataset_id}, type = {type(dataset_id)}
    table_id = {table_id}, type = {type(table_id)}
    partitions = {partitions}, type = {type(partitions)}
    """
    )
    if status["error"] is not None:
        return status["error"]

    error = None

    try:
        # Upload raw to staging
        if raw_filepath:
            st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
            log(
                f"""Uploading raw file to bucket {st_obj.bucket_name} at
                {st_obj.bucket_name}/{dataset_id}/{table_id}"""
            )
            st_obj.upload(
                path=raw_filepath,
                partitions=partitions,
                mode="raw",
                if_exists="replace",
            )

        # Creates and publish table if it does not exist, append to it otherwise
        create_or_append_table(
            dataset_id=dataset_id,
            table_id=table_id,
            path=filepath,
            partitions=partitions,
        )
    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


@task
def bq_upload_from_dict(paths: dict, dataset_id: str, partition_levels: int = 1):
    """Upload multiple tables from a dict structured as {table_id: csv_path}.
        Present use case assumes table partitioned once. Adjust the parameter
        'partition_levels' to best suit new uses.
        i.e. if your csv is saved as:
            <table_id>/date=<run_date>/<filename>.csv
        it has 1 level of partition.
        if your csv file is saved as:
            <table_id>/date=<run_date>/hour=<run_hour>/<filename>.csv
        it has 2 levels of partition

    Args:
        paths (dict): _description_
        dataset_id (str): _description_

    Returns:
        _type_: _description_
    """
    for key in paths.keys():
        log("#" * 80)
        log(f"KEY = {key}")
        tb_dir = paths[key].parent
        # climb up the partition directories to reach the table dir
        for i in range(partition_levels):  # pylint: disable=unused-variable
            tb_dir = tb_dir.parent
        log(f"tb_dir = {tb_dir}")
        create_or_append_table(dataset_id=dataset_id, table_id=key, path=tb_dir)

    log(f"Returning -> {tb_dir.parent}")

    return tb_dir.parent


@task
def upload_logs_to_bq(  # pylint: disable=R0913
    dataset_id: str,
    parent_table_id: str,
    timestamp: str,
    error: str = None,
    previous_error: str = None,
    recapture: bool = False,
):
    """
    Upload execution status table to BigQuery.
    Table is uploaded to the same dataset, named {parent_table_id}_logs.
    If passing status_dict, should not pass timestamp and error.

    Args:
        dataset_id (str): dataset_id on BigQuery
        parent_table_id (str): Parent table id related to the status table
        timestamp (str): ISO formatted timestamp string
        error (str, optional): String associated with error caught during execution
    Returns:
        None
    """
    table_id = parent_table_id + "_logs"
    # Create partition directory
    filename = f"{table_id}_{timestamp.isoformat()}"
    partition = f"data={timestamp.date()}"
    filepath = Path(f"""data/staging/{dataset_id}/{table_id}/{partition}/{filename}.csv""")
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # Create dataframe to be uploaded
    if not error and recapture is True:
        # if the recapture is succeeded, update the column erro
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [True],
                "erro": [f"[recapturado]{previous_error}"],
            }
        )
        log(f"Recapturing {timestamp} with previous error:\n{error}")
    else:
        # not recapturing or error during flow execution
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [error is None],
                "erro": [error],
            }
        )
    # Save data local
    dataframe.to_csv(filepath, index=False)
    # Upload to Storage
    create_or_append_table(
        dataset_id=dataset_id,
        table_id=table_id,
        path=filepath.as_posix(),
        partitions=partition,
    )
    if error is not None:
        raise Exception(f"Pipeline failed with error: {error}")


@task
def upload_raw_data_to_gcs(
    error: str,
    raw_filepath: str,
    table_id: str,
    dataset_id: str,
    partitions: list,
    bucket_name: str = None,
) -> Union[str, None]:
    """
    Upload raw data to GCS.

    Args:
        error (str): Error catched from upstream tasks.
        raw_filepath (str): Path to the saved raw .json file
        table_id (str): table_id on BigQuery
        dataset_id (str): dataset_id on BigQuery
        partitions (list): list of partition strings

    Returns:
        Union[str, None]: if there is an error returns it traceback, otherwise returns None
    """
    if error is None:
        try:
            st_obj = Storage(table_id=table_id, dataset_id=dataset_id, bucket_name=bucket_name)
            log(
                f"""Uploading raw file to bucket {st_obj.bucket_name} at
                {st_obj.bucket_name}/{dataset_id}/{table_id}"""
            )
            st_obj.upload(
                path=raw_filepath,
                partitions=partitions,
                mode="raw",
                if_exists="replace",
            )
        except Exception:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error


@task
def upload_staging_data_to_gcs(
    error: str,
    staging_filepath: str,
    timestamp: datetime,
    table_id: str,
    dataset_id: str,
    partitions: str,
    previous_error: str = None,
    recapture: bool = False,
    bucket_name: str = None,
) -> Union[str, None]:
    """
    Upload staging data to GCS.

    Args:
        error (str): Error catched from upstream tasks.
        staging_filepath (str): Path to the saved treated .csv file.
        timestamp (datetime): timestamp for flow run.
        table_id (str): table_id on BigQuery.
        dataset_id (str): dataset_id on BigQuery.
        partitions (str): partition string.
        previous_error (str, Optional): Previous error on recaptures.
        recapture: (bool, Optional): Flag that indicates if the run is recapture or not.
        bucket_name (str, Optional): The bucket name to save the data.

    Returns:
        Union[str, None]: if there is an error returns it traceback, otherwise returns None
    """
    log(f"FILE PATH: {staging_filepath}")
    if error is None:
        try:
            # Creates and publish table if it does not exist, append to it otherwise
            create_or_append_table(
                dataset_id=dataset_id,
                table_id=table_id,
                path=staging_filepath,
                partitions=partitions,
                bucket_name=bucket_name,
            )
        except Exception:
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    upload_run_logs_to_bq(
        dataset_id=dataset_id,
        parent_table_id=table_id,
        error=error,
        timestamp=timestamp,
        mode="staging",
        previous_error=previous_error,
        recapture=recapture,
        bucket_name=bucket_name,
    )

    return error


###############
#
# Daterange tasks
#
###############


@task(
    checkpoint=False,
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_materialization_date_range(  # pylint: disable=R0913
    dataset_id: str,
    table_id: str,
    raw_dataset_id: str,
    raw_table_id: str,
    table_run_datetime_column_name: str = None,
    mode: str = "prod",
    delay_hours: int = 0,
    end_ts: datetime = None,
    truncate_minutes: bool = True,
):
    """
    Task for generating dict with variables to be passed to the
    --vars argument on DBT.
    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        eg: if you have a model defined in the file <filename>.sql,
        the table_id should be <filename>
        table_date_column_name (Optional, str): if it's the first time this
        is ran, will query the table for the maximum value on this field.
        If rebuild is true, will query the table for the minimum value
        on this field.
        rebuild (Optional, bool): if true, queries the minimum date value on the
        table and return a date range from that value to the datetime.now() time
        delay(Optional, int): hours delayed from now time for materialization range
        end_ts(Optional, datetime): date range's final date
    Returns:
        dict: containing date_range_start and date_range_end
    """
    timestr = "%Y-%m-%dT%H:%M:%S"
    # get start from redis
    last_run = get_last_run_timestamp(dataset_id=dataset_id, table_id=table_id, mode=mode)
    # if there's no timestamp set on redis, get max timestamp on source table
    if last_run is None:
        log("Failed to fetch key from Redis...\n Querying tables for last suceeded run")
        if Table(dataset_id=dataset_id, table_id=table_id).table_exists("prod"):
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=dataset_id,
                table_id=table_id,
                field_name=table_run_datetime_column_name,
                kind="max",
            )
            log(
                f"""
            Queried last run from {dataset_id}.{table_id}
            Got:
            {last_run} as type {type(last_run)}
            """
            )
        else:
            last_run = get_table_min_max_value(
                query_project_id=bq_project(),
                dataset_id=raw_dataset_id,
                table_id=raw_table_id,
                field_name=table_run_datetime_column_name,
                kind="max",
            )
        log(
            f"""
            Queried last run from {raw_dataset_id}.{raw_table_id}
            Got:
            {last_run} as type {type(last_run)}
            """
        )
    else:
        last_run = datetime.strptime(last_run, timestr)

    if (not isinstance(last_run, datetime)) and (isinstance(last_run, date)):
        last_run = datetime(last_run.year, last_run.month, last_run.day)

    # set start to last run hour (H)
    start_ts = last_run.replace(second=0, microsecond=0)
    if truncate_minutes:
        start_ts = start_ts.replace(minute=0)

    start_ts = start_ts.strftime(timestr)

    # set end to now - delay

    if not end_ts:
        end_ts = pendulum.now(constants.TIMEZONE.value).replace(
            tzinfo=None, second=0, microsecond=0
        )

    end_ts = (end_ts - timedelta(hours=delay_hours)).replace(second=0, microsecond=0)
    if truncate_minutes:
        end_ts = end_ts.replace(minute=0)

    end_ts = end_ts.strftime(timestr)

    date_range = {"date_range_start": start_ts, "date_range_end": end_ts}
    log(f"Got date_range as: {date_range}")
    return date_range


@task
def set_last_run_timestamp(
    dataset_id: str, table_id: str, timestamp: str, mode: str = "prod", wait=None
):  # pylint: disable=unused-argument
    """
    Set the `last_run_timestamp` key for the dataset_id/table_id pair
    to datetime.now() time. Used after running a materialization to set the
    stage for the next to come

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): model filename on the queries repo.
        timestamp: Last run timestamp end.
        wait (Any, optional): Used for defining dependencies inside the flow,
        in general, pass the output of the task which should be run imediately
        before this. Defaults to None.

    Returns:
        _type_: _description_
    """
    log(f"Saving timestamp {timestamp} on Redis for {dataset_id}.{table_id}")
    redis_client = get_redis_client()
    key = dataset_id + "." + table_id
    if mode != "prod":
        key = f"{mode}.{key}"
        log(f"Will set last run timestamp on key: {key}")
    content = redis_client.get(key)
    if not content:
        content = {}
    content["last_run_timestamp"] = timestamp
    redis_client.set(key, content)
    return True


@task
def delay_now_time(timestamp: str, delay_minutes=6):
    """Return timestamp string delayed by <delay_minutes>

    Args:
        timestamp (str): Isoformat timestamp string
        delay_minutes (int, optional): Minutes to delay timestamp by Defaults to 6.

    Returns:
        str : timestamp string formatted as "%Y-%m-%dT%H-%M-%S"
    """
    ts_obj = datetime.fromisoformat(timestamp)
    ts_obj = ts_obj - timedelta(minutes=delay_minutes)
    return ts_obj.strftime("%Y-%m-%dT%H-%M-%S")


@task
def fetch_dataset_sha(dataset_id: str):
    """Fetches the SHA of a branch from Github"""
    url = "https://api.github.com/repos/prefeitura-rio/pipelines_rj_smtr"
    url += f"/commits?pipelines_rj_smtr/queries/models/{dataset_id}"
    response = requests.get(url)

    if response.status_code != 200:
        return None

    dataset_version = response.json()[0]["sha"]
    return {"version": dataset_version}


@task
def get_run_dates(
    date_range_start: str, date_range_end: str, day_datetime: datetime = None
) -> List:
    """
    Generates a list of dates between date_range_start and date_range_end.

    Args:
        date_range_start (str): the start date to create the date range
        date_range_end (str): the end date to create the date range
        day_datetime (datetime, Optional): a timestamp to use as run_date
                                            if the range start or end is False

    Returns:
        list: the list of run_dates
    """
    if (date_range_start is False) or (date_range_end is False):
        if day_datetime:
            run_date = day_datetime.strftime("%Y-%m-%d")
        else:
            run_date = get_now_date.run()
        dates = [{"run_date": run_date}]
    else:
        dates = [
            {"run_date": d.strftime("%Y-%m-%d")}
            for d in pd.date_range(start=date_range_start, end=date_range_end)
        ]
    log(f"Will run the following dates: {dates}")
    return dates


@task
def get_join_dict(dict_list: list, new_dict: dict) -> List:
    """
    Updates a list of dictionaries with a new dictionary.
    """
    for dict_temp in dict_list:
        dict_temp.update(new_dict)

    log(f"get_join_dict: {dict_list}")
    return dict_list


@task(checkpoint=False)
def get_previous_date(days):
    """
    Returns the date of {days} days ago in YYYY-MM-DD.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo")).subtract(days=days)

    return now.to_date_string()


@task(checkpoint=False)
def get_posterior_date(days: int) -> str:
    """
    Returns the date of {days} days from now in YYYY-MM-DD.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo")).add(days=days)

    return now.to_date_string()


###############
#
# Pretreat data
#
###############


@task(nout=2)
def transform_raw_to_nested_structure(
    raw_filepath: str,
    filepath: str,
    error: str,
    timestamp: datetime,
    primary_key: list = None,
    flag_private_data: bool = False,
    reader_args: dict = None,
) -> tuple[str, str]:
    """
    Task to transform raw data to nested structure

    Args:
        raw_filepath (str): Path to the saved raw .json file
        filepath (str): Path to the saved treated .csv file
        error (str): Error catched from upstream tasks
        timestamp (datetime): timestamp for flow run
        primary_key (list, optional): Primary key to be used on nested structure
        flag_private_data (bool, optional): Flag to indicate if the task should log the data
        reader_args (dict): arguments to pass to pandas.read_csv or read_json

    Returns:
        str: Error traceback
        str: Path to the saved treated .csv file
    """
    if error is None:
        try:
            # leitura do dado raw
            error, data = read_raw_data(filepath=raw_filepath, reader_args=reader_args)

            if primary_key is None:
                primary_key = []

            if not flag_private_data:
                log(
                    f"""
                    Received inputs:
                    - timestamp:\n{timestamp}
                    - data:\n{data.head()}"""
                )

            if error is None:
                # Check empty dataframe
                if data.empty:
                    log("Empty dataframe, skipping transformation...")

                else:
                    log(f"Raw data:\n{data_info_str(data)}", level="info")

                    if "customFieldValues" not in data:
                        log("Striping string columns...", level="info")
                        object_cols = data.select_dtypes(include=["object"]).columns
                        data[object_cols] = data[object_cols].apply(lambda x: x.str.strip())

                    if (
                        constants.GTFS_DATASET_ID.value in raw_filepath
                        and "ordem_servico" in raw_filepath
                        and "tipo_os" not in data.columns
                    ):
                        data["tipo_os"] = "Regular"

                    log(f"Finished cleaning! Data:\n{data_info_str(data)}", level="info")

                    log("Creating nested structure...", level="info")

                    content_columns = [c for c in data.columns if c not in primary_key]
                    data["content"] = data.apply(
                        lambda row: row[[c for c in content_columns]].to_json(
                            force_ascii=(
                                constants.CONTROLE_FINANCEIRO_DATASET_ID.value not in raw_filepath
                            ),
                        ),
                        axis=1,
                    )

                    log("Adding captured timestamp column...", level="info")
                    data["timestamp_captura"] = timestamp
                    data = data[primary_key + ["content", "timestamp_captura"]]

                    log(
                        f"Finished nested structure! Data:\n{data_info_str(data)}",
                        level="info",
                    )

            # save treated local
            filepath = save_treated_local_func(data=data, error=error, filepath=filepath)

        except Exception:  # pylint: disable=W0703
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, filepath


@task(nout=2)
def transform_raw_to_nested_structure_chunked(
    raw_filepath: str,
    filepath: str,
    error: str,
    timestamp: datetime,
    chunksize: int,
    primary_key: list = None,
    reader_args: dict = None,
) -> tuple[str, str]:
    """
    Task to transform raw data to nested structure

    Args:
        raw_filepath (str): Path to the saved raw .json file
        filepath (str): Path to the saved treated .csv file
        error (str): Error catched from upstream tasks
        timestamp (datetime): timestamp for flow run
        chunksize (int): Number of lines to read from the file per chunk
        primary_key (list, optional): Primary key to be used on nested structure
        reader_args (dict): arguments to pass to pandas.read_csv or read_json

    Returns:
        str: Error traceback
        str: Path to the saved treated .csv file
    """
    if error is None:
        try:
            # leitura do dado raw
            error, data_chunks = read_raw_data(
                filepath=raw_filepath, reader_args={"chunksize": chunksize}
            )

            if primary_key is None:
                primary_key = []

            if error is None:

                log("Creating nested structure...", level="info")

                index = 0
                for chunk in data_chunks:

                    if "customFieldValues" not in chunk:
                        object_cols = chunk.select_dtypes(include=["object"]).columns
                        chunk[object_cols] = chunk[object_cols].apply(lambda x: x.str.strip())

                    if (
                        constants.GTFS_DATASET_ID.value in raw_filepath
                        and "ordem_servico" in raw_filepath
                        and "tipo_os" not in chunk.columns
                    ):
                        chunk["tipo_os"] = "Regular"

                    transformed_chunk = transform_to_nested_structure(chunk, primary_key)
                    transformed_chunk["timestamp_captura"] = timestamp
                    if index == 0:
                        filepath = save_treated_local_func(
                            data=transformed_chunk,
                            error=error,
                            filepath=filepath,
                            args={"header": True, "mode": "w"},
                        )
                    else:
                        filepath = save_treated_local_func(
                            data=transformed_chunk,
                            error=error,
                            filepath=filepath,
                            log_param=False,
                            args={"header": False, "mode": "a"},
                        )
                    index += 1

                log("Finished nested structure!", level="info")

        except Exception:  # pylint: disable=W0703
            error = traceback.format_exc()
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error, filepath


# SUBSIDIO CHECKS


def perform_check(desc: str, check_params: dict, request_params: dict) -> dict:
    """
    Perform a check on a query

    Args:
        desc (str): The check description
        check_params (dict): The check parameters
            * query (str): SQL query to be executed
            * order_columns (list): order columns for query log results,
            in case of failure (optional)
        request_params (dict): The request parameters

    Returns:
        dict: The check status
    """
    try:
        q = check_params["query"].format(**request_params)
        order_columns = check_params.get("order_columns", None)
    except KeyError as e:
        raise ValueError(f"Missing key in check_params: {e}") from e

    log(q)
    df = bd.read_sql(q)

    check_status = df.empty

    check_status_dict = {"desc": desc, "status": check_status}

    log(f"Check status:\n{check_status_dict}")

    if not check_status:
        log(f"Data info:\n{data_info_str(df)}")
        log(f"Sorted data:\n{df.sort_values(by=order_columns) if order_columns else df}")

    return check_status_dict


def perform_checks_for_table(
    table_id: str, request_params: dict, test_check_list: dict, check_params: dict
) -> dict:
    """
    Perform checks for a table

    Args:
        table_id (str): The table id
        request_params (dict): The request parameters
        test_check_list (dict): The test check list
        check_params (dict): The check parameters

    Returns:
        dict: The checks
    """
    request_params["table_id"] = table_id
    checks = list()

    for description, test_check in test_check_list.items():
        request_params["expression"] = test_check.get("expression", "")
        checks.append(
            perform_check(
                description,
                check_params.get(test_check.get("test", "expression_is_true")),
                request_params | test_check.get("params", {}),
            )
        )

    return checks


###############
#
# Utilitary tasks
#
###############


@task(checkpoint=False)
def coalesce_task(value_list: Iterable):
    """
    Task to get the first non None value of a list

    Args:
        value_list (Iterable): a iterable object with the values
    Returns:
        any: value_list's first non None item
    """

    try:
        return next(value for value in value_list if value is not None)
    except StopIteration:
        return None


@task(checkpoint=False, nout=2)
def unpack_mapped_results_nout2(
    mapped_results: Iterable,
) -> tuple[list[Any], list[Any]]:
    """
    Task to unpack the results from an nout=2 tasks in 2 lists when it is mapped

    Args:
        mapped_results (Iterable): The mapped task return

    Returns:
        tuple[list[Any], list[Any]]: The task original return splited in 2 lists:
            - 1st list being all the first return
            - 2nd list being all the second return

    """
    return [r[0] for r in mapped_results], [r[1] for r in mapped_results]


@task
def check_mapped_query_logs_output(query_logs_output: list[tuple]) -> bool:
    """
    Task to check if there is recaptures pending

    Args:
        query_logs_output (list[tuple]): the return from a mapped query_logs execution

    Returns:
        bool: True if there is recaptures to do, otherwise False
    """

    if len(query_logs_output) == 0:
        return False

    recapture_list = [i[0] for i in query_logs_output]
    return any(recapture_list)


@task
def get_scheduled_start_times(
    timestamp: datetime, parameters: list, intervals: Union[None, dict] = None
):
    """
    Task to get start times to schedule flows

    Args:
        timestamp (datetime): initial flow run timestamp
        parameters (list): parameters for the flow
        intervals (Union[None, dict], optional): intervals between each flow run. Defaults to None.
            Optionally, you can pass specific intervals for some table_ids.
            Suggests to pass intervals based on previous table observed execution times.
            Defaults to dict(default=timedelta(minutes=2)).

    Returns:
        list[datetime]: list of scheduled start times
    """

    if intervals is None:
        intervals = dict()

    if "default" not in intervals.keys():
        intervals["default"] = timedelta(minutes=2)

    timestamps = [None]
    last_schedule = timestamp

    for param in parameters[1:]:
        last_schedule += intervals.get(param.get("table_id", "default"), intervals["default"])
        timestamps.append(last_schedule)

    return timestamps


@task
def rename_current_flow_run_now_time(prefix: str, now_time=None, wait=None) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{now_time}")


@prefect.task(checkpoint=False)
def get_now_time():
    """
    Returns the HH:MM.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))

    return f"{now.hour}:{f'0{now.minute}' if len(str(now.minute))==1 else now.minute}"


@prefect.task(checkpoint=False)
def get_now_date():
    """
    Returns the current date in YYYY-MM-DD.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))

    return now.to_date_string()


@task
def get_current_flow_mode() -> str:
    """
    Get the mode (prod/dev/staging) of the current flow.
    """
    project_name = prefect.context.get("project_name")

    if project_name != "production":
        return "dev"
    return "prod"


@task
def get_flow_project():
    return prefect.context.get("project_name")


@task
def check_date_in_range(start_date: str, end_date: str, comparison_date: str) -> bool:
    """
    Check if comparison_date is between start_date and end_date.
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    comparison_date = datetime.strptime(comparison_date, "%Y-%m-%d").date()

    return start_date < comparison_date <= end_date


@task
def split_date_range(start_date: str, end_date: str, comparison_date: str) -> dict:
    """
    Split the date range into two ranges on comparison_date.
    Returns the first and second ranges.
    """
    comparison_date = datetime.strptime(comparison_date, "%Y-%m-%d").date()

    return {
        "first_range": {
            "start_date": start_date,
            "end_date": (comparison_date - timedelta(days=1)).strftime("%Y-%m-%d"),
        },
        "second_range": {
            "start_date": comparison_date.strftime("%Y-%m-%d"),
            "end_date": end_date,
        },
    }
