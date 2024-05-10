# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants
from pipelines.utils.backup.utils import connect_ftp, data_info_str, filter_data
from pipelines.utils.utils import log  # ,get_vault_secret

# EMD Imports #


# SMTR Imports #


# Tasks #
def build_table_id(mode: str, report_type: str):
    """Build table_id based on which table is the target
    of current flow run

    Args:
        mode (str): SPPO or STPL
        report_type (str): RHO or RDO

    Returns:
        str: table_id
    """
    if mode == "SPPO":
        if report_type == "RDO":
            table_id = constants.SPPO_RDO_TABLE_ID.value
        else:
            table_id = constants.SPPO_RHO_TABLE_ID.value
    if mode == "STPL":
        # slice the string to get rid of V at end of
        # STPL reports filenames
        if report_type[:3] == "RDO":
            table_id = constants.STPL_RDO_TABLE_ID.value
        else:
            table_id = constants.STPL_RHO_TABLE_ID.value
    return table_id


@task
def download_and_save_local_from_ftp(file_info: dict, dataset_id: str = None, table_id: str = None):
    """
    Downloads file from FTP and saves to data/raw/<dataset_id>/<table_id>.
    """
    # table_id: str, kind: str, rho: bool = False, rdo: bool = True
    if file_info["error"] is not None:
        return file_info
    if not dataset_id:
        dataset_id = constants.RDO_DATASET_ID.value
    if not table_id:
        table_id = build_table_id(  # mudar pra task
            mode=file_info["transport_mode"], report_type=file_info["report_type"]
        )

    base_path = f'{os.getcwd()}/{os.getenv("DATA_FOLDER", "data")}/{{bucket_mode}}/{dataset_id}'

    # Set general local path to save file (bucket_modes: raw or staging)
    file_info["local_path"] = (
        f"""{base_path}/{table_id}/{file_info["partitions"]}/{file_info['filename']}.{{file_ext}}"""
    )

    # Get raw data
    file_info["raw_path"] = file_info["local_path"].format(bucket_mode="raw", file_ext="txt")
    file_info["treated_path"] = file_info["local_path"].format(
        bucket_mode="staging", file_ext="csv"
    )
    Path(file_info["raw_path"]).parent.mkdir(parents=True, exist_ok=True)
    try:
        # Get data from FTP - TODO: create get_raw() error alike
        ftp_client = connect_ftp(constants.RDO_FTPS_SECRET_PATH.value)
        if not Path(file_info["raw_path"]).is_file():
            with open(file_info["raw_path"], "wb") as raw_file:
                ftp_client.retrbinary(
                    "RETR " + file_info["ftp_path"],
                    raw_file.write,
                )
        ftp_client.quit()
        # Get timestamp of download time
        file_info["timestamp_captura"] = pendulum.now(constants.TIMEZONE.value).isoformat()

        log(f"Timestamp captura is {file_info['timestamp_captura']}")
        log(f"Update file info: {file_info}")
    except Exception as error:  # pylint: disable=W0703
        file_info["error"] = error
    return file_info


@task
def get_ftp_filepaths(search_dir: str, wait=None, timestamp=None):
    # min_timestamp = datetime(2022, 1, 1).timestamp()  # set min timestamp for search
    # Connect to FTP & search files
    # try:
    ftp_client = connect_ftp(constants.RDO_FTPS_SECRET_PATH.value)
    if timestamp is not None:
        target_date = timestamp.date()
        filenames = [
            file
            for file, info in ftp_client.mlsd(search_dir)
            if datetime.strptime(file.split(".")[0].split("_")[1], "%Y%m%d").date() == target_date
        ]
    else:
        filenames = [file for file, info in ftp_client.mlsd(search_dir)]
    files = []
    for file in filenames:
        filename = file.split(".")[0]
        file_date = datetime.strptime(filename.split("_")[1], "%Y%m%d").date()
        partitions = f"data={file_date.isoformat()}"
        ftp_path = f"{search_dir}/{file}"
        file_info = {
            "filename": filename,
            "partitions": partitions,
            "ftp_path": ftp_path,
            "error": None,
        }
        files.append(file_info)

    return files


@task(nout=4)
def pre_treatment_sppo_licenciamento(files: list):
    """Basic data treatment for vehicle data. Apply filtering to raw data.

    Args:
        status_dict (dict): dict containing the status of the request made.
        Must contain keys: data, timestamp and error
        timestamp (datetime): timestamp of the data capture

    Returns:
        dict: dict containing the data treated and the current error status.
    """
    # Check previous error
    treated_paths, raw_paths, partitions, status = [], [], [], []
    for file_info in files:
        if file_info["error"] is not None:
            return {"data": pd.DataFrame(), "error": file_info["error"]}

        try:
            error = None
            data = pd.json_normalize(file_info["data"])

            log(
                f"""
            Received inputs:
            - timestamp:\n{file_info["timestamp_captura"]}
            - data:\n{data.head()}"""
            )

            log(f"Raw data:\n{data_info_str(data)}", level="info")

            log("Renaming columns...", level="info")
            data = data.rename(columns=constants.SPPO_LICENCIAMENTO_MAPPING_KEYS.value)

            log("Adding captured timestamp column...", level="info")
            data["timestamp_captura"] = file_info["timestamp_captura"]

            log("Striping string columns...", level="info")
            for col in data.columns[data.dtypes == "object"].to_list():
                data[col] = data[col].str.strip()

            log("Converting boolean values...", level="info")
            for col in data.columns[data.columns.str.contains("indicador")].to_list():
                data[col] = data[col].map({"Sim": True, "Nao": False})

            # Check data
            # check_columns = [["id_veiculo", "placa"], ["tipo_veiculo", "id_planta"]]

            # check_relation(data, check_columns)

            log("Filtering null primary keys...", level="info")
            primary_key = ["id_veiculo"]
            data.dropna(subset=primary_key, inplace=True)

            log("Update indicador_ar_condicionado based on tipo_veiculo...", level="info")
            data["indicador_ar_condicionado"] = data["tipo_veiculo"].map(
                lambda x: None if not isinstance(x, str) else bool("C/AR" in x.replace(" ", ""))
            )

            log("Update status...", level="info")
            data["status"] = "Licenciado"

            log(f"Finished cleaning! Pre-treated data:\n{data_info_str(data)}", level="info")

            log("Creating nested structure...", level="info")
            pk_cols = primary_key + ["timestamp_captura"]
            data = (
                data.groupby(pk_cols)
                .apply(lambda x: x[data.columns.difference(pk_cols)].to_json(orient="records"))
                .str.strip("[]")
                .reset_index(name="content")[primary_key + ["content", "timestamp_captura"]]
            )

            log(
                f"Finished nested structure! Pre-treated data:\n{data_info_str(data)}",
                level="info",
            )
            # Save Local
            Path(file_info["treated_path"]).parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(file_info["treated_path"])

            # Update successful outputs
            raw_paths.append(file_info["raw_path"])
            treated_paths.append(file_info["treated_path"])
            partitions.append(file_info["partitions"])
            status.append({"error": None})

        except Exception as e:  # pylint: disable=W0703
            log(f"Pre Treatment failed with error: {e}")
            treated_paths.append(None)
            raw_paths.append(None)
            partitions.append(None)
            status.append({"error": e})

    return treated_paths, raw_paths, partitions, status


@task(nout=4)
def pre_treatment_sppo_infracao(files: list):
    """Basic data treatment for violation data. Apply filtering to raw data.

    Args:
        status_dict (dict): dict containing the status of the request made.
        Must contain keys: data, timestamp and error
        timestamp (datetime): timestamp of the data capture

    Returns:
        dict: dict containing the data treated and the current error status.
    """

    # Check previous error
    treated_paths, raw_paths, partitions, status = [], [], [], []
    for file_info in files:
        if file_info["error"] is not None:
            return {"data": pd.DataFrame(), "error": file_info["error"]}

        try:
            error = None
            data = pd.read_csv(file_info["raw_path"], sep=";", header=None, index_col=False)

            log(
                f"""
            Received inputs:
            - data:\n{data.head()}"""
            )

            log(f"Raw data:\n{data_info_str(data)}", level="info")

            log("Adding columns...", level="info")
            # data = data.rename(columns=constants.SPPO_INFRACAO_MAPPING_KEYS.value)
            # log(data.head(50))
            data.columns = constants.SPPO_INFRACAO_COLUMNS.value

            log("Adding captured timestamp column...", level="info")
            data["timestamp_captura"] = file_info["timestamp_captura"]

            log("Striping string columns and replacing empty strings...", level="info")
            for col in data.columns[data.dtypes == "object"].to_list():
                data[col] = data[col].str.strip().replace("", np.nan)

            log("Updating valor type to float...", level="info")
            data["valor"] = data["valor"].str.replace(",", ".").astype(float)

            filters = ["modo != 'ONIBUS'"]
            log(f"Filtering '{filters}'...", level="info")
            data = filter_data(data, filters)

            log("Filtering null primary keys...", level="info")
            primary_key = ["placa", "id_auto_infracao"]
            data.dropna(subset=primary_key, inplace=True)

            # Check primary keys
            # pk_columns = ["placa", "id_auto_infracao"]
            # check_new_data = f"data_infracao == '{timestamp.strftime('%Y-%m-%d')}'"
            # check_not_null(data, pk_columns, subset_query=check_new_data)

            log(f"Finished cleaning! Pre-treated data:\n{data_info_str(data)}", level="info")

            log("Creating nested structure...", level="info")
            pk_cols = primary_key + ["timestamp_captura"]
            data = (
                data.groupby(pk_cols)
                .apply(lambda x: x[data.columns.difference(pk_cols)].to_json(orient="records"))
                .str.strip("[]")
                .reset_index(name="content")[primary_key + ["content", "timestamp_captura"]]
            )

            log(
                f"Finished nested structure! Pre-treated data:\n{data_info_str(data)}",
                level="info",
            )
            # Save Local
            Path(file_info["treated_path"]).parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(file_info["treated_path"])

            # Update successful outputs
            raw_paths.append(file_info["raw_path"])
            treated_paths.append(file_info["treated_path"])
            partitions.append(file_info["partitions"])
            status.append({"error": None})

        except Exception as e:  # pylint: disable=W0703
            log(f"Pre Treatment failed with error: {e}")
            treated_paths.append(None)
            raw_paths.append(None)
            partitions.append(None)
            status.append({"error": e})

    return treated_paths, raw_paths, partitions, status
