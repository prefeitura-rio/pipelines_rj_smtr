# -*- coding: utf-8 -*-
"""
Tasks for veiculos
"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.utils.backup.utils import connect_ftp, data_info_str, filter_data
from pipelines.utils.utils import log  # ,get_vault_secret

# EMD Imports #


# SMTR Imports #


# Tasks #


@task
def get_ftp_filepaths(search_dir: str, wait=None):
    # min_timestamp = datetime(2022, 1, 1).timestamp()  # set min timestamp for search
    # Connect to FTP & search files
    # try:
    ftp_client = connect_ftp(constants.RDO_FTPS_SECRET_PATH.value)
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


@task
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
