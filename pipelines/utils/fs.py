# -*- coding: utf-8 -*-
"""Module to deal with the filesystem"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
import pytz
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.utils import custom_serialization


def get_data_folder_path() -> str:
    """
    Retorna a pasta raíz para salvar os dados

    Returns:
        str: Caminho para a pasta data
    """
    return os.path.join(os.getcwd(), os.getenv("DATA_FOLDER", "data"))


def create_partition(
    timestamp: datetime,
    partition_date_only: bool,
) -> str:
    """
    Cria a partição Hive de acordo com a timestamp

    Args:
        timestamp (datetime): timestamp de referência
        partition_date_only (bool): True se o particionamento deve ser feito apenas por data
            False se o particionamento deve ser feito por data e hora
    Returns:
        str: string com o particionamento
    """
    log("Creating file partition...")
    log(f"Timestamp received: {timestamp}")
    timestamp = timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE.value))
    log(f"Timestamp converted to {constants.TIMEZONE.value}: {timestamp}")
    partition = f"data={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition = os.path.join(partition, f"hora={timestamp.strftime('%H')}")

    log(f"Partition created successfully: {partition}")
    return partition


def create_capture_filepath(
    dataset_id: str,
    table_id: str,
    timestamp: datetime,
    raw_filetype: str,
    partition: str = None,
) -> dict[str, str]:
    """
    Cria os caminhos para salvar os dados localmente

    Args:
        dataset_id (str): dataset_id no BigQuery
        table_id (str): table_id no BigQuery
        timestamp (datetime): timestamp da captura
        partition (str, optional): Partição dos dados em formato Hive, ie "data=2020-01-01/hora=06"
    Returns:
        dict: caminhos para os dados raw e source
    """
    log("Creating filepaths...")
    log(f"Timestamp received: {timestamp}")
    data_folder = get_data_folder_path()
    log(f"Data folder: {data_folder}")
    template_filepath = os.path.join(
        data_folder,
        "{mode}",
        dataset_id,
        table_id,
    )

    if partition is not None:
        log("Received partition, appending it to filepath template")
        template_filepath = os.path.join(template_filepath, partition)

    template_filepath = os.path.join(
        template_filepath,
        f"{timestamp.strftime(constants.FILENAME_PATTERN.value)}.{{filetype}}",
    )

    log(f"Filepath template: {template_filepath}")

    filepath = {
        "raw": template_filepath.format(mode="raw", filetype=raw_filetype),
        "source": template_filepath.format(mode="source", filetype="csv"),
    }

    log(f"Created filepaths successfully: {filepath}")

    return filepath


def get_filetype(filepath: str):
    """Retorna a extensão de um arquivo

    Args:
        filepath (str): caminho para o arquivo
    """
    return os.path.splitext(filepath)[1].removeprefix(".")


def save_local_file(filepath: str, filetype: str, data: Union[str, dict, list[dict], pd.DataFrame]):
    """
    Salva um arquivo localmente

    Args:
        filepath (str): Caminho para salvar o arquivo
        data Union[str, dict, list[dict], pd.DataFrame]: Dados que serão salvos no arquivo
    """
    log(f"Saving data on local file: {filepath}")

    log("Creating parent folder...")
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    log("Parent folder created!")

    if isinstance(data, pd.DataFrame):
        log("Received a DataFrame, saving file as CSV")
        data.to_csv(filepath, index=False)
        log("File saved!")
        return

    log(f"Saving {filetype.upper()}")
    with open(filepath, "w", encoding="utf-8") as file:
        if filetype == "json":
            if isinstance(data, str):
                log("Converting string to python object")
                data = json.loads(data)

            json.dump(data, file, default=custom_serialization)

        elif filetype in ("txt", "csv"):
            file.write(data)

        else:
            raise NotImplementedError(
                "Unsupported raw file extension. Supported only: json, csv and txt"
            )

    log("File saved!")


def read_raw_data(filepath: str, reader_args: dict = None) -> pd.DataFrame:
    """
    Lê os dados de um arquivo Raw

    Args:
        filepath (str): Caminho do arquivo
        reader_args (dict, optional): Argumentos para passar na função
            de leitura (pd.read_csv ou pd.read_json)

    Returns:
        pd.DataFrame: DataFrame com os dados lidos
    """

    log(f"Reading raw data in {filepath}")
    if reader_args is None:
        reader_args = {}

    filetype = get_filetype(filepath=filepath)

    log(f"Reading {filetype.upper()}")
    if filetype == "json":
        data = pd.read_json(filepath, **reader_args)

    elif filetype in ("txt", "csv"):
        data = pd.read_csv(filepath, **reader_args)
    else:
        raise NotImplementedError(
            "Unsupported raw file extension. Supported only: json, csv and txt"
        )

    return data
