# -*- coding: utf-8 -*-
"""Module to deal with the filesystem"""
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import custom_serialization


def get_data_folder_path() -> str:
    """Gets the parent path to save data

    Returns:
        str: Data folder path
    """
    return os.path.join(os.getcwd(), os.getenv("DATA_FOLDER", "data"))


def create_partition(
    timestamp: datetime,
    partition_date_only: bool,
) -> str:
    """
    Create a date (and hour) Hive partition structure from timestamp.

    Args:
        timestamp (datetime): timestamp to be used as reference
        partition_date_only (bool): whether to add hour partition or not

    Returns:
        str: partition string
    """
    timestamp = timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE.value))
    partition = f"data={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition = os.path.join(partition, f"hora={timestamp.strftime('%H')}")
    return partition


def create_capture_filepath(
    dataset_id: str,
    table_id: str,
    timestamp: datetime,
    raw_filetype: str,
    partition: str = None,
) -> dict[str, str]:
    """
    Create the full path sctructure which to save data locally before
    upload.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        timestamp (datetime): capture timestamp
        partition (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01"
    Returns:
        dict: raw and source filepaths
    """
    timestamp = timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE.value))
    data_folder = get_data_folder_path()
    template_filepath = f"{os.getcwd()}/{data_folder}/{{mode}}/{dataset_id}/{table_id}"
    template_filepath = os.path.join(
        os.getcwd(),
        data_folder,
        "{mode}",
        dataset_id,
        table_id,
    )
    if partition is not None:
        template_filepath = os.path.join(template_filepath, partition)

    template_filepath = os.path.join(
        template_filepath,
        f"{timestamp.strftime(constants.FILENAME_PATTERN.value)}.{{filetype}}",
    )

    filepath = {
        "raw": template_filepath.format(mode="raw", filetype=raw_filetype),
        "source": template_filepath.format(mode="source", filetype="csv"),
    }

    return filepath


def get_filetype(filepath: str):
    return os.path.splitext(filepath)[1].removeprefix(".")


def save_local_file(filepath: str, data):
    """
    Saves the data at the local file path

    Args:
        filepath (str): File path
        data: Data to write on the file
    """

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

    if isinstance(data, pd.DataFrame):
        data.to_csv(filepath, index=False)
        return

    filetype = get_filetype(filepath)

    with open(filepath, "w", encoding="utf-8") as file:
        if filetype == "json":
            if isinstance(data, str):
                data = json.loads(data)

            json.dump(data, file, default=custom_serialization)

        elif filetype in ("txt", "csv"):
            file.write(data)


def read_raw_data(filepath: str, reader_args: dict = None) -> pd.DataFrame:
    """
    Read raw data from file

    Args:
        filepath (str): filepath to read
        reader_args (dict): arguments to pass to pandas.read_csv or read_json

    Returns:
        pd.DataFrame: data
    """
    if reader_args is None:
        reader_args = {}

    file_type = get_filetype(filepath=filepath)

    if file_type == "json":
        data = pd.read_json(filepath, **reader_args)

    elif file_type in ("txt", "csv"):
        data = pd.read_csv(filepath, **reader_args)
    else:
        raise ValueError("Unsupported raw file extension. Supported only: json, csv and txt")

    return data
