# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
import os
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.fs import (
    create_capture_filepath,
    create_partition,
    read_raw_data,
    save_local_file,
)
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.prefect import rename_current_flow_run
from pipelines.utils.pretreatment import transform_to_nested_structure
from pipelines.utils.utils import (
    convert_timezone,
    create_timestamp_captura,
    data_info_str,
)

############################
# Flow Configuration Tasks #
############################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def set_env(env: str, table_id: str, source_map: dict[str, SourceTable]) -> SourceTable:
    """
    Cria um objeto de tabela source para interagir com o BigQuery

    Args:
        env (str): dev ou prod
        table_id (str): Nome da tabela que será capturada
        source_map (dict[str, SourceTable]): Dicionário no formato
            {"table_id": SourceTable(), ...}

    Returns:
        SourceTable: Objeto para manipular a tabela source no BigQuery
    """
    if table_id not in source_map.keys():
        raise ValueError(
            f"source {table_id} não disponível no flow.\n sources: {source_map.keys()}"
        )
    source = deepcopy(source_map[table_id])

    return source.set_env(env=env)


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def rename_capture_flow(
    table_id: str,
    timestamp: datetime,
    recapture: bool,
) -> bool:
    """
    Renomeia a run atual do Flow de captura com o formato:
    <table_id>: <timestamp> - recaptura: <recapture>

    Args:
        env (str): dev ou prod
        table_id (str): Nome da tabela que será capturada
        recaptura (bool): Se a execução é uma recaptura ou não
    Returns:
        bool: Se o flow foi renomeado
    """
    name = f"{table_id}: {timestamp.isoformat()} - recaptura: {recapture}"
    return rename_current_flow_run(name=name)


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_capture_timestamps(
    source: SourceTable,
    timestamp: datetime,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
) -> list[datetime]:
    """
    Retorna os timestamps que serão capturados pelo flow

    Args:
        source (SourceTable): Objeto representando a fonte de dados que será capturada
        timestamp (datetime): Datetime de referência da execução do flow
        recapture (bool): Se a execução é uma recaptura ou não
        recapture_days (int): A quantidade de dias que serão considerados para achar datas
            a serem recapturadas
        recapture_timestamps (list[str]): Lista manual de timestamps a serem recapturadas

    Returns:
        list[datetime]: Lista de datetimes para executar a captura
    """
    if recapture:
        if recapture_timestamps:
            return [convert_timezone(datetime.fromisoformat(t)) for t in recapture_timestamps]

        return source.get_uncaptured_timestamps(
            timestamp=timestamp,
            retroactive_days=recapture_days,
        )

    return [timestamp]


#####################
# Raw Capture Tasks #
#####################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def create_partition_task(source: SourceTable, timestamp: datetime) -> str:
    """
    Retorna a partição no formato Hive

    Args:
        source (SourceTable): Objeto representando a fonte de dados que será capturada
        timestamp (datetime): Datetime para criar a partição

    Returns:
        str: partição no formato Hive
    """
    return create_partition(
        timestamp=timestamp,
        partition_date_only=source.partition_date_only,
    )


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def create_filepaths(source: SourceTable, partition: str, timestamp: datetime) -> dict:
    """
    Retorna os caminhos para salvar os dados source e raw

    Args:
        source (SourceTable): Objeto representando a fonte de dados que será capturada
        partition (str): Partição no formato Hive
        timestamp (datetime): Datetime para criar o nome do arquivo

    Returns:
        dict: Dicionário no formato:
            {
                "raw": raw/caminho/para/salvar/arquivo.extensao,
                "source": source/caminho/para/salvar/arquivo.extensao
            }
    """

    return create_capture_filepath(
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        timestamp=timestamp,
        raw_filetype=source.raw_filetype,
        partition=partition,
    )


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_raw_data(data_extractor: Callable, filepaths: dict, raw_filetype: str, source: SourceTable):
    """
    Faz a extração dos dados raw e salva localmente

    Args:
        data_extractor (Callable): função a ser executada
        filepaths (dict): Dicionário no formato:
            {
                "raw": raw/caminho/para/salvar/arquivo.extensao,
                "source": source/caminho/para/salvar/arquivo.extensao
            }
        raw_filetype (str): tipo de dado raw
        source (SourceTable): Objeto representando a fonte de dados capturados
    """
    raw_filepath = filepaths["raw"]

    if source.file_chunk_size is not None:
        raw_filepaths = data_extractor(raw_filepath=raw_filepath)
    else:
        data = data_extractor()
        base_path, ext = os.path.splitext(raw_filepath)
        filepath = f"{base_path}_0{ext}"
        save_local_file(filepath=filepath, filetype=raw_filetype, data=data)
        raw_filepaths = [filepath]

    return raw_filepaths


################
# Upload Tasks #
################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def upload_raw_file_to_gcs(source: SourceTable, raw_filepaths: list[str], partition: str):
    """
    Sobe o arquivo raw para o GCS

    Args:
        source (SourceTable): Objeto representando a fonte de dados capturados
        raw_filepaths (list): Lista de caminhos para ler os dados raw
        partition (str): Partição Hive
    """
    for filepath in raw_filepaths:
        source.upload_raw_file(raw_filepath=filepath, partition=partition)


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def upload_source_data_to_gcs(source: SourceTable, partition: str, filepaths: dict):
    """
    Sobe os dados aninhados e o log do Flow para a pasta source do GCS

    Args:
        source (SourceTable): Objeto representando a fonte de dados capturados
        filepaths (dict): Dicionário no formato:
            {
                "raw": raw/caminho/para/salvar/arquivo.extensao,
                "source": source/caminho/para/salvar/arquivo.extensao
            }
        partition (str): Partição Hive
    """
    if not source.exists():
        log("Staging Table does not exist, creating table...")
        source.append(source_filepath=filepaths["source"], partition=partition)
        source.create(sample_filepath=filepaths["source"])
    else:
        log("Staging Table already exists, appending to it...")
        source.append(source_filepath=filepaths["source"], partition=partition)


######################
# Pretreatment Tasks #
######################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def transform_raw_to_nested_structure(
    raw_filepaths: list[str],
    filepaths: dict[str],
    timestamp: datetime,
    primary_keys: list[str],
    reader_args: dict,
    pretreat_funcs: Callable[[pd.DataFrame, datetime, list[str]], pd.DataFrame],
):
    """
    Task para aplicar pre-tratamentos e transformar os dados para o formato aninhado

    Args:
        raw_filepaths (list): Lista de caminhos para ler os dados raw
        source_filepath (str): Caminho para salvar os dados tratados
        timestamp (datetime): A timestamp da execução do Flow
        primary_keys (list): Lista de primary keys da tabela
        reader_args (dict): Dicionário de argumentos para serem passados no leitor de dados raw
            (pd.read_json ou pd.read_csv)
        pretreat_funcs (Callable[[pd.DataFrame, datetime, list[str]], pd.DataFrame]): funções para
            serem executadas antes de transformar em nested. Devem receber os argumentos:
                data (pd.DataFrame)
                timestamp (datetime)
                primary_keys (list[str])
            e retornar um pd.DataFrame
    """
    csv_mode = "w"
    for raw_filepath in raw_filepaths:
        data = read_raw_data(filepath=raw_filepath, reader_args=reader_args)

        if data.empty:
            log("Empty dataframe, skipping transformation...")
            data = pd.DataFrame()
        else:
            log(f"Raw data:\n{data_info_str(data)}", level="info")

            data_columns_len = len(data.columns)
            captura = create_timestamp_captura(timestamp=datetime.now())
            data["_datetime_execucao_flow"] = captura

            for step in pretreat_funcs:
                data = step(data=data, timestamp=timestamp, primary_keys=primary_keys)

            if len(primary_keys) < data_columns_len:
                data = transform_to_nested_structure(data=data, primary_keys=primary_keys)

            data["timestamp_captura"] = create_timestamp_captura(timestamp=timestamp)

        log(
            f"Finished nested structure! Data: \n{data_info_str(data)}",
            level="info",
        )

        source_filepath = filepaths["source"]
        save_local_file(filepath=source_filepath, filetype="csv", data=data, csv_mode=csv_mode)
        csv_mode = "a"
        log(f"Data saved in {source_filepath}")
