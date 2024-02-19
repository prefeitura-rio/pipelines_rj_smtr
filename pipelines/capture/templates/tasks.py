# -*- coding: utf-8 -*-
"""
Tasks for rj_smtr
"""
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
from pipelines.utils.incremental_capture_strategy import (
    IncrementalCaptureStrategy,
    IncrementalInfo,
    incremental_strategy_from_dict,
)
from pipelines.utils.prefect import flow_is_running_local, rename_current_flow_run
from pipelines.utils.pretreatment import transform_to_nested_structure
from pipelines.utils.utils import create_timestamp_captura, data_info_str

############################
# Flow Configuration Tasks #
############################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def create_table_object(
    env: str,
    dataset_id: str,
    table_id: str,
    bucket_name: Union[None, str],
    timestamp: datetime,
    partition_date_only: bool,
    raw_filetype: str,
) -> BQTable:
    """
    Cria um objeto de tabela para interagir com o BigQuery
    Creates basedosdados Table object

    Args:
        env (str): dev ou prod,
        dataset_id (str): dataset_id no BigQuery,
        table_id (str): table_id no BigQuery,
        bucket_name (Union[None, str]): Nome do bucket com os dados da tabela no GCS,
            se for None, usa o bucket padrão do ambiente
        timestamp (datetime): timestamp gerado pela execução do flow,
        partition_date_only (bool): True se o particionamento deve ser feito apenas por data
            False se o particionamento deve ser feito por data e hora,
        raw_filetype (str): Tipo do arquivo raw (json, csv...),

    Returns:
        BQTable: Objeto para manipular a tabela no BigQuery
    """

    return BQTable(
        env=env,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        timestamp=timestamp,
        partition_date_only=partition_date_only,
        raw_filetype=raw_filetype,
    )


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def rename_capture_flow(
    dataset_id: str,
    table_id: str,
    timestamp: datetime,
    incremental_info: IncrementalInfo,
) -> bool:
    """
    Renomeia a run atual do Flow de captura com o formato:
    [<timestamp> | <FULL/INCR>] <dataset_id>.<table_id>: from <valor inicial> to <valor final>

    Returns:
        bool: Se o flow foi renomeado
    """
    name = f"[{timestamp.astimezone(tz=timezone(constants.TIMEZONE.value))} | \
{incremental_info.execution_mode.upper()}] {dataset_id}.{table_id}: from \
{incremental_info.start_value} to {incremental_info.end_value}"
    return rename_current_flow_run(name=name)


#####################
# Raw Capture Tasks #
#####################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def get_raw_data(data_extractor: DataExtractor):
    """
    Faz a extração dos dados raw e salva localmente

    Args:
        data_extractor (DataExtractor): Extrator de dados a ser executado
    """
    data_extractor.extract()
    data_extractor.save_raw_local()


################
# Upload Tasks #
################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def upload_raw_file_to_gcs(table: BQTable):
    """
    Sobe o arquivo raw para o GCS

    Args:
        table (BQTable): Objeto de tabela para BigQuery
    """
    table.upload_raw_file()


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def upload_source_data_to_gcs(table: BQTable):
    """
    Sobe os dados aninhados e o log do Flow para a pasta source do GCS

    Args:
        table (BQTable): Objeto de tabela para BigQuery
    """

    if not table.exists():
        log("Staging Table does not exist, creating table...")
        table.create()
    else:
        log("Staging Table already exists, appending to it...")
        table.append()


######################
# Pretreatment Tasks #
######################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def transform_raw_to_nested_structure(
    pretreat_funcs: list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]],
    raw_filepath: str,
    source_filepath: str,
    timestamp: datetime,
    primary_keys: Union[list, str],
    print_inputs: bool,
    reader_args: dict,
):
    """
    Task para aplicar pre-tratamentos e transformar os dados para o formato aninhado

    Args:
        pretreat_funcs (list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]]):
            Lista de funções para serem executadas antes de aninhar os dados
            A função pode receber os argumentos:
                data (pd.DataFrame): O DataFrame a ser tratado
                timestamp (datetime): A timestamp da execução do Flow
                primary_keys (list): Lista de primary keys da tabela
            Deve retornar um DataFrame
        raw_filepath (str): Caminho para ler os dados raw
        source_filepath (str): Caminho para salvar os dados tratados
        timestamp (datetime): A timestamp da execução do Flow
        primary_keys (list): Lista de primary keys da tabela
        print_inputs (bool): Se a task deve exibir os dados lidos no log ou não
        reader_args (dict): Dicionário de argumentos para serem passados no leitor de dados raw
            (pd.read_json ou pd.read_csv)
    """
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

    for step in pretreat_funcs:
        log(f"Starting treatment step: {step.__name__}...")
        data = step(data=data, timestamp=timestamp, primary_keys=primary_keys)
        log(f"Step {step.__name__} finished")

    log("Creating nested structure...", level="info")

    data = transform_to_nested_structure(data=data, primary_keys=primary_keys)

    timestamp = create_timestamp_captura(timestamp=timestamp)
    data["timestamp_captura"] = timestamp
    log(f"timestamp column = {timestamp}", level="info")

    log(
        f"Finished nested structure! Data:\n{data_info_str(data)}",
        level="info",
    )

    save_local_file(filepath=source_filepath, data=data)
    log(f"Data saved in {source_filepath}")


#####################
# Incremental Tasks #
#####################


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def create_incremental_strategy(
    strategy_dict: Union[None, dict],
    table: BQTable,
    overwrite_start_value: Any,
    overwrite_end_value: Any,
) -> Union[dict, IncrementalCaptureStrategy]:
    """
    Cria a estratégia de captura incremental

    Args:
        strategy_dict (Union[None, dict]): dicionario retornado pelo
            método .to_dict() do objeto de IncrementalCaptureStrategy
        table (BQTable): Objeto de tabela para BigQuery
        overwrite_start_value: Valor para substituir o inicial manualmente
        overwrite_end_value: Valor para substituir o final manualmente

    Returns:
        Union[dict, IncrementalCaptureStrategy]: Se strategy_dict for None, retorna um Dicionário
            contendo um objeto IncrementalInfo com os valores de start e end sendo
            overwrite_start_value e overwrite_end_value respectivamente
            e execution_mode full
            Se houver valor no argumento strategy_dict, retorna um objeto IncrementalCaptureStrategy
            de acordo com as especificações descritas no dicionário
    """
    if strategy_dict:
        incremental_strategy = incremental_strategy_from_dict(strategy_dict=strategy_dict)
        incremental_strategy.initialize(
            table=table,
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


@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def save_incremental_redis(
    incremental_capture_strategy: Union[dict, IncrementalCaptureStrategy],
):
    """
    Salva o último valor incremental capturado no Redis


    Args:
        incremental_capture_strategy: Union[dict, IncrementalCaptureStrategy]: Objeto de estratégia
            de captura incremental. apenas salva no Redis se for do tipo IncrementalCaptureStrategy
    """
    is_local_run = flow_is_running_local()
    if isinstance(incremental_capture_strategy, IncrementalCaptureStrategy) and not is_local_run:
        incremental_capture_strategy.save_on_redis()
    else:
        log(
            f"""Save on Redis skipped:
            incremental_capture_strategy type: {type(incremental_capture_strategy)}
            flow is running local: {is_local_run}
            """
        )
