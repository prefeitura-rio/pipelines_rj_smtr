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
    create_table_object,
    get_raw_data,
    rename_capture_flow,
    save_incremental_redis,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.constants import constants
from pipelines.tasks import get_current_timestamp, get_run_env
from pipelines.utils.prefect import TypedParameter

# from pipelines.utils.pretreatment import strip_string_columns


def create_default_capture_flow(
    flow_name: str,
    source_name: str,
    partition_date_only: bool,
    create_extractor_task: FunctionTask,
    overwrite_flow_params: dict,
    agent_label: str,
    pretreat_funcs: list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]] = None,
):  # pylint: disable=R0914, R0913
    """
    Cria um flow de captura

    Args:
        flow_name (str): O nome do flow
        source_name (str): Nome da fonte do dado (exemplo: jae)
        partition_date_only (bool): True se o particionamento deve ser feito apenas por data
            False se o particionamento deve ser feito por data e hora
        create_extractor_task (FunctionTask):
            A task que cria o DataExtractor
            Pode receber os argumentos:
                env (str): dev ou prod
                source_name (str): O nome do source
                table_id (str): table_id no BigQuery
                save_filepath (str): O caminho para salvar o arquivo raw localmente
                data_extractor_params (dict): Dicionario com parametros personalizados
                incremental_info (IncrementalInfo): Objeto contendo informações sobre
                    a execução incremental
            Deve retornar uma classe derivada de DataExtractor
        overwrite_optional_flow_params (dict): Dicionário para substituir
            o valor padrão dos parâmetros opcionais do flow
        agent_label (str): Label do flow
        pretreat_funcs (list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]], optional):
            Lista de funções de pre-tratamento para serem executadas antes de aninhar os dados
            A função pode receber os argumentos:
                data (pd.DataFrame): O DataFrame para ser tratado
                timestamp (datetime): A timestamp do flow
                primary_key (list): A lista de primary keys
            Deve retornar um DataFrame

    Returns:
        Flow: The capture flow
    """

    if pretreat_funcs is None:
        pretreat_funcs = []

    with Flow(flow_name) as capture_flow:
        # Parâmetros Gerais #

        # table_id no BigQuery
        table_id = TypedParameter(
            name="table_id",
            default=overwrite_flow_params.get("table_id"),
            accepted_types=str,
        )
        # Tipo do arquivo raw (json, csv...)
        raw_filetype = TypedParameter(
            name="raw_filetype",
            default=overwrite_flow_params.get("raw_filetype"),
            accepted_types=str,
        )

        # Parâmetros Incremental #

        # Dicionário para gerar o objeto de estratégia incremental
        # Modo de usar:
        # Instancie o objeto da estrategia escolhida e chame o metodo to_dict()
        # ex.: DatetimeIncremental(max_incremental_window={"hours": 3}).to_dict()
        incremental_capture_strategy = TypedParameter(
            name="incremental_capture_strategy",
            default=overwrite_flow_params.get("incremental_capture_strategy"),
            accepted_types=(dict, NoneType),
        )
        # Valor inicial de captura para sobrescrever o padrão
        # valor inicial padrão = valor do salvo no Redis
        # para incrementais do tipo datetime, o valor deve ser uma string
        # de data no formato iso (timezone padrão = UTC)
        # para incrementais de id deve ser um inteiro
        incremental_start_value = TypedParameter(
            name="incremental_start_value",
            default=overwrite_flow_params.get("incremental_start_value"),
            accepted_types=(str, int, NoneType),
        )
        # Valor final de captura para sobrescrever o padrão
        # valor final padrão = valor inicial + max_incremental_window
        # para incrementais do tipo datetime, o valor deve ser uma string
        # de data no formato iso (timezone padrão = UTC)
        # para incrementais de id deve ser um inteiro
        incremental_end_value = TypedParameter(
            name="incremental_end_value",
            default=overwrite_flow_params.get("incremental_end_value"),
            accepted_types=(str, int, NoneType),
        )

        # Parâmetros para Captura #

        # Dicionário com valores personalizados para serem acessados na task
        # passada no argumento create_extractor_task
        data_extractor_params = Parameter(
            "data_extractor_params",
            default=overwrite_flow_params.get("data_extractor_params"),
        )

        # Parâmetros para Pré-tratamento #

        # Lista de primary keys da tabela
        primary_keys = TypedParameter(
            name="primary_keys",
            default=overwrite_flow_params.get("primary_keys"),
            accepted_types=(list, NoneType),
        )
        # Dicionário com argumentos para serem passados na função de ler os dados raw:
        # pd.read_csv ou pd.read_json
        pretreatment_reader_args = TypedParameter(
            name="pretreatment_reader_args",
            default=overwrite_flow_params.get("pretreatment_reader_args"),
            accepted_types=(dict, NoneType),
        )

        # Parâmetros para Carregamento de Dados #

        # Nome do bucket para salvar os dados
        # Se for None, salva no bucket padrão do ambiente atual
        save_bucket_name = TypedParameter(
            name="save_bucket_name",
            default=overwrite_flow_params.get("save_bucket_name"),
            accepted_types=(str, NoneType),
        )

        # Preparar execução #

        timestamp = get_current_timestamp()
        dataset_id = source_name + "_source"

        env = get_run_env()

        table = create_table_object(
            env=env,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name=save_bucket_name,
            timestamp=timestamp,
            partition_date_only=partition_date_only,
            raw_filetype=raw_filetype,
        )

        incremental_capture_strategy = create_incremental_strategy(
            strategy_dict=incremental_capture_strategy,
            table=table,
            overwrite_start_value=incremental_start_value,
            overwrite_end_value=incremental_end_value,
        )

        incremental_info = incremental_capture_strategy["incremental_info"]

        rename_flow_run = rename_capture_flow(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=timestamp,
            incremental_info=incremental_info,
        )

        # Extração #

        data_extractor = create_extractor_task(
            env=env,
            dataset_id=dataset_id,
            table_id=table_id,
            save_filepath=table["raw_filepath"],
            data_extractor_params=data_extractor_params,
            incremental_info=incremental_info,
        )

        data_extractor.set_upstream(rename_flow_run)

        get_raw = get_raw_data(data_extractor=data_extractor)

        upload_raw_gcs = upload_raw_file_to_gcs(table=table, upstream_tasks=[get_raw])

        # Pré-tratamento #

        pretreatment = transform_raw_to_nested_structure(
            pretreat_funcs=pretreat_funcs,
            raw_filepath=table["raw_filepath"],
            source_filepath=table["source_filepath"],
            timestamp=timestamp,
            primary_keys=primary_keys,
            print_inputs=save_bucket_name.is_equal(None),
            reader_args=pretreatment_reader_args,
            upstream_tasks=[get_raw],
        )

        upload_source_gcs = upload_source_data_to_gcs(table=table, upstream_tasks=[pretreatment])

        # Finalizar Flow #

        save_incremental_redis(
            incremental_capture_strategy=incremental_capture_strategy,
            upstream_tasks=[upload_source_gcs, upload_raw_gcs],
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
