# -*- coding: utf-8 -*-

from datetime import datetime
from types import NoneType
from typing import Callable

import pandas as pd

# from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.core.function import FunctionTask
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.capture.templates.tasks import (
    get_raw_data,
    rename_capture_flow,
    set_env,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.capture.templates.utils import Source
from pipelines.constants import constants
from pipelines.tasks import get_current_timestamp, get_run_env
from pipelines.utils.prefect import TypedParameter

# from pipelines.utils.pretreatment import strip_string_columns


def create_default_capture_flow(
    flow_name_sufix: str,
    source: Source,
    create_extractor_task: FunctionTask,
    agent_label: str,
    pretreat_funcs: list[Callable[[pd.DataFrame, datetime, list], pd.DataFrame]] = None,
    pretreatment_reader_args=None,
    skip_if_running=False,
    recapture_interval_minutes=None,
):  # pylint: disable=R0914, R0913
    """
    Cria um flow de captura

    Args:
        flow_name (str): O nome do flow
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

    with Flow(flow_name_sufix) as capture_flow:
        # Parâmetros Gerais #

        # table_id no BigQuery
        table_id = TypedParameter(
            name="table_id",
            default=source.table_id,
            accepted_types=str,
        )
        # Tipo do arquivo raw (json, csv...)
        # raw_filetype = TypedParameter(
        #     name="raw_filetype",
        #     default=source.raw_filetype,
        #     accepted_types=str,
        # )

        timestamp = TypedParameter(
            name="timestamp",
            default=None,
            accepted_types=(NoneType, str),
        )

        # recapture_days = TypedParameter(
        #     name="recapture_days",
        #     default=source.recapture_days,
        #     accepted_types=int,
        # )

        # recapture = TypedParameter(
        #     name="recapture",
        #     default=False,
        #     accepted_types=bool,
        # )

        # Parâmetros para Pré-tratamento #

        # Lista de primary keys da tabela
        primary_keys = TypedParameter(
            name="primary_keys",
            default=source.primary_keys,
            accepted_types=(list, NoneType),
        )
        # Dicionário com argumentos para serem passados na função de ler os dados raw:
        # pd.read_csv ou pd.read_json
        pretreatment_reader_args = TypedParameter(
            name="pretreatment_reader_args",
            default=source.pretreatment_reader_args,
            accepted_types=(dict, NoneType),
        )

        # Parâmetros para Carregamento de Dados #

        # Nome do bucket para salvar os dados
        # Se for None, salva no bucket padrão do ambiente atual
        save_bucket_names = TypedParameter(
            name="save_bucket_names",
            default=source.bucket_names,
            accepted_types=(dict, NoneType),
        )

        # Preparar execução #

        timestamp = get_current_timestamp()

        dataset_id = "source_name" + "_source"

        env = get_run_env()

        source = set_env(env=env, source=source)

        rename_capture_flow(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=timestamp,
        )

        # Extração #

        data_extractor = create_extractor_task(
            env=env,
            source=source,
            save_filepath=source["raw_filepath"],
        )

        get_raw = get_raw_data(data_extractor=data_extractor)

        upload_raw_file_to_gcs(source=source, upstream_tasks=[get_raw])

        # Pré-tratamento #

        pretreatment = transform_raw_to_nested_structure(
            pretreat_funcs=pretreat_funcs,
            raw_filepath=source["raw_filepath"],
            source_filepath=source["source_filepath"],
            timestamp=timestamp,
            primary_keys=primary_keys,
            print_inputs=save_bucket_names.is_equal(None),
            reader_args=pretreatment_reader_args,
            upstream_tasks=[get_raw],
        )

        upload_source_data_to_gcs(table=source, upstream_tasks=[pretreatment])

    capture_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    capture_flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[agent_label],
    )
    capture_flow.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

    if skip_if_running:
        capture_flow.state_handlers.append(handler_skip_if_running)

    return capture_flow
