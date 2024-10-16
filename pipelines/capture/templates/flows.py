# -*- coding: utf-8 -*-

from types import NoneType

from prefect import unmapped

# from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage import GCS
from prefect.tasks.core.function import FunctionTask
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.templates.tasks import (
    create_filepaths,
    create_partition_task,
    get_capture_timestamps,
    get_raw_data,
    rename_capture_flow,
    set_env,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.constants import constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.prefect import TypedParameter


def create_default_capture_flow(
    flow_name: str,
    source: SourceTable,
    create_extractor_task: FunctionTask,
    agent_label: str,
    recapture_days: int = 1,
    generate_schedule: bool = True,
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

    Returns:
        Flow: The capture flow
    """

    with Flow(flow_name) as capture_flow:
        # Parâmetros Gerais #

        # table_id no BigQuery
        # exclude_table_id = TypedParameter(
        #     name="exclude_table_id",
        #     default=None,
        #     accepted_types=(NoneType, list),
        # )

        timestamp = TypedParameter(
            name="timestamp",
            default=None,
            accepted_types=(NoneType, str),
        )

        recapture_days = TypedParameter(
            name="recapture_days",
            default=recapture_days,
            accepted_types=int,
        )

        recapture = TypedParameter(
            name="recapture",
            default=False,
            accepted_types=bool,
        )

        # Preparar execução #

        timestamp = get_scheduled_timestamp(timestamp=timestamp)

        env = get_run_env()

        activated_source = set_env(env=env, source=source)

        timestamps = get_capture_timestamps(
            source=activated_source,
            timestamp=timestamp,
            recapture=recapture,
            recapture_days=recapture_days,
        )

        rename_capture_flow(
            flow_name=flow_name,
            timestamp=timestamp,
            recapture=recapture,
        )

        partitions = create_partition_task.map(
            source=unmapped(activated_source),
            timestamp=timestamps,
        )

        filepaths = create_filepaths.map(
            source=unmapped(activated_source),
            partition=partitions,
            timestamp=timestamps,
        )

        # Extração #

        data_extractors = create_extractor_task.map(
            source=unmapped(activated_source),
            timestamp=timestamps,
        )

        get_raw = get_raw_data.map(
            data_extractor=data_extractors,
            filepaths=filepaths,
            raw_filetype=unmapped(activated_source["raw_filetype"]),
        )

        upload_raw = upload_raw_file_to_gcs.map(
            source=unmapped(activated_source),
            filepaths=filepaths,
            partition=partitions,
        )
        upload_raw.set_upstream(get_raw)

        # Pré-tratamento #

        pretreatment = transform_raw_to_nested_structure.map(
            filepaths=filepaths,
            timestamp=timestamps,
            primary_keys=unmapped(activated_source["primary_keys"]),
            reader_args=unmapped(activated_source["pretreatment_reader_args"]),
            pretreat_funcs=unmapped(activated_source.pretreat_funcs),
        )
        pretreatment.set_upstream(get_raw)

        upload_source = upload_source_data_to_gcs.map(
            source=unmapped(activated_source),
            partition=partitions,
            filepaths=filepaths,
        )

        upload_source.set_upstream(pretreatment)

    capture_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    capture_flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[agent_label],
    )
    capture_flow.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

    if generate_schedule:

        capture_flow.schedule = Schedule(
            [
                CronClock(
                    source.schedule_cron,
                    labels=[
                        agent_label,
                    ],
                )
            ]
        )

    return capture_flow
