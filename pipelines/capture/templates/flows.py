# -*- coding: utf-8 -*-
"""Flow genérico de captura"""

from datetime import datetime
from types import NoneType

from prefect import task, unmapped
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
from pytz import timezone

from pipelines.capture.templates.tasks import (
    create_filepaths,
    create_partition_task,
    get_capture_date_range,
    get_capture_timestamps,
    get_raw_data,
    rename_capture_flow,
    set_capture_date_range_redis,
    set_env,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.capture.templates.utils import DateRangeSourceTable, DefaultSourceTable
from pipelines.constants import constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.utils.prefect import TypedParameter, handler_skip_if_running_tolerant


def create_default_capture_flow(
    flow_name: str,
    source: DefaultSourceTable,
    create_extractor_task: FunctionTask,
    agent_label: str,
    recapture_days: int = 2,
    generate_schedule: bool = True,
    recapture_schedule_cron: str = None,
):  # pylint: disable=R0914, R0913
    """
    Cria um flow de captura com captura e recaptura

    Args:
        flow_name (str): O nome do flow
        source (DefaultSourceTable): Objeto representando a fonte de dados que será capturada
        create_extractor_task (FunctionTask):
            A task que prepara a função de extração
            Pode receber os argumentos:
                source (DefaultSourceTable): Objeto representando a fonte de dados que será
                    capturada
                timestamp (datetime): a timestamp de referência da execução
            Deve retornar uma função que execute sem argumentos
        agent_label (str): Label do flow
        recapture_days (int): A quantidade de dias que o flows vai considerar para achar datas
            a serem recapturadas
        generate_schedule (bool): Se a função vai agendar o flow com base
            no parametro schedule_cron do source

    Returns:
        Flow: O flow de captura
    """

    with Flow(flow_name) as capture_flow:

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
            pretreat_funcs=unmapped(activated_source["pretreat_funcs"]),
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
        clocks = [
            CronClock(
                source.schedule_cron,
                labels=[
                    agent_label,
                ],
                start_date=datetime.now(tz=timezone(constants.TIMEZONE.value)),
            )
        ]

        if recapture_schedule_cron:
            clocks.append(
                CronClock(
                    recapture_schedule_cron,
                    labels=[
                        agent_label,
                    ],
                    start_date=datetime.now(tz=timezone(constants.TIMEZONE.value)),
                    parameter_defaults={"recapture": True},
                )
            )

        capture_flow.schedule = Schedule(clocks)

    return capture_flow


def create_date_range_capture_flow(
    flow_name: str,
    sources: list[DateRangeSourceTable],
    create_extractor_task: FunctionTask,
    agent_label: str,
    generate_schedule: bool = True,
    skip_tolerance_minutes: int = 0,
) -> Flow:  # pylint: disable=R0914, R0913
    """
    Cria um flow de captura com range de datas que pode capturar de várias tabelas simultaneamente

    Args:
        flow_name (str): O nome do flow
        sources (list[DateRangeSourceTable]): Lista de objetos representando as fontes de dados
        que serão capturadas (todos devem ter o mesmo schedule_cron)
        create_extractor_task (FunctionTask):
            A task que prepara a função de extração
            Pode receber os argumentos:
                source (DateRangeSourceTable): Objeto representando a fonte de dados que será
                    capturada
                date_range (dict): dicionário com as keys:
                    date_range_start (datetime) e date_range_end (datetime)
            Deve retornar uma função que execute sem argumentos
        agent_label (str): Label do flow
        generate_schedule (bool): Se a função vai agendar o flow com base
            no parametro schedule_cron do source
        skip_tolerance_minutes (int): Quantidade de minutos que o flow deve esperar para
            ser cancelado caso haja outra execução acontecendo
    Returns:
        Flow: O flow de captura
    """
    with Flow(name=flow_name) as capture_flow:

        table_ids = TypedParameter(
            name="table_ids",
            default=[s.table_id for s in sources],
            accepted_types=list,
        )

        date_range_start_param = TypedParameter(
            name="date_range_start",
            default=None,
            accepted_types=(NoneType, str),
        )

        date_range_end_param = TypedParameter(
            name="date_range_end",
            default=None,
            accepted_types=(NoneType, str),
        )

        # Preparar execução #

        timestamp = get_scheduled_timestamp()

        env = get_run_env()

        filtered_sources = task(
            lambda sources, table_ids: [s for s in sources if s.table_id in table_ids],
            name="filter_sources",
        )(sources=sources, table_ids=table_ids)

        activated_sources = set_env.map(env=unmapped(env), source=filtered_sources)

        rename_capture_flow(
            flow_name=flow_name,
            timestamp=timestamp,
            recapture=False,
        )

        partitions = create_partition_task.map(
            source=activated_sources,
            timestamp=unmapped(timestamp),
        )

        filepaths = create_filepaths.map(
            source=activated_sources,
            partition=partitions,
            timestamp=unmapped(timestamp),
        )

        date_ranges = get_capture_date_range.map(
            source=activated_sources,
            timestamp=unmapped(timestamp),
            date_range_start_param=unmapped(date_range_start_param),
            date_range_end_param=unmapped(date_range_end_param),
        )

        # Extração #

        data_extractors = create_extractor_task.map(
            source=activated_sources,
            date_range=date_ranges,
        )

        raw_filetypes = task(
            lambda sources: [s["raw_filetype"] for s in sources], name="get_raw_filetypes"
        )(sources=activated_sources)

        get_raw = get_raw_data.map(
            data_extractor=data_extractors,
            filepaths=filepaths,
            raw_filetype=raw_filetypes,
        )

        upload_raw = upload_raw_file_to_gcs.map(
            source=activated_sources,
            filepaths=filepaths,
            partition=partitions,
        )
        upload_raw.set_upstream(get_raw)

        # Pré-tratamento #

        primary_keys = task(
            lambda sources: [s["primary_keys"] for s in sources], name="get_primary_keys"
        )(sources=activated_sources)

        reader_args = task(
            lambda sources: [s["pretreatment_reader_args"] for s in sources], name="get_reader_args"
        )(sources=activated_sources)

        pretreat_funcs = task(
            lambda sources: [s["pretreat_funcs"] for s in sources], name="get_pretreat_funcs"
        )(sources=activated_sources)

        pretreatment = transform_raw_to_nested_structure.map(
            filepaths=filepaths,
            timestamp=unmapped(timestamp),
            primary_keys=primary_keys,
            reader_args=reader_args,
            pretreat_funcs=pretreat_funcs,
        )
        pretreatment.set_upstream(get_raw)

        upload_source = upload_source_data_to_gcs.map(
            source=activated_sources,
            partition=partitions,
            filepaths=filepaths,
        )

        upload_source.set_upstream(pretreatment)

        save_redis = set_capture_date_range_redis.map(
            source=activated_sources, date_range=date_ranges
        )
        save_redis.set_upstream(upload_source)

        capture_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
        capture_flow.run_config = KubernetesRun(
            image=constants.DOCKER_IMAGE.value,
            labels=[agent_label],
        )
        capture_flow.state_handlers = [
            handler_inject_bd_credentials,
            handler_initialize_sentry,
            handler_skip_if_running_tolerant(tolerance_minutes=skip_tolerance_minutes),
        ]

        if generate_schedule:

            capture_flow.schedule = Schedule(
                [
                    CronClock(
                        sources[0].schedule_cron,
                        labels=[
                            agent_label,
                        ],
                        start_date=datetime.now(tz=timezone(constants.TIMEZONE.value)),
                    )
                ]
            )

        return capture_flow
