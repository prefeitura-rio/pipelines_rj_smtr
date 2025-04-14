# -*- coding: utf-8 -*-
"""Flows de Tratamento de dados Genéricos"""
from datetime import datetime
from types import NoneType

from prefect import case
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from pytz import timezone

from pipelines.constants import constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.treatment.templates.tasks import (
    create_dbt_run_vars,
    get_datetime_end,
    get_datetime_start,
    get_repo_version,
    rename_materialization_flow,
    run_dbt_selector,
    run_dbt_snapshot,
    save_materialization_datetime_redis,
    wait_data_sources,
)
from pipelines.treatment.templates.utils import DBTSelector
from pipelines.utils.prefect import TypedParameter


def create_default_materialization_flow(
    flow_name: str,
    selector: DBTSelector,
    agent_label: str,
    wait: list = None,
    generate_schedule: bool = True,
    run_snapshot: bool = False,
    snapshot_selector: DBTSelector = None,
) -> Flow:
    """
    Cria um flow de materialização

    Args:
        flow_name (str): O nome do Flow
        selector (DBTSelector): Objeto que representa o selector do DBT
        agent_label (str): Label do flow
        wait (list): Lista de DBTSelectors e/ou SourceTables para verificar a completude dos dados
        generate_schedule (bool): Se a função vai agendar o flow com base
            no parametro schedule_cron do selector
        run_snapshot (bool): Se o flow deve executar um snapshot após a materialização
        snapshot_selector (DBTSelector): Objeto que representa o selector do DBT para snapshot.
            Deve ser fornecido se run_snapshot for True.

        Returns:
            Flow: Flow de materialização
    """
    if wait is None:
        wait = []
    with Flow(flow_name) as default_materialization_flow:

        flags = TypedParameter(
            name="flags",
            default=None,
            accepted_types=(str, NoneType),
        )

        # Parâmetros para filtros incrementais #

        # Substitui a data inicial da execução incremental
        datetime_start = TypedParameter(
            name="initial_datetime",
            default=None,
            accepted_types=(str, NoneType),
        )
        # Substitui a data final da execução incremental
        datetime_end = TypedParameter(
            name="end_datetime",
            default=None,
            accepted_types=(str, NoneType),
        )

        skip_source_check = TypedParameter(
            name="skip_source_check",
            default=False,
            accepted_types=bool,
        )

        env = get_run_env()

        timestamp = get_scheduled_timestamp()

        datetime_start = get_datetime_start(
            env=env,
            selector=selector,
            datetime_start=datetime_start,
        )

        datetime_end = get_datetime_end(
            selector=selector,
            timestamp=timestamp,
            datetime_end=datetime_end,
        )

        complete_sources = wait_data_sources(
            env=env,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            data_sources=wait,
            skip=skip_source_check,
        )

        rename_flow_run = rename_materialization_flow(
            selector=selector,
            timestamp=timestamp,
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )

        repo_version = get_repo_version(upstream_tasks=[rename_flow_run])

        dbt_run_vars = create_dbt_run_vars(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            repo_version=repo_version,
        )

        dbt_run = run_dbt_selector(
            selector_name=selector.name,
            flags=flags,
            _vars=dbt_run_vars,
            upstream_tasks=[complete_sources],
        )

        with case(run_snapshot, True):
            dbt_snapshot = run_dbt_snapshot(
                selector_name=snapshot_selector.name,
                flags=flags,
                _vars=dbt_run_vars,
                upstream_tasks=[dbt_run],
            )
            wait_dbt = dbt_snapshot

        with case(run_snapshot, False):
            wait_dbt = dbt_run

        save_materialization_datetime_redis(
            env=env, selector=selector, value=datetime_end, upstream_tasks=[wait_dbt]
        )

    default_materialization_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    default_materialization_flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[agent_label],
    )

    default_materialization_flow.state_handlers = [
        handler_inject_bd_credentials,
        handler_skip_if_running,
    ]

    if generate_schedule:

        default_materialization_flow.schedule = Schedule(
            [
                CronClock(
                    selector.schedule_cron,
                    labels=[
                        agent_label,
                    ],
                    start_date=datetime.now(tz=timezone(constants.TIMEZONE.value)),
                )
            ]
        )

    return default_materialization_flow
