# -*- coding: utf-8 -*-
"""Flows de Tratamento de dados Genéricos"""
from datetime import datetime, time
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
    dbt_data_quality_checks,
    get_datetime_end,
    get_datetime_start,
    get_repo_version,
    rename_materialization_flow,
    run_dbt,
    save_materialization_datetime_redis,
    setup_dbt_test,
    test_fallback_run,
    wait_data_sources,
)
from pipelines.treatment.templates.utils import DBTSelector, DBTTest
from pipelines.utils.prefect import TypedParameter, handler_skip_if_running_tolerant


def create_default_materialization_flow(
    flow_name: str,
    selector: DBTSelector,
    agent_label: str,
    wait: list = None,
    generate_schedule: bool = True,
    snapshot_selector: DBTSelector = None,
    test_scheduled_time: time = None,
    pre_tests: DBTTest = None,
    post_tests: DBTTest = None,
    test_webhook_key: str = "dataplex",
    skip_if_running_tolerance: int = 0,
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
        snapshot_selector (DBTSelector): Objeto que representa o selector do DBT para snapshot
        test_scheduled_time (time): Horário para rodar o test no formato "HH:MM:SS"
        pre_tests (DBTTest): Configuração para testes pré-materialização
        post_tests (DBTTest): Configuração para testes pós-materialização
        test_webhook_key (str): Key do secret do webhook para enviar a notificação do teste
        skip_if_running_tolerance (int): Quantidade de tempo em minutos para esperar antes de
            cancelar a execução se outra run já estiver rodando

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

        additional_vars = TypedParameter(
            name="additional_vars",
            default=None,
            accepted_types=(dict, NoneType),
        )

        fallback_run = TypedParameter(
            name="fallback_run",
            default=False,
            accepted_types=bool,
        )

        env = get_run_env()

        timestamp = get_scheduled_timestamp()

        run = test_fallback_run(
            env=env,
            fallback_run=fallback_run,
            timestamp=timestamp,
            selector=selector,
        )

        with case(run, True):

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
                additional_vars=additional_vars,
            )

            if pre_tests:
                run_pre_test, pre_test_vars = setup_dbt_test(
                    timestamp,
                    test_scheduled_time,
                    dbt_run_vars,
                    pre_tests,
                    upstream_tasks=[complete_sources],
                )

                with case(run_pre_test, True):
                    dbt_pre_test = run_dbt(
                        resource="test",
                        test_name=pre_tests["test_name"],
                        dataset_id=pre_tests["dataset_id"],
                        table_id=pre_tests["table_id"],
                        model=pre_tests["model"],
                        exclude=pre_tests["exclude"],
                        flags=flags,
                        _vars=pre_test_vars,
                        upstream_tasks=[run_pre_test],
                    )
                    notify_pre_test = dbt_data_quality_checks(
                        dbt_logs=dbt_pre_test,
                        checks_list=pre_tests["checks_list"],
                        params=pre_test_vars,
                    )
                    wait_pre_test = notify_pre_test

                with case(run_pre_test, False):
                    wait_pre_test = complete_sources
            else:
                wait_pre_test = complete_sources

            dbt_run = run_dbt(
                resource="model",
                selector_name=selector.name,
                flags=flags,
                _vars=dbt_run_vars,
                upstream_tasks=[wait_pre_test],
            )

            if post_tests:
                run_post_test, post_test_vars = setup_dbt_test(
                    timestamp,
                    test_scheduled_time,
                    dbt_run_vars,
                    post_tests,
                    upstream_tasks=[dbt_run],
                )

                with case(run_post_test, True):
                    dbt_post_test = run_dbt(
                        resource="test",
                        test_name=post_tests["test_name"],
                        dataset_id=post_tests["dataset_id"],
                        table_id=post_tests["table_id"],
                        model=post_tests["model"],
                        exclude=post_tests["exclude"],
                        flags=flags,
                        _vars=post_test_vars,
                        upstream_tasks=[run_post_test],
                    )
                    notify_post_test = dbt_data_quality_checks(
                        dbt_logs=dbt_post_test,
                        checks_list=post_tests["checks_list"],
                        webhook_key=test_webhook_key,
                        params=post_test_vars,
                    )
                    wait_post_test = notify_post_test

                with case(run_post_test, False):
                    wait_post_test = dbt_run
            else:
                wait_post_test = dbt_run

            if snapshot_selector:
                dbt_snapshot = run_dbt(
                    resource="snapshot",
                    selector_name=snapshot_selector.name,
                    flags=flags,
                    _vars=dbt_run_vars,
                    upstream_tasks=[wait_post_test],
                )
                wait_dbt = dbt_snapshot
            else:
                wait_dbt = wait_post_test

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
    ]

    if skip_if_running_tolerance > 0:
        default_materialization_flow.state_handlers.append(
            handler_skip_if_running_tolerant(tolerance_minutes=skip_if_running_tolerance)
        )
    else:
        default_materialization_flow.state_handlers.append(handler_skip_if_running)

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
