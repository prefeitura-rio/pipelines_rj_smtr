# -*- coding: utf-8 -*-
"""Flows de Tratamento de dados Genéricos"""
# from datetime import datetime
from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.treatment.templates.tasks import get_datetime_end, get_datetime_start

# create_dbt_run_vars,; get_repo_version,; rename_materialization_flow,;
# save_materialization_datetime_redis,
from pipelines.treatment.templates.utils import DBTSelector
from pipelines.utils.prefect import TypedParameter


def create_default_materialization_flow(
    selector: DBTSelector,
    agent_label: str,
    wait: list = None,
    create_schedule: bool = True,
) -> Flow:
    """
    Cria um flow de materialização

    Args:
        flow_name (str): O nome do Flow
        dataset_id (str): O dataset_id no BigQuery
        datetime_column_name (str): O nome da coluna de datetime para buscar a
            última data materializada, caso não tenha registrado no Redis
        create_datetime_variables_task (FunctionTask): Task de criação das variáveis de
            data para execuções incrementais
        overwrite_flow_param_values (dict): Dicionário para substituir os valores padrões dos
            parâmetros do flow
        agent_label (str): Label do flow
        data_quality_checks (list[DataQualityCheckArgs]): Lista de checks do Dataplex para
            serem executados

        Returns:
            Flow: Flow de materialização
    """
    with Flow(selector + " - materializacao") as default_materialization_flow:

        # flags = TypedParameter(
        #     name="flags",
        #     default=False,
        #     accepted_types=(str, NoneType),
        # )

        # Parâmetros para filtros incrementais #

        # Data de referência da execução, ela subtraída pelo incremental_delay_hours
        # será a data final do filtro incremental
        timestamp = TypedParameter(
            name="timestamp",
            default=None,
            accepted_types=(str, NoneType),
        )

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

        env = get_run_env()

        timestamp = get_scheduled_timestamp(timestamp=timestamp)

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

        # datetime_vars, datetime_start, datetime_end = create_datetime_variables_task(
        #     timestamp=timestamp,
        #     last_materialization_datetime=last_materialization_datetime,
        #     incremental_delay_hours=incremental_delay_hours,
        #     overwrite_initial_datetime=overwrite_initial_datetime,
        # )

        # rename_flow_run = rename_materialization_flow(
        #     dataset_id=dataset_id,
        #     # table_id=table_id,
        #     timestamp=timestamp,
        #     datetime_start=datetime_start,
        #     datetime_end=datetime_end,
        # )

        # repo_version = get_repo_version(upstream_tasks=[rename_flow_run])

        # dbt_run_vars = create_dbt_run_vars(
        #     datetime_vars=datetime_vars,
        #     repo_version=repo_version,
        # )

        # run_dbt = run_dbt_model_task(
        #     dataset_id=dataset_id,
        #     # table_id=table_id,
        #     # upstream=upstream,
        #     # downstream=downstream,
        #     # exclude=exclude,
        #     rebuild=rebuild,
        #     dbt_run_vars=dbt_run_vars,
        # )

        # save_materialization_datetime_redis(
        #     redis_key=redis_key,
        #     value=datetime_end,
        #     upstream_tasks=[run_dbt],
        # )

        # if data_quality_checks is not None:
        #     pass
        # run_data_quality_checks(
        #     data_quality_checks=data_quality_checks,
        #     initial_partition=datetime_start,
        #     final_partition=datetime_end,
        #     upstream_tasks=[save_redis],
        # )

    default_materialization_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    default_materialization_flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        labels=[agent_label],
    )

    default_materialization_flow.state_handlers = [
        handler_inject_bd_credentials,
        handler_skip_if_running,
    ]

    return default_materialization_flow
