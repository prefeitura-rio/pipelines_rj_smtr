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
from pipelines.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    parse_string_to_timestamp,
)
from pipelines.treatment.templates.tasks import get_last_materialization_datetime

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

        # rebuild = TypedParameter(
        #     name="rebuild",
        #     default=False,
        #     accepted_types=bool,
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
        initial_datetime = TypedParameter(
            name="initial_datetime",
            default=None,
            accepted_types=(str, NoneType),
        )
        # Substitui a data final da execução incremental
        end_datetime = TypedParameter(
            name="end_datetime",
            default=None,
            accepted_types=(str, NoneType),
        )

        env = get_run_env()

        timestamp = get_scheduled_timestamp(timestamp=timestamp)

        last_materialization_datetime, redis_key = get_last_materialization_datetime(
            env=env, selector=selector
        )

        initial_datetime = parse_string_to_timestamp(timestamp_str=initial_datetime)
        end_datetime = parse_string_to_timestamp(timestamp_str=end_datetime)

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
